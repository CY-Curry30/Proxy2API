package project

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"Proxy2API/internal/boxmgr"
	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/state"
	"Proxy2API/internal/subscription"
)

type RuntimeStatus string

const (
	StatusStopped  RuntimeStatus = "stopped"
	StatusStarting RuntimeStatus = "starting"
	StatusRunning  RuntimeStatus = "running"
	StatusStopping RuntimeStatus = "stopping"
	StatusFailed   RuntimeStatus = "failed"
)

// Runtime owns every mutable component belonging to one project.
type Runtime struct {
	id           string
	configPath   string
	sharedPath   string
	sharedCfg    *config.Config
	sharedMu     *sync.RWMutex
	clashAPIPort uint16
	ports        *PortRegistry
	parentCtx    context.Context

	lifecycleMu sync.Mutex
	mu          sync.RWMutex
	status      RuntimeStatus
	lastError   string
	startedAt   time.Time
	cfg         *config.Config
	stateStore  *state.Store
	boxMgr      *boxmgr.Manager
	subMgr      *subscription.Manager
	cancel      context.CancelFunc
	logBuffer   *monitor.LogBuffer
	logger      *runtimeLogger
}

func NewRuntime(parent context.Context, id, configPath, sharedPath string, sharedCfg *config.Config, sharedMu *sync.RWMutex, clashAPIPort uint16, ports *PortRegistry) *Runtime {
	if parent == nil {
		parent = context.Background()
	}
	buffer := monitor.NewLogBuffer(64 * 1024)
	return &Runtime{
		id:           id,
		configPath:   configPath,
		sharedPath:   sharedPath,
		sharedCfg:    sharedCfg,
		sharedMu:     sharedMu,
		clashAPIPort: clashAPIPort,
		ports:        ports,
		parentCtx:    parent,
		status:       StatusStopped,
		logBuffer:    buffer,
		logger:       newRuntimeLogger(id, buffer),
	}
}

func (r *Runtime) Start() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	r.mu.RLock()
	status := r.status
	r.mu.RUnlock()
	if status == StatusRunning || status == StatusStarting {
		return nil
	}
	r.setStatus(StatusStarting, "")

	if r.sharedMu != nil {
		r.sharedMu.RLock()
	}
	cfg, err := config.LoadProjectWithShared(r.configPath, r.sharedCfg)
	if r.sharedMu != nil {
		r.sharedMu.RUnlock()
	}
	if err != nil {
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("load project %q config: %w", r.id, err)
	}
	cfg.ClashAPIPort = r.clashAPIPort
	if err := r.ports.Reserve(r.id, cfg); err != nil {
		r.setStatus(StatusFailed, err.Error())
		return err
	}
	store, err := state.Open(cfg.FilePath())
	if err != nil {
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("open project %q state: %w", r.id, err)
	}
	ctx, cancel := context.WithCancel(r.parentCtx)
	monitorCfg := monitorConfigForProject(cfg, store)
	boxMgr := boxmgr.New(
		cfg,
		monitorCfg,
		boxmgr.WithLogger(r.logger),
		boxmgr.WithConfigValidator(func(next *config.Config) error {
			return r.ports.Reserve(r.id, next)
		}),
		boxmgr.WithSharedConfig(r.sharedCfg, r.sharedMu),
	)
	if err := boxMgr.Start(ctx); err != nil {
		cancel()
		_ = boxMgr.Close()
		_ = store.Close()
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("start project %q: %w", r.id, err)
	}

	subMgr := subscription.New(cfg, boxMgr, subscription.WithStateStore(store))
	subMgr.Start()

	r.mu.Lock()
	r.cfg = cfg
	r.stateStore = store
	r.boxMgr = boxMgr
	r.subMgr = subMgr
	r.cancel = cancel
	r.status = StatusRunning
	r.lastError = ""
	r.startedAt = time.Now()
	r.mu.Unlock()
	r.logger.Infof("project runtime started")
	return nil
}

// Restart rebuilds a running project from its project settings and the latest
// shared node/subscription catalog.
func (r *Runtime) Restart() error {
	status, _, _ := r.Status()
	if status != StatusRunning {
		return nil
	}
	if err := r.Stop(); err != nil {
		return err
	}
	return r.Start()
}

func monitorConfigForProject(cfg *config.Config, store *state.Store) monitor.Config {
	proxyUsername := cfg.Listener.Username
	proxyPassword := cfg.Listener.Password
	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		proxyUsername = cfg.MultiPort.Username
		proxyPassword = cfg.MultiPort.Password
	}
	return monitor.Config{
		Enabled:          false,
		StateStore:       store,
		ProbeTarget:      cfg.ProbeTargetOrDefault(),
		ProbeInterval:    cfg.ProbeIntervalOrDefault(),
		ProbeTimeout:     cfg.ProbeTimeoutOrDefault(),
		ProxyUsername:    proxyUsername,
		ProxyPassword:    proxyPassword,
		ExternalIP:       cfg.ExternalIP,
		ProbeConcurrency: cfg.ProbeConcurrencyOrDefault(),
		StickyNode:       cfg.Sticky.FixedNode,
		SkipCertVerify:   cfg.SkipCertVerify,
		TrafficAPI:       fmt.Sprintf("http://127.0.0.1:%d/traffic", cfg.ClashAPIPort),
	}
}

func (r *Runtime) Stop() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	r.mu.Lock()
	if r.status == StatusStopped {
		r.mu.Unlock()
		return nil
	}
	r.status = StatusStopping
	subMgr := r.subMgr
	boxMgr := r.boxMgr
	store := r.stateStore
	cancel := r.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if subMgr != nil {
		subMgr.Stop()
	}
	var closeErr error
	if boxMgr != nil {
		closeErr = boxMgr.Close()
	}
	if store != nil {
		if err := store.Close(); closeErr == nil {
			closeErr = err
		}
	}
	r.mu.Lock()
	r.cfg = nil
	r.stateStore = nil
	r.boxMgr = nil
	r.subMgr = nil
	r.cancel = nil
	r.startedAt = time.Time{}
	if closeErr != nil {
		r.status = StatusFailed
		r.lastError = closeErr.Error()
	} else {
		r.status = StatusStopped
		r.lastError = ""
	}
	r.mu.Unlock()
	return closeErr
}

func (r *Runtime) Reload(ctx context.Context) error {
	r.mu.RLock()
	boxMgr := r.boxMgr
	r.mu.RUnlock()
	if boxMgr == nil {
		return fmt.Errorf("project %q is not running", r.id)
	}
	if err := boxMgr.TriggerReload(ctx); err != nil {
		r.setLastError(err)
		return err
	}
	r.setLastError(nil)
	return nil
}

func (r *Runtime) setStatus(status RuntimeStatus, message string) {
	r.mu.Lock()
	r.status = status
	r.lastError = message
	r.mu.Unlock()
}

func (r *Runtime) setLastError(err error) {
	r.mu.Lock()
	if err == nil {
		r.lastError = ""
	} else {
		r.lastError = err.Error()
	}
	r.mu.Unlock()
}

func (r *Runtime) Status() (RuntimeStatus, string, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status, r.lastError, r.startedAt
}

func (r *Runtime) Config() *config.Config {
	r.mu.RLock()
	boxMgr := r.boxMgr
	cfg := r.cfg
	r.mu.RUnlock()
	if boxMgr != nil {
		return boxMgr.CurrentConfig()
	}
	return cfg
}

func (r *Runtime) MonitorManager() *monitor.Manager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.boxMgr == nil {
		return nil
	}
	return r.boxMgr.MonitorManager()
}

func (r *Runtime) NodeManager() monitor.NodeManager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.boxMgr == nil {
		return nil
	}
	return r.boxMgr
}

func (r *Runtime) SubscriptionRefresher() monitor.SubscriptionRefresher {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.subMgr == nil {
		return nil
	}
	return r.subMgr
}

func (r *Runtime) LogBuffer() *monitor.LogBuffer { return r.logBuffer }

type runtimeLogger struct {
	logger *log.Logger
}

func newRuntimeLogger(projectID string, buffer io.Writer) *runtimeLogger {
	return &runtimeLogger{logger: log.New(io.MultiWriter(log.Writer(), buffer), "[project:"+projectID+"] ", log.LstdFlags|log.Lshortfile)}
}

func (l *runtimeLogger) Infof(format string, args ...any)  { l.logger.Printf("INFO "+format, args...) }
func (l *runtimeLogger) Warnf(format string, args ...any)  { l.logger.Printf("WARN "+format, args...) }
func (l *runtimeLogger) Errorf(format string, args ...any) { l.logger.Printf("ERROR "+format, args...) }
