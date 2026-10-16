package project

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
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

	lifecycleMu      sync.Mutex
	mu               sync.RWMutex
	status           RuntimeStatus
	lastError        string
	startedAt        time.Time
	cfg              *config.Config
	stateStore       *state.Store
	boxMgr           *boxmgr.Manager
	subMgr           *subscription.Manager
	cancel           context.CancelFunc
	trafficDone      chan struct{}
	logBuffer        *monitor.LogBuffer
	logger           *runtimeLogger
	fetchCoordinator *subscription.FetchCoordinator
	probeCoordinator *monitor.ProbeCoordinator
	// systemProxyURL resolves the workspace-wide proxy from the system
	// settings. It is read on every subscription refresh so a settings change
	// applies without restarting the project.
	systemProxyURL func() string
	// globalProbeTarget resolves the workspace-wide latency target. A non-empty
	// value overrides this project's probe.target on every load.
	globalProbeTarget func() string
	// manualCooldown mirrors the operator's parked/released decision from
	// projects.yaml. It is pushed into the monitor on every start and applied
	// live when the management page toggles it.
	manualCooldown bool
	// lastApplied is the last node application seen by this runtime. It is
	// carried across an internal restart (a subscription refresh reload) so the
	// idle timer measures real idle time instead of restarting from zero.
	lastApplied time.Time
}

type RuntimeOption func(*Runtime)

func WithFetchCoordinator(coordinator *subscription.FetchCoordinator) RuntimeOption {
	return func(r *Runtime) { r.fetchCoordinator = coordinator }
}

func WithProbeCoordinator(coordinator *monitor.ProbeCoordinator) RuntimeOption {
	return func(r *Runtime) { r.probeCoordinator = coordinator }
}

func WithSystemProxyURL(provider func() string) RuntimeOption {
	return func(r *Runtime) { r.systemProxyURL = provider }
}

func WithGlobalProbeTarget(provider func() string) RuntimeOption {
	return func(r *Runtime) { r.globalProbeTarget = provider }
}

// applyGlobalProbeTarget lets the workspace-wide latency target win over the
// project's own probe.target. An empty value keeps the project's choice.
func (r *Runtime) applyGlobalProbeTarget(cfg *config.Config) {
	if r == nil || cfg == nil || r.globalProbeTarget == nil {
		return
	}
	if target := strings.TrimSpace(r.globalProbeTarget()); target != "" {
		cfg.Probe.Target = target
	}
}

// SetProbeTarget hot-applies a latency target to a running project. A stopped
// project needs nothing: the override is applied whenever its config is loaded.
func (r *Runtime) SetProbeTarget(target string) {
	r.mu.RLock()
	boxMgr := r.boxMgr
	cfg := r.cfg
	r.mu.RUnlock()
	if boxMgr == nil {
		return
	}
	skipCertVerify := cfg != nil && cfg.SkipCertVerify
	boxMgr.SetProbeTarget(target, skipCertVerify)
}

func NewRuntime(parent context.Context, id, configPath, sharedPath string, sharedCfg *config.Config, sharedMu *sync.RWMutex, clashAPIPort uint16, ports *PortRegistry, opts ...RuntimeOption) *Runtime {
	if parent == nil {
		parent = context.Background()
	}
	buffer := monitor.NewLogBuffer(64 * 1024)
	runtime := &Runtime{
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
	for _, opt := range opts {
		if opt != nil {
			opt(runtime)
		}
	}
	return runtime
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
	cfg, err := config.LoadProjectReadOnlyWithShared(r.configPath, r.sharedCfg)
	if r.sharedMu != nil {
		r.sharedMu.RUnlock()
	}
	if err != nil {
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("加载项目 %q 的配置失败: %w", r.id, err)
	}
	cfg.ClashAPIPort = r.clashAPIPort
	// The workspace-wide latency target wins over the project's own probe.target.
	r.applyGlobalProbeTarget(cfg)
	// Reconcile restored node ports against the process-wide registry before
	// claiming this project. The OS may report a port as free while another
	// stopped project still has it assigned in its persisted state, so node-port
	// restoration DID pass those reserved ports even though they are not
	// currently listening — otherwise a newly added node steals a stopped
	// sibling's port and that sibling fails to bind on its next start.
	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		if err := cfg.NormalizePersistedPortsRespecting(r.ports.ReservedPorts(r.id)); err != nil {
			r.setStatus(StatusFailed, err.Error())
			return fmt.Errorf("规范化项目 %q 的节点端口失败: %w", r.id, err)
		}
	}
	if err := r.ports.Reserve(r.id, cfg); err != nil {
		r.setStatus(StatusFailed, err.Error())
		return err
	}
	store, err := state.Open(cfg.FilePath())
	if err != nil {
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("打开项目 %q 的状态库失败: %w", r.id, err)
	}
	ctx, cancel := context.WithCancel(r.parentCtx)
	r.mu.RLock()
	cooldown := monitor.CooldownSettings{
		Enabled:     cfg.Cooldown.EnabledOrDefault(),
		Duration:    cfg.Cooldown.EffectiveDuration(),
		Manual:      r.manualCooldown,
		LastApplied: r.lastApplied,
	}
	r.mu.RUnlock()
	monitorCfg := monitorConfigForProject(cfg, store, r.probeCoordinator, r.id, cooldown)
	boxMgr := boxmgr.New(
		cfg,
		monitorCfg,
		boxmgr.WithLogger(r.logger),
		boxmgr.WithConfigValidator(func(next *config.Config) error {
			return r.ports.Reserve(r.id, next)
		}),
		boxmgr.WithReservedPorts(func() map[uint16]struct{} {
			return r.ports.ReservedPorts(r.id)
		}),
		boxmgr.WithSharedConfig(r.sharedCfg, r.sharedMu),
	)
	if err := boxMgr.Start(ctx); err != nil {
		cancel()
		_ = boxMgr.Close()
		_ = store.Close()
		r.setStatus(StatusFailed, err.Error())
		return fmt.Errorf("启动项目 %q 失败: %w", r.id, err)
	}

	subMgr := subscription.New(cfg, boxMgr,
		subscription.WithStateStore(store),
		subscription.WithFetchCoordinator(r.fetchCoordinator),
		subscription.WithFetchOwner(r.id),
		subscription.WithSystemProxyURL(r.systemProxyURL),
		// The fallback borrows this project's own proxy entry, read from the
		// live config so a reload (mode, listener port, credentials) is picked
		// up without recreating the subscription manager.
		subscription.WithProjectProxyURL(func() string {
			return subscription.ProjectProxyURL(boxMgr.CurrentConfigSnapshot())
		}),
	)
	subMgr.Start()
	trafficDone := make(chan struct{})

	r.mu.Lock()
	r.cfg = cfg
	r.stateStore = store
	r.boxMgr = boxMgr
	r.subMgr = subMgr
	r.cancel = cancel
	r.trafficDone = trafficDone
	r.status = StatusRunning
	r.lastError = ""
	r.startedAt = time.Now()
	r.mu.Unlock()
	go func() {
		defer close(trafficDone)
		collectTraffic(ctx, monitorCfg.TrafficAPI, store)
	}()
	r.logger.Infof("项目运行时已启动")
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

func monitorConfigForProject(cfg *config.Config, store *state.Store, probeCoordinator *monitor.ProbeCoordinator, probeOwner string, cooldown monitor.CooldownSettings) monitor.Config {
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
		ProbeCoordinator: probeCoordinator,
		ProbeOwner:       probeOwner,
		StickyNode:       cfg.Sticky.FixedNode,
		SkipCertVerify:   cfg.SkipCertVerify,
		TrafficAPI:       fmt.Sprintf("http://127.0.0.1:%d/traffic", cfg.ClashAPIPort),
		Cooldown:         cooldown,
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
	trafficDone := r.trafficDone
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if trafficDone != nil {
		<-trafficDone
	}
	if subMgr != nil {
		subMgr.Stop()
	}
	var closeErr error
	if boxMgr != nil {
		// Remember when an application last used this project before the monitor
		// that tracks it goes away, so an internal restart (a subscription
		// refresh reload) does not reset the idle timer to zero.
		if monitorMgr := boxMgr.MonitorManager(); monitorMgr != nil {
			if applied := monitorMgr.LastAppliedAt(); !applied.IsZero() {
				r.mu.Lock()
				r.lastApplied = applied
				r.mu.Unlock()
			}
		}
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
	r.trafficDone = nil
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
		return fmt.Errorf("项目 %q 未运行", r.id)
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

func (r *Runtime) setSubscriptionReferences(selected, excluded []string) {
	r.mu.RLock()
	cfg := r.cfg
	boxMgr := r.boxMgr
	r.mu.RUnlock()
	apply := func(target *config.Config) {
		if target == nil {
			return
		}
		target.SelectedSubscriptions = append([]string(nil), selected...)
		target.ExcludedSubscriptions = append([]string(nil), excluded...)
	}
	apply(cfg)
	if boxMgr != nil {
		live := boxMgr.CurrentConfig()
		if live != cfg {
			apply(live)
		}
	}
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

// SetManualCooldown parks or releases the project by hand. A running project
// picks the change up immediately; a stopped one keeps it for its next start.
func (r *Runtime) SetManualCooldown(active bool) {
	r.mu.Lock()
	r.manualCooldown = active
	boxMgr := r.boxMgr
	r.mu.Unlock()
	if boxMgr == nil {
		return
	}
	if monitorMgr := boxMgr.MonitorManager(); monitorMgr != nil {
		monitorMgr.SetManualCooldown(active)
	}
}

// ManualCooldown reports the persisted operator decision, even while stopped.
func (r *Runtime) ManualCooldown() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manualCooldown
}

// CooldownStatus reports the live cool-down state. A stopped project has no
// probing to pause, so only the persisted manual intent is reported.
func (r *Runtime) CooldownStatus() monitor.CooldownStatus {
	r.mu.RLock()
	boxMgr := r.boxMgr
	manual := r.manualCooldown
	lastApplied := r.lastApplied
	r.mu.RUnlock()
	if boxMgr != nil {
		if monitorMgr := boxMgr.MonitorManager(); monitorMgr != nil {
			return monitorMgr.CooldownStatus()
		}
	}
	return monitor.CooldownStatus{Manual: manual, LastApplied: lastApplied}
}

// ResetIdleTimer restarts the idle countdown. An explicit operator start is a
// fresh usage window: the project was just switched on, so it should not be
// treated as idle since whenever it was last used.
func (r *Runtime) ResetIdleTimer() {
	r.mu.Lock()
	r.lastApplied = time.Time{}
	r.mu.Unlock()
}

type runtimeLogger struct {
	logger *log.Logger
}

func newRuntimeLogger(projectID string, buffer io.Writer) *runtimeLogger {
	return &runtimeLogger{logger: log.New(io.MultiWriter(log.Writer(), buffer), "[project:"+projectID+"] ", log.LstdFlags|log.Lshortfile)}
}

func (l *runtimeLogger) Infof(format string, args ...any) { l.logger.Printf("信息 "+format, args...) }
func (l *runtimeLogger) Warnf(format string, args ...any) { l.logger.Printf("警告 "+format, args...) }
func (l *runtimeLogger) Errorf(format string, args ...any) {
	l.logger.Printf("错误 "+format, args...)
}
