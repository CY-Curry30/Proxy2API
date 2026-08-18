package project

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"Proxy2API/internal/boxmgr"
	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/subscription"
)

const sharedCatalogID = "__shared_catalog__"

// CatalogRuntime makes the shared node/subscription catalog independently
// operable. It has no persistent node-state store and does not run automatic
// probes or automatic subscription refreshes.
type CatalogRuntime struct {
	parentCtx        context.Context
	sharedCfg        *config.Config
	sharedMu         *sync.RWMutex
	ports            *PortRegistry
	syncShared       func() error
	probeCoordinator *monitor.ProbeCoordinator

	lifecycleMu sync.Mutex
	mu          sync.RWMutex
	cfg         *config.Config
	boxMgr      *boxmgr.Manager
	subMgr      *subscription.Manager
	subView     monitor.SubscriptionRefresher
	cancel      context.CancelFunc
}

type CatalogOption func(*CatalogRuntime)

func WithCatalogProbeCoordinator(coordinator *monitor.ProbeCoordinator) CatalogOption {
	return func(r *CatalogRuntime) { r.probeCoordinator = coordinator }
}

func NewCatalogRuntime(parent context.Context, shared *config.Config, sharedMu *sync.RWMutex, ports *PortRegistry, syncShared func() error, opts ...CatalogOption) *CatalogRuntime {
	if parent == nil {
		parent = context.Background()
	}
	runtime := &CatalogRuntime{parentCtx: parent, sharedCfg: shared, sharedMu: sharedMu, ports: ports, syncShared: syncShared}
	for _, opt := range opts {
		if opt != nil {
			opt(runtime)
		}
	}
	return runtime
}

func (r *CatalogRuntime) Start() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.RLock()
	alreadyRunning := r.boxMgr != nil
	r.mu.RUnlock()
	if alreadyRunning {
		return nil
	}
	if r.sharedCfg == nil || r.ports == nil {
		return errors.New("共享目录未初始化")
	}

	listenerPort, err := r.ports.NextAvailable(49152)
	if err != nil {
		return fmt.Errorf("分配共享目录探测端口: %w", err)
	}
	if err := r.ports.Claim(sharedCatalogID, listenerPort, "shared catalog probe listener"); err != nil {
		return err
	}
	claimed := true
	defer func() {
		if claimed {
			r.ports.Release(sharedCatalogID)
		}
	}()
	clashPort, err := r.ports.NextAvailable(listenerPort + 1)
	if err != nil {
		return fmt.Errorf("分配共享目录内部端口: %w", err)
	}
	if err := r.ports.Claim(sharedCatalogID, clashPort, "shared catalog traffic API"); err != nil {
		return err
	}

	cfg := r.catalogConfig(listenerPort, clashPort)
	ctx, cancel := context.WithCancel(r.parentCtx)
	monitorCfg := monitor.Config{
		Enabled:          false,
		ProbeTarget:      cfg.ProbeTargetOrDefault(),
		ProbeInterval:    cfg.ProbeIntervalOrDefault(),
		ProbeTimeout:     cfg.ProbeTimeoutOrDefault(),
		ProbeConcurrency: cfg.ProbeConcurrencyOrDefault(),
		ProbeCoordinator: r.probeCoordinator,
		ProbeOwner:       sharedCatalogID,
		SkipCertVerify:   cfg.SkipCertVerify,
	}
	boxMgr := boxmgr.New(
		cfg,
		monitorCfg,
		boxmgr.WithSharedConfig(r.sharedCfg, r.sharedMu),
		boxmgr.WithAutomaticHealthChecks(false),
	)
	if err := boxMgr.Start(ctx); err != nil {
		_ = boxMgr.Close()
		log.Printf("[共享目录] 探测运行时不可用，增删改查服务仍保持在线: %v", err)
		fallback := *cfg
		fallback.Nodes = nil
		boxMgr = boxmgr.New(
			&fallback,
			monitorCfg,
			boxmgr.WithSharedConfig(r.sharedCfg, r.sharedMu),
			boxmgr.WithAutomaticHealthChecks(false),
		)
		if fallbackErr := boxMgr.Start(ctx); fallbackErr != nil {
			cancel()
			_ = boxMgr.Close()
			return fmt.Errorf("启动共享目录管理运行时: %w", errors.Join(err, fallbackErr))
		}
		cfg = &fallback
	}
	// Use the catalog clone so its refresh flag is always false. The shared
	// config pointer remains the source owner for CRUD, but project fetches must
	// never drive the catalog's runtime state.
	subMgr := subscription.New(cfg, boxMgr)
	subMgr.Start()
	view := monitor.SubscriptionRefresher(subMgr)
	if r.syncShared != nil {
		view = &catalogSubscriptionRefresher{inner: subMgr, syncShared: r.syncShared}
	}

	r.mu.Lock()
	r.cfg = cfg
	r.boxMgr = boxMgr
	r.subMgr = subMgr
	r.subView = view
	r.cancel = cancel
	r.mu.Unlock()
	claimed = false
	return nil
}

func (r *CatalogRuntime) Stop() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.Lock()
	boxMgr := r.boxMgr
	subMgr := r.subMgr
	cancel := r.cancel
	r.cfg = nil
	r.boxMgr = nil
	r.subMgr = nil
	r.subView = nil
	r.cancel = nil
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
	r.ports.Release(sharedCatalogID)
	return closeErr
}

func (r *CatalogRuntime) ReloadFromShared() error {
	r.mu.RLock()
	boxMgr := r.boxMgr
	cfg := r.cfg
	subMgr := r.subMgr
	r.mu.RUnlock()
	if boxMgr == nil || cfg == nil {
		return r.Start()
	}
	next := r.catalogConfig(cfg.Listener.Port, cfg.ClashAPIPort)
	if err := boxMgr.ReloadWithPortMap(next, boxMgr.CurrentPortMap()); err != nil {
		return err
	}
	if subMgr != nil {
		subMgr.UpdateConfig(next.Subscriptions, false, next.SubscriptionRefresh.Interval)
	}
	r.mu.Lock()
	r.cfg = next
	r.mu.Unlock()
	return nil
}

// ReloadIfRunning updates an already-open catalog without turning a shared
// config change into an implicit catalog startup.
func (r *CatalogRuntime) ReloadIfRunning() error {
	r.mu.RLock()
	running := r.boxMgr != nil
	r.mu.RUnlock()
	if !running {
		return nil
	}
	return r.ReloadFromShared()
}

func (r *CatalogRuntime) Binding() (monitor.ProjectBinding, error) {
	if err := r.Start(); err != nil {
		return monitor.ProjectBinding{}, err
	}
	r.mu.RLock()
	boxMgr := r.boxMgr
	subView := r.subView
	r.mu.RUnlock()
	if boxMgr == nil || boxMgr.MonitorManager() == nil {
		return monitor.ProjectBinding{}, errors.New("共享目录运行时未就绪")
	}
	return monitor.ProjectBinding{
		ID:                    sharedCatalogID,
		Name:                  "全局目录",
		CatalogOnly:           true,
		Config:                boxMgr.CurrentConfig(),
		SharedConfig:          r.sharedCfg,
		SharedConfigMu:        r.sharedMu,
		Monitor:               boxMgr.MonitorManager(),
		NodeManager:           boxMgr,
		SubscriptionRefresher: subView,
		LogBuffer:             monitor.SharedLogBuffer,
	}, nil
}

func (r *CatalogRuntime) catalogConfig(listenerPort, clashPort uint16) *config.Config {
	if r.sharedMu != nil {
		r.sharedMu.RLock()
		defer r.sharedMu.RUnlock()
	}
	cloned := *r.sharedCfg
	cloned.Nodes = append([]config.NodeConfig(nil), r.sharedCfg.Nodes...)
	cloned.Subscriptions = append([]string(nil), r.sharedCfg.Subscriptions...)
	cloned.DisabledSubscriptions = append([]string(nil), r.sharedCfg.DisabledSubscriptions...)
	cloned.Mode = "pool"
	cloned.Listener.Address = "0.0.0.0"
	cloned.Listener.Port = listenerPort
	cloned.Listener.Username = ""
	cloned.Listener.Password = ""
	cloned.Sticky.Enabled = false
	cloned.Sticky.FixedNode = ""
	cloned.ClashAPIPort = clashPort
	cloned.SubscriptionRefresh.Enabled = false
	if cloned.SubscriptionRefresh.Interval <= 0 {
		cloned.SubscriptionRefresh.Interval = time.Hour
	}
	disabled := false
	cloned.Management.Enabled = &disabled
	return &cloned
}

type catalogSubscriptionRefresher struct {
	inner      monitor.SubscriptionRefresher
	syncShared func() error
}

func (r *catalogSubscriptionRefresher) sync(err error) error {
	if r.syncShared == nil {
		return err
	}
	return errors.Join(err, r.syncShared())
}

func (r *catalogSubscriptionRefresher) RefreshNow() error {
	return r.sync(r.inner.RefreshNow())
}
func (r *catalogSubscriptionRefresher) Status() monitor.SubscriptionStatus {
	return r.inner.Status()
}
func (r *catalogSubscriptionRefresher) Subscriptions() []monitor.SubscriptionInfo {
	return r.inner.Subscriptions()
}
func (r *catalogSubscriptionRefresher) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	r.inner.UpdateConfig(urls, enabled, interval)
	_ = r.syncShared()
}
func (r *catalogSubscriptionRefresher) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	return r.sync(r.inner.UpdateConfigAndRefresh(urls, enabled, interval))
}
func (r *catalogSubscriptionRefresher) UpdateConfigAndRefreshSelected(urls []string, enabled bool, interval time.Duration, refreshURLs []string) error {
	return r.sync(r.inner.UpdateConfigAndRefreshSelected(urls, enabled, interval, refreshURLs))
}
func (r *catalogSubscriptionRefresher) SetSubscriptionEnabled(rawURL string, enabled bool) error {
	return r.sync(r.inner.SetSubscriptionEnabled(rawURL, enabled))
}
