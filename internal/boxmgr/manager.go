package boxmgr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"Proxy2API/internal/builder"
	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/outbound/pool"
	"Proxy2API/internal/state"

	"github.com/sagernet/sing-box"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
)

// Ensure Manager implements monitor.NodeManager.
var _ monitor.NodeManager = (*Manager)(nil)

const (
	defaultDrainTimeout       = 10 * time.Second
	defaultHealthCheckTimeout = 2 * time.Minute
	healthCheckPollInterval   = 500 * time.Millisecond
)

// Logger defines logging interface for the manager.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Option configures the Manager.
type Option func(*Manager)

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(m *Manager) { m.logger = l }
}

// WithConfigValidator installs a project-aware validation hook. It runs before
// listeners are replaced so a cross-project port conflict cannot interrupt the
// currently active instance.
func WithConfigValidator(validate func(*config.Config) error) Option {
	return func(m *Manager) { m.configValidator = validate }
}

// WithSharedConfig supplies the standalone node/subscription catalog used by
// config-node CRUD while m.cfg continues to own project runtime settings.
func WithSharedConfig(shared *config.Config, mu *sync.RWMutex) Option {
	return func(m *Manager) {
		m.sharedCfg = shared
		m.sharedMu = mu
	}
}

// WithAutomaticHealthChecks controls background and post-reload probes. It is
// disabled by the global shared catalog, where probes are manual only.
func WithAutomaticHealthChecks(enabled bool) Option {
	return func(m *Manager) { m.automaticHealthChecks = enabled }
}

// Manager owns the lifecycle of the active sing-box instance.
type Manager struct {
	mu sync.RWMutex

	currentBox    *box.Box
	monitorMgr    *monitor.Manager
	monitorServer *monitor.Server
	cfg           *config.Config
	sharedCfg     *config.Config
	sharedMu      *sync.RWMutex
	monitorCfg    monitor.Config
	poolState     *pool.SharedStateStore

	drainTimeout      time.Duration
	minAvailableNodes int
	logger            Logger
	configValidator   func(*config.Config) error

	baseCtx               context.Context
	healthCheckStarted    bool
	automaticHealthChecks bool
	closeOnce             sync.Once
	closeErr              error
}

// New creates a BoxManager with the given config.
func New(cfg *config.Config, monitorCfg monitor.Config, opts ...Option) *Manager {
	m := &Manager{
		cfg:                   cfg,
		monitorCfg:            monitorCfg,
		poolState:             pool.NewSharedStateStore(),
		automaticHealthChecks: true,
	}
	m.applyConfigSettings(cfg)
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	if m.drainTimeout <= 0 {
		m.drainTimeout = defaultDrainTimeout
	}
	return m
}

// Start creates and starts the initial sing-box instance.
func (m *Manager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.ensureMonitor(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	if m.cfg == nil {
		m.mu.Unlock()
		return errors.New("代理核心管理器缺少配置")
	}
	if m.currentBox != nil {
		m.mu.Unlock()
		return errors.New("sing-box 已在运行")
	}
	m.applyConfigSettings(m.cfg)
	m.baseCtx = ctx
	cfg := m.cfg
	m.mu.Unlock()
	if err := m.validateConfig(cfg); err != nil {
		return err
	}

	if len(cfg.Nodes) == 0 {
		if err := m.persistActiveNodeCatalog(cfg); err != nil {
			m.logger.Warnf("保存空的活动节点目录失败: %v", err)
		}
		m.mu.Lock()
		monitorMgr := m.monitorMgr
		startHealthCheck := m.automaticHealthChecks && monitorMgr != nil && !m.healthCheckStarted
		if startHealthCheck {
			m.healthCheckStarted = true
		}
		m.mu.Unlock()
		if monitorMgr != nil {
			monitorMgr.SetProbeSchedule(cfg.ProbeIntervalOrDefault(), cfg.ProbeTimeoutOrDefault())
			if startHealthCheck {
				monitorMgr.StartPeriodicHealthCheck(cfg.ProbeIntervalOrDefault(), cfg.ProbeTimeoutOrDefault())
			}
		}
		m.logger.Warnf("当前没有活动节点，管理和订阅服务仍可使用")
		return nil
	}

	// Try to start, with automatic port conflict resolution
	var instance *box.Box
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		var err error
		instance, err = m.createBox(ctx, cfg)
		if err != nil {
			return err
		}
		if err = instance.Start(); err != nil {
			_ = instance.Close()
			// Check if it's a port conflict error
			if conflictPort := extractPortFromBindError(err); conflictPort > 0 {
				m.logger.Warnf("端口 %d 已被占用，正在重新分配并重试...", conflictPort)
				if reassigned := reassignConflictingPort(cfg, conflictPort); reassigned {
					m.poolState.Reset() // Reset only this project's state for rebuild
					continue
				}
			}
			return fmt.Errorf("启动 sing-box 失败: %w", err)
		}
		break // Success
	}

	m.mu.Lock()
	m.currentBox = instance
	m.mu.Unlock()
	m.poolState.ActivateRestoredBlacklists()
	if err := m.persistActiveNodeCatalog(cfg); err != nil {
		m.logger.Warnf("保存活动节点目录失败: %v", err)
	}

	// Start periodic health check after nodes are registered
	m.mu.Lock()
	monitorMgr := m.monitorMgr
	startHealthCheck := m.automaticHealthChecks && monitorMgr != nil && !m.healthCheckStarted
	if startHealthCheck {
		m.healthCheckStarted = true
	}
	m.mu.Unlock()
	if monitorMgr != nil {
		monitorMgr.SetProbeSchedule(cfg.ProbeIntervalOrDefault(), cfg.ProbeTimeoutOrDefault())
		if startHealthCheck {
			monitorMgr.StartPeriodicHealthCheck(cfg.ProbeIntervalOrDefault(), cfg.ProbeTimeoutOrDefault())
		}
		if m.automaticHealthChecks && (!startHealthCheck || monitorMgr.HasRecoveredNodes()) {
			go monitorMgr.ProbeAllNow(cfg.ProbeTimeoutOrDefault())
		}
	}

	m.logger.Infof("sing-box 实例已启动，共 %d 个节点", len(cfg.Nodes))

	return nil
}

// Reload gracefully switches to a new configuration.
// For multi-port mode, we must stop the old instance first to release ports.
func (m *Manager) Reload(newCfg *config.Config) error {
	if newCfg == nil {
		return errors.New("新配置不能为空")
	}
	if err := m.validateConfig(newCfg); err != nil {
		return err
	}

	m.mu.Lock()
	if m.currentBox == nil {
		m.mu.Unlock()
		return errors.New("管理器尚未启动")
	}
	ctx := m.baseCtx
	oldBox := m.currentBox
	oldCfg := m.cfg
	m.currentBox = nil // Mark as reloading
	m.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	m.logger.Infof("正在使用 %d 个节点重新加载", len(newCfg.Nodes))

	// For multi-port mode, we must close old instance first to release ports
	// This causes a brief interruption but avoids port conflicts
	if oldBox != nil {
		m.logger.Infof("正在停止旧实例以释放端口...")
		if err := oldBox.Close(); err != nil {
			m.logger.Warnf("关闭旧实例时出错: %v", err)
		}
	}

	// Give OS time to release ports
	time.Sleep(500 * time.Millisecond)

	// Reset shared state store to ensure clean state for new config
	m.poolState.Reset()

	// Clear stale monitor nodes so the dashboard reflects the new config
	if m.monitorMgr != nil {
		m.monitorMgr.ClearNodes()
	}

	// Create and start new box instance.
	//
	// A bind conflict on start is almost always transient: the old box's
	// listeners (the hybrid listener on 2323 and every per-node port on 24000+)
	// were just closed and the OS has not yet released them. The fixed sleep
	// above is a best-effort head start, not a guarantee. So on "address already
	// in use" we WAIT and retry the SAME ports, keeping every preserved port
	// stable across the refresh. Reassigning a node to a fresh port is a
	// last-resort escape hatch (only once we've waited through several attempts):
	// moving a port silently breaks clients pointed at the old one and is exactly
	// the failure this guards against.
	var instance *box.Box
	started := false
	var lastStartErr error
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		var err error
		instance, err = m.createBox(ctx, newCfg)
		if err != nil {
			m.rollbackToOldConfig(ctx, oldCfg)
			return fmt.Errorf("创建新的代理核心失败: %w", err)
		}
		if err = instance.Start(); err == nil {
			started = true
			break // Success
		}
		_ = instance.Close()
		lastStartErr = err

		conflictPort := extractPortFromBindError(err)
		if conflictPort == 0 {
			// Not a port conflict: unrecoverable by retrying. Roll back now.
			m.rollbackToOldConfig(ctx, oldCfg)
			return fmt.Errorf("启动新的代理核心失败: %w", err)
		}

		// Give the OS more time to release the just-closed listener, then retry
		// the same port assignment.
		m.logger.Warnf("第 %d/%d 次启动时端口 %d 仍被占用，等待释放后重试...", retry+1, maxRetries, conflictPort)
		time.Sleep(300 * time.Millisecond)

		// Only after we've waited through the first half of our attempts do we
		// treat the conflict as persistent (some other process owns the port)
		// and reassign the offending node. The listener port cannot be
		// reassigned this way; if it is the conflict we simply keep waiting.
		if retry >= maxRetries/2 {
			if reassignConflictingPort(newCfg, conflictPort) {
				m.logger.Warnf("端口 %d 持续被占用，已为受影响节点重新分配端口", conflictPort)
				m.poolState.Reset()
			}
		}
	}

	if !started {
		// Never commit a closed / non-running instance as the current box: doing
		// so silently kills every port (2323 and all 24000+) with no error. Fall
		// back to the previous configuration instead.
		m.logger.Errorf("新代理核心在尝试 %d 次后仍启动失败: %v", maxRetries, lastStartErr)
		m.rollbackToOldConfig(ctx, oldCfg)
		return fmt.Errorf("启动新的代理核心失败，已尝试 %d 次: %w", maxRetries, lastStartErr)
	}

	m.applyConfigSettings(newCfg)

	m.mu.Lock()
	m.currentBox = instance
	m.cfg = newCfg
	m.mu.Unlock()
	m.poolState.ActivateRestoredBlacklists()
	if err := m.persistActiveNodeCatalog(newCfg); err != nil {
		m.logger.Warnf("保存活动节点目录失败: %v", err)
	}

	// Sync config to monitor server so future WebUI settings changes target the current config pointer
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(m.cfg)
	}

	// Trigger initial health check for newly registered nodes
	if m.monitorMgr != nil {
		m.monitorMgr.SetProbeSchedule(newCfg.ProbeIntervalOrDefault(), newCfg.ProbeTimeoutOrDefault())
		if m.automaticHealthChecks {
			go m.monitorMgr.ProbePendingNow(newCfg.ProbeTimeoutOrDefault())
		}
	}

	m.logger.Infof("重新加载成功，共 %d 个节点", len(newCfg.Nodes))

	return nil
}

// rollbackToOldConfig attempts to restart with the previous configuration.
func (m *Manager) rollbackToOldConfig(ctx context.Context, oldCfg *config.Config) {
	if oldCfg == nil {
		return
	}
	m.logger.Warnf("正在回滚到上一份配置...")
	if err := m.validateConfig(oldCfg); err != nil {
		m.logger.Errorf("回滚配置验证失败: %v", err)
		return
	}
	m.poolState.Reset()
	if m.monitorMgr != nil {
		m.monitorMgr.ClearNodes()
	}

	// The rollback binds the same ports the failed start attempted, which may
	// still be draining. Retry with backoff so a transient bind conflict does
	// not leave the manager with no running box at all.
	var instance *box.Box
	const rollbackAttempts = 5
	for attempt := 0; attempt < rollbackAttempts; attempt++ {
		var err error
		instance, err = m.createBox(ctx, oldCfg)
		if err != nil {
			m.logger.Errorf("回滚时创建代理核心失败: %v", err)
			return
		}
		if err = instance.Start(); err == nil {
			break
		}
		_ = instance.Close()
		instance = nil
		m.logger.Warnf("第 %d/%d 次回滚启动失败: %v", attempt+1, rollbackAttempts, err)
		time.Sleep(300 * time.Millisecond)
	}
	if instance == nil {
		m.logger.Errorf("回滚时代理核心在尝试 %d 次后仍启动失败", rollbackAttempts)
		return
	}
	m.mu.Lock()
	m.currentBox = instance
	m.cfg = oldCfg
	m.mu.Unlock()
	m.poolState.ActivateRestoredBlacklists()
	if err := m.persistActiveNodeCatalog(oldCfg); err != nil {
		m.logger.Warnf("回滚后恢复活动节点目录失败: %v", err)
	}
	// Sync config pointer to monitor server after rollback
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(m.cfg)
	}
	m.logger.Infof("回滚成功")
}

func (m *Manager) validateConfig(cfg *config.Config) error {
	if m.configValidator == nil {
		return nil
	}
	if err := m.configValidator(cfg); err != nil {
		return fmt.Errorf("验证项目配置失败: %w", err)
	}
	return nil
}

// Close terminates the active instance and auxiliary components.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.RLock()
		instance := m.currentBox
		server := m.monitorServer
		monitorMgr := m.monitorMgr
		m.mu.RUnlock()

		// Drain management requests first, then cancel and wait for every tracked
		// probe before the state store is closed by the app.
		if server != nil {
			server.Shutdown(context.Background())
		}
		if monitorMgr != nil {
			monitorMgr.Stop()
		}
		if instance != nil {
			m.closeErr = instance.Close()
		}
		m.poolState.Reset()
		if monitorMgr != nil {
			monitorMgr.ClearNodes()
		}

		m.mu.Lock()
		m.currentBox = nil
		m.monitorServer = nil
		m.monitorMgr = nil
		m.healthCheckStarted = false
		m.baseCtx = nil
		m.mu.Unlock()
	})
	return m.closeErr
}

func (m *Manager) persistActiveNodeCatalog(cfg *config.Config) error {
	if cfg == nil || m.monitorCfg.StateStore == nil {
		return nil
	}
	if m.monitorMgr == nil {
		return errors.New("监控管理器不可用")
	}
	snapshots := m.monitorMgr.Snapshot()
	records := make([]state.NodeRecord, 0, len(snapshots))
	for _, snapshot := range snapshots {
		timeline := make([]state.TimelineEvent, 0, len(snapshot.Timeline))
		for _, event := range snapshot.Timeline {
			timeline = append(timeline, state.TimelineEvent{
				Time: event.Time, Success: event.Success,
				LatencyMS: event.LatencyMs, Error: event.Error,
			})
		}
		records = append(records, state.NodeRecord{
			ID: snapshot.ID, Name: snapshot.Name, URI: snapshot.URI,
			Source: snapshot.Source, SubscriptionURL: snapshot.SubscriptionURL,
			Port: snapshot.Port, Username: snapshot.Username, Password: snapshot.Password,
			Disabled: snapshot.Suppressed, Order: snapshot.Order, Active: true,
			IP: snapshot.IP, Region: snapshot.Region, Country: snapshot.Country,
			FailureCount: snapshot.FailureCount, SuccessCount: snapshot.SuccessCount,
			Blacklisted: snapshot.Blacklisted, BlacklistedUntil: snapshot.BlacklistedUntil,
			LastError: snapshot.LastError, LastFailure: snapshot.LastFailure,
			LastSuccess: snapshot.LastSuccess, LastProbeLatency: snapshot.LastProbeLatency,
			InitialCheckDone: snapshot.InitialCheckDone, Available: snapshot.Available,
			Timeline: timeline,
		})
	}
	return m.monitorCfg.StateStore.ReconcileActiveNodes(records, cfg.Subscriptions)
}

// MonitorManager returns the shared monitor manager.
func (m *Manager) MonitorManager() *monitor.Manager {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.monitorMgr
}

// MonitorServer returns the monitor HTTP server.
func (m *Manager) MonitorServer() *monitor.Server {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.monitorServer
}

// newBoxRecover wraps box.New and converts panics into errors. Some malformed
// subscription nodes make the sing-box library panic during outbound
// initialization (e.g. stringifying an unexpected *string while formatting its
// own error) instead of returning an error. Without this guard such a node
// crashes the whole process and the service never starts.
func newBoxRecover(opts box.Options) (instance *box.Box, err error) {
	defer func() {
		if r := recover(); r != nil {
			instance = nil
			err = fmt.Errorf("sing-box 初始化时发生异常: %v", r)
		}
	}()
	return box.New(opts)
}

// createBox builds a sing-box instance from config.
// It retries automatically when individual outbounds fail sing-box validation,
// removing the offending outbound each time.
func (m *Manager) createBox(ctx context.Context, cfg *config.Config) (*box.Box, error) {
	if cfg == nil {
		return nil, errors.New("配置不能为空")
	}
	if m.monitorMgr == nil {
		return nil, errors.New("监控管理器尚未初始化")
	}

	opts, err := builder.Build(cfg)
	if err != nil {
		return nil, fmt.Errorf("构建 sing-box 选项失败: %w", err)
	}

	maxRetries := len(cfg.Nodes)*3 + 50 // Dynamically scale retries to configuration size
	outboundErrRe := regexp.MustCompile(`initialize outbound\[(\d+)\]`)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		inboundRegistry := include.InboundRegistry()
		outboundRegistry := include.OutboundRegistry()
		pool.Register(outboundRegistry)
		endpointRegistry := include.EndpointRegistry()
		dnsRegistry := include.DNSTransportRegistry()
		serviceRegistry := include.ServiceRegistry()

		boxCtx := box.Context(ctx, inboundRegistry, outboundRegistry, endpointRegistry, dnsRegistry, serviceRegistry)
		boxCtx = monitor.ContextWith(boxCtx, m.monitorMgr)
		boxCtx = pool.ContextWithSharedStateStore(boxCtx, m.poolState)

		instance, err := newBoxRecover(box.Options{Context: boxCtx, Options: opts})
		if err == nil {
			if attempt > 0 {
				log.Printf("✅ 移除 %d 个无效出站后已创建 sing-box 实例", attempt)
			}
			return instance, nil
		}

		// Check if this is an outbound initialization error we can recover from
		matches := outboundErrRe.FindStringSubmatch(err.Error())
		if matches == nil {
			return nil, fmt.Errorf("创建 sing-box 实例失败: %w", err)
		}

		idx, convErr := strconv.Atoi(matches[1])
		if convErr != nil || idx < 0 || idx >= len(opts.Outbounds) {
			return nil, fmt.Errorf("创建 sing-box 实例失败: %w", err)
		}

		badTag := opts.Outbounds[idx].Tag
		log.Printf("⚠️  出站 %q 未通过 sing-box 验证: %v（将移除并重试）", badTag, err)

		// Remove the offending outbound
		opts.Outbounds = append(opts.Outbounds[:idx], opts.Outbounds[idx+1:]...)

		// Clean up pool outbounds that contained this tag
		var newOutbounds []option.Outbound
		var removedPoolTags []string
		for _, ob := range opts.Outbounds {
			if ob.Type == pool.Type {
				if poolOpts, ok := ob.Options.(*pool.Options); ok {
					poolOpts.Members = removeFromSlice(poolOpts.Members, badTag)
					delete(poolOpts.Metadata, badTag)

					// If the pool is now empty, remove it to avoid another validation error
					if len(poolOpts.Members) == 0 {
						log.Printf("⚠️  正在移除空节点池 %q", ob.Tag)
						removedPoolTags = append(removedPoolTags, ob.Tag)
						continue // skip adding this empty pool
					}
				}
			}
			newOutbounds = append(newOutbounds, ob)
		}
		opts.Outbounds = newOutbounds

		// Also remove any routes that pointed to the removed pools or the badTag
		if (len(removedPoolTags) > 0 || badTag != "") && opts.Route != nil {
			removedSet := make(map[string]bool)
			for _, t := range removedPoolTags {
				removedSet[t] = true
			}
			removedSet[badTag] = true

			var newRules []option.Rule
			for _, r := range opts.Route.Rules {
				// We expect DefaultRules in our builder
				if r.Type == C.RuleTypeDefault {
					outboundTarget := r.DefaultOptions.RuleAction.RouteOptions.Outbound
					if !removedSet[outboundTarget] {
						newRules = append(newRules, r)
					} else {
						// Remove this rule since it points to a deleted outbound
					}
				} else {
					newRules = append(newRules, r)
				}
			}
			opts.Route.Rules = newRules
		}
	}

	return nil, fmt.Errorf("创建 sing-box 实例失败：无效出站过多（已超过 %d 次重试）", maxRetries)
}

// gracefulSwitch swaps the current box with a new one.
func (m *Manager) gracefulSwitch(newBox *box.Box) error {
	if newBox == nil {
		return errors.New("新的代理核心实例为空")
	}

	m.mu.Lock()
	old := m.currentBox
	m.currentBox = newBox
	drainTimeout := m.drainTimeout
	m.mu.Unlock()

	if old != nil {
		go m.drainOldBox(old, drainTimeout)
	}

	m.logger.Infof("已切换到新实例，旧实例将在 %s 内完成连接排空", drainTimeout)
	return nil
}

// drainOldBox waits for drain timeout then closes the old box.
func (m *Manager) drainOldBox(oldBox *box.Box, timeout time.Duration) {
	if oldBox == nil {
		return
	}
	if timeout > 0 {
		time.Sleep(timeout)
	}
	if err := oldBox.Close(); err != nil {
		m.logger.Errorf("关闭旧实例失败: %v", err)
		return
	}
	m.logger.Infof("旧实例已在 %s 排空后关闭", timeout)
}

// removeFromSlice removes an element from a string slice.
func removeFromSlice(slice []string, element string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != element {
			result = append(result, s)
		}
	}
	return result
}

// waitForHealthCheck polls until enough nodes are available or timeout.
func (m *Manager) waitForHealthCheck(timeout time.Duration) error {
	if m.monitorMgr == nil || m.minAvailableNodes <= 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(healthCheckPollInterval)
	defer ticker.Stop()

	for {
		available, total := m.availableNodeCount()
		if available >= m.minAvailableNodes {
			m.logger.Infof("健康检查通过：%d/%d 个节点可用", available, total)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("等待超时：%d/%d 个节点可用（至少需要 %d 个）", available, total, m.minAvailableNodes)
		}
		<-ticker.C
	}
}

// availableNodeCount returns (available, total) node counts.
func (m *Manager) availableNodeCount() (int, int) {
	if m.monitorMgr == nil {
		return 0, 0
	}
	snapshots := m.monitorMgr.SnapshotVisible()
	total := len(snapshots)
	available := 0
	for _, snap := range snapshots {
		if snap.InitialCheckDone && snap.Available {
			available++
		}
	}
	return available, total
}

// ensureMonitor initializes monitor manager and server if needed.
func (m *Manager) ensureMonitor(ctx context.Context) error {
	m.mu.Lock()
	if m.monitorMgr != nil {
		m.mu.Unlock()
		return nil
	}

	monitorMgr, err := monitor.NewManager(m.monitorCfg)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("初始化监控管理器失败: %w", err)
	}
	monitorMgr.SetLogger(monitorLoggerAdapter{logger: m.logger})
	m.monitorMgr = monitorMgr

	var serverToStart *monitor.Server
	if m.monitorCfg.Enabled {
		if m.monitorServer == nil {
			serverToStart = monitor.NewServer(m.monitorCfg, monitorMgr, log.Default())
			m.monitorServer = serverToStart
		}
		// Set config early so WebUI has data before Start() completes
		if m.monitorServer != nil && m.cfg != nil {
			m.monitorServer.SetConfig(m.cfg)
		}
		// Set NodeManager for config CRUD endpoints
		if m.monitorServer != nil {
			m.monitorServer.SetNodeManager(m)
		}
		// Note: StartPeriodicHealthCheck is called after nodes are registered in Start()
	}
	m.mu.Unlock()

	if serverToStart != nil {
		serverToStart.Start(ctx)
	}
	return nil
}

// applyConfigSettings extracts runtime settings from config.
func (m *Manager) applyConfigSettings(cfg *config.Config) {
	if cfg == nil {
		return
	}
	if cfg.SubscriptionRefresh.DrainTimeout > 0 {
		m.drainTimeout = cfg.SubscriptionRefresh.DrainTimeout
	} else if m.drainTimeout == 0 {
		m.drainTimeout = defaultDrainTimeout
	}
	m.minAvailableNodes = cfg.SubscriptionRefresh.MinAvailableNodes
}

// defaultLogger is the fallback logger using standard log.
type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[boxmgr] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[代理核心] 警告: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[代理核心] 错误: "+format, args...)
}

// monitorLoggerAdapter adapts Logger to monitor.Logger interface.
type monitorLoggerAdapter struct {
	logger Logger
}

func (a monitorLoggerAdapter) Info(args ...any) {
	if a.logger != nil {
		a.logger.Infof("%s", fmt.Sprint(args...))
	}
}

func (a monitorLoggerAdapter) Warn(args ...any) {
	if a.logger != nil {
		a.logger.Warnf("%s", fmt.Sprint(args...))
	}
}

// --- NodeManager interface implementation ---

var errConfigUnavailable = errors.New("配置尚未初始化")

func (m *Manager) sourceConfigLocked() *config.Config {
	if m.sharedCfg != nil {
		return m.sharedCfg
	}
	return m.cfg
}

// ListConfigNodes returns the effective nodes loaded by this runtime. A project
// manager therefore exposes only that project's nodes, while the catalog
// runtime exposes the complete source catalog.
func (m *Manager) ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.sharedMu != nil {
		m.sharedMu.RLock()
		defer m.sharedMu.RUnlock()
	}

	cfg := m.cfg
	if cfg == nil {
		return nil, errConfigUnavailable
	}
	return cloneNodes(cfg.Nodes), nil
}

// CreateNode adds a new node to the config and saves it.
func (m *Manager) CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedMu != nil {
		m.sharedMu.Lock()
		defer m.sharedMu.Unlock()
	}

	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	normalized, err := m.prepareNodeLocked(node, "")
	if err != nil {
		return config.NodeConfig{}, err
	}

	// A node added through the WebUI is an explicit user configuration, so it is
	// always persisted as an inline node in config.yaml — regardless of whether
	// subscriptions are configured. Classifying it as a subscription/file source
	// would route it to nodes.txt, which the next subscription refresh overwrites
	// (createNewConfig preserves only inline nodes), silently losing the node.
	normalized.Source = config.NodeSourceInline

	cfg.Nodes = append(cfg.Nodes, normalized)
	if err := cfg.Save(); err != nil {
		cfg.Nodes = cfg.Nodes[:len(cfg.Nodes)-1]
		return config.NodeConfig{}, fmt.Errorf("保存配置失败: %w", err)
	}
	return normalized, nil
}

// ImportConfigNodes atomically appends parsed nodes, skipping duplicate
// endpoints and assigning unique display names when imported names collide.
func (m *Manager) ImportConfigNodes(ctx context.Context, nodes []config.NodeConfig) ([]config.NodeConfig, int, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	}
	if len(nodes) == 0 {
		return nil, 0, fmt.Errorf("%w: 没有可导入的节点", monitor.ErrInvalidNode)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedMu != nil {
		m.sharedMu.Lock()
		defer m.sharedMu.Unlock()
	}
	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return nil, 0, errConfigUnavailable
	}

	backup := cloneNodes(cfg.Nodes)
	existingKeys := make(map[string]struct{}, len(cfg.Nodes)+len(nodes))
	for idx := range cfg.Nodes {
		existingKeys[cfg.Nodes[idx].NodeKey()] = struct{}{}
	}

	added := make([]config.NodeConfig, 0, len(nodes))
	skipped := 0
	for _, node := range nodes {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				cfg.Nodes = backup
				return nil, skipped, err
			}
		}
		node.URI = strings.TrimSpace(node.URI)
		if !config.IsProxyURI(node.URI) {
			skipped++
			continue
		}
		key := node.NodeKey()
		if _, exists := existingKeys[key]; exists {
			skipped++
			continue
		}

		if strings.TrimSpace(node.Name) == "" {
			node.Name = config.ExtractNodeName(node.URI)
		}
		if strings.TrimSpace(node.Name) == "" {
			node.Name = "node"
		}
		node.Name = m.uniqueNodeNameLocked(node.Name)
		normalized, err := m.prepareNodeLocked(node, "")
		if err != nil {
			skipped++
			continue
		}
		normalized.Source = config.NodeSourceInline
		cfg.Nodes = append(cfg.Nodes, normalized)
		existingKeys[key] = struct{}{}
		added = append(added, normalized)
	}

	if len(added) == 0 {
		return []config.NodeConfig{}, skipped, nil
	}
	if err := cfg.Save(); err != nil {
		cfg.Nodes = backup
		return nil, skipped, fmt.Errorf("保存导入节点失败: %w", err)
	}
	return added, skipped, nil
}

// UpdateNode updates an existing node by name and saves the config.
func (m *Manager) UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	name = strings.TrimSpace(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedMu != nil {
		m.sharedMu.Lock()
		defer m.sharedMu.Unlock()
	}

	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	idx := m.nodeIndexLocked(name)
	if idx == -1 {
		return config.NodeConfig{}, monitor.ErrNodeNotFound
	}

	normalized, err := m.prepareNodeLocked(node, name)
	if err != nil {
		return config.NodeConfig{}, err
	}

	// Preserve the original source
	normalized.Source = cfg.Nodes[idx].Source

	prev := cfg.Nodes[idx]
	cfg.Nodes[idx] = normalized
	if err := cfg.Save(); err != nil {
		cfg.Nodes[idx] = prev
		return config.NodeConfig{}, fmt.Errorf("保存配置失败: %w", err)
	}
	return normalized, nil
}

// DeleteNode removes a node by name and saves the config.
func (m *Manager) DeleteNode(ctx context.Context, name string) error {
	_, err := m.DeleteNodes(ctx, []string{name})
	return err
}

// DeleteNodes removes multiple nodes atomically and saves the config once.
func (m *Manager) DeleteNodes(ctx context.Context, names []string) (int, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}

	requested := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return 0, fmt.Errorf("%w: 节点名称不能为空", monitor.ErrInvalidNode)
		}
		requested[name] = struct{}{}
	}
	if len(requested) == 0 {
		return 0, fmt.Errorf("%w: 请选择要删除的节点", monitor.ErrInvalidNode)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedMu != nil {
		m.sharedMu.Lock()
		defer m.sharedMu.Unlock()
	}

	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return 0, errConfigUnavailable
	}

	found := make(map[string]struct{}, len(requested))
	remaining := make([]config.NodeConfig, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		if _, shouldDelete := requested[node.Name]; shouldDelete {
			found[node.Name] = struct{}{}
			continue
		}
		remaining = append(remaining, node)
	}
	for name := range requested {
		if _, exists := found[name]; !exists {
			return 0, fmt.Errorf("%w: %s", monitor.ErrNodeNotFound, name)
		}
	}

	backup := cloneNodes(cfg.Nodes)
	cfg.Nodes = remaining
	if err := cfg.Save(); err != nil {
		cfg.Nodes = backup
		return 0, fmt.Errorf("保存配置失败: %w", err)
	}
	return len(requested), nil
}

// TriggerReload reloads the sing-box instance with current config.
func (m *Manager) TriggerReload(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	m.mu.RLock()
	cfgCopy := m.copyConfigLocked()
	portMap := m.cfg.BuildPortMap() // Preserve existing port assignments
	m.mu.RUnlock()

	if cfgCopy == nil {
		return errConfigUnavailable
	}
	return m.ReloadWithPortMap(cfgCopy, portMap)
}

func (m *Manager) deactivateProxyCore(newCfg *config.Config) error {
	m.mu.Lock()
	oldBox := m.currentBox
	m.currentBox = nil
	m.cfg = newCfg
	m.mu.Unlock()

	if oldBox != nil {
		if err := oldBox.Close(); err != nil {
			m.logger.Warnf("清空节点池时关闭代理核心出错: %v", err)
		}
	}
	m.poolState.Reset()
	if m.monitorMgr != nil {
		m.monitorMgr.ClearNodes()
		m.monitorMgr.SetProbeSchedule(newCfg.ProbeIntervalOrDefault(), newCfg.ProbeTimeoutOrDefault())
	}
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(newCfg)
	}
	if err := m.persistActiveNodeCatalog(newCfg); err != nil {
		m.logger.Warnf("保存空的活动节点目录失败: %v", err)
	}
	m.applyConfigSettings(newCfg)
	m.logger.Warnf("由于没有活动节点，代理核心已停止")
	return nil
}

// ReloadWithPortMap gracefully switches to a new configuration, preserving port assignments.
func (m *Manager) ReloadWithPortMap(newCfg *config.Config, portMap map[string]uint16) error {
	if newCfg == nil {
		return errors.New("新配置不能为空")
	}
	if len(newCfg.Nodes) == 0 {
		if err := m.validateConfig(newCfg); err != nil {
			return err
		}
		return m.deactivateProxyCore(newCfg)
	}

	// Apply port mapping and assign ports. NormalizeWithPortMap preserves the
	// port of any node present in portMap and assigns fresh, collision-free
	// ports to the rest. It is always run (an empty map simply means "assign
	// all ports fresh"), since createNewConfig no longer pre-assigns them.
	if err := newCfg.NormalizeWithPortMap(portMap); err != nil {
		return fmt.Errorf("使用端口映射规范化配置失败: %w", err)
	}

	m.mu.RLock()
	inactive := m.currentBox == nil
	ctx := m.baseCtx
	m.mu.RUnlock()
	if inactive {
		m.mu.Lock()
		m.cfg = newCfg
		m.mu.Unlock()
		m.poolState.Reset()
		if m.monitorMgr != nil {
			m.monitorMgr.ClearNodes()
		}
		if err := m.Start(ctx); err != nil {
			return fmt.Errorf("从空节点池重启代理核心失败: %w", err)
		}
		if m.monitorServer != nil {
			m.monitorServer.SetConfig(newCfg)
		}
	} else if err := m.Reload(newCfg); err != nil {
		return err
	}

	// Persist the (possibly updated) assignments so a restart keeps the same
	// port per node. Best-effort: a write failure does not affect the running
	// proxy, only the next restart's ability to restore ports.
	if err := newCfg.SaveNodePortMap(); err != nil {
		m.logger.Warnf("保存节点端口失败: %v", err)
	}
	return nil
}

// CurrentPortMap returns the current port mapping from the active configuration.
func (m *Manager) CurrentPortMap() map[string]uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cfg == nil {
		return nil
	}
	return m.cfg.BuildPortMap()
}

// --- Helper functions ---

// portBindErrorRegex matches "listen tcp4 0.0.0.0:24282: bind: address already in use"
var portBindErrorRegex = regexp.MustCompile(`listen tcp[46]? [^:]+:(\d+): bind: address already in use`)

// extractPortFromBindError extracts the port number from a bind error message.
func extractPortFromBindError(err error) uint16 {
	if err == nil {
		return 0
	}
	matches := portBindErrorRegex.FindStringSubmatch(err.Error())
	if len(matches) < 2 {
		return 0
	}
	var port int
	fmt.Sscanf(matches[1], "%d", &port)
	if port > 0 && port <= 65535 {
		return uint16(port)
	}
	return 0
}

// reassignConflictingPort finds the node using the conflicting port and assigns a new port.
func reassignConflictingPort(cfg *config.Config, conflictPort uint16) bool {
	// Build set of used ports
	usedPorts := make(map[uint16]bool)
	if cfg.Mode == "hybrid" {
		usedPorts[cfg.Listener.Port] = true
	}
	for _, node := range cfg.Nodes {
		usedPorts[node.Port] = true
	}

	// Find and reassign the conflicting node
	for idx := range cfg.Nodes {
		if cfg.Nodes[idx].Port == conflictPort {
			// Find next available port
			newPort := conflictPort + 1
			address := cfg.MultiPort.Address
			if address == "" {
				address = "0.0.0.0"
			}
			for usedPorts[newPort] || !config.IsPortAvailable(address, newPort) {
				newPort++
				if newPort > 65535 {
					log.Printf("❌ 没有可分配给节点 %q 的端口", cfg.Nodes[idx].Name)
					return false
				}
			}
			log.Printf("⚠️  端口 %d 已被占用，正在将节点 %q 重新分配到端口 %d", conflictPort, cfg.Nodes[idx].Name, newPort)
			cfg.Nodes[idx].Port = newPort
			return true
		}
	}
	return false
}

func cloneNodes(nodes []config.NodeConfig) []config.NodeConfig {
	if len(nodes) == 0 {
		return []config.NodeConfig{} // Return empty slice, not nil, for proper JSON serialization
	}
	out := make([]config.NodeConfig, len(nodes))
	copy(out, nodes)
	return out
}

// CurrentConfigSnapshot returns an isolated copy of the configuration backing
// the running box. Subscription reloads use it so unrelated live settings are
// not replaced by an older manager snapshot.
func (m *Manager) CurrentConfigSnapshot() *config.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.copyConfigLocked()
}

// CurrentConfig returns the live persistable configuration object. Callers
// must use it only for the existing settings API contract, which serializes
// mutations before triggering a reload.
func (m *Manager) CurrentConfig() *config.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

// AutomaticHealthChecksEnabled reports whether this runtime may start probes
// outside explicit monitor API requests.
func (m *Manager) AutomaticHealthChecksEnabled() bool {
	return m != nil && m.automaticHealthChecks
}

func (m *Manager) copyConfigLocked() *config.Config {
	if m.cfg == nil {
		return nil
	}
	cloned := *m.cfg
	cloned.Nodes = cloneNodes(m.cfg.Nodes)
	// Clone Subscriptions slice to avoid shared backing array issues
	if len(m.cfg.Subscriptions) > 0 {
		cloned.Subscriptions = make([]string, len(m.cfg.Subscriptions))
		copy(cloned.Subscriptions, m.cfg.Subscriptions)
	}
	if len(m.cfg.DisabledSubscriptions) > 0 {
		cloned.DisabledSubscriptions = append([]string(nil), m.cfg.DisabledSubscriptions...)
	}
	if len(m.cfg.SelectedSubscriptions) > 0 {
		cloned.SelectedSubscriptions = append([]string(nil), m.cfg.SelectedSubscriptions...)
	}
	if len(m.cfg.ExcludedSubscriptions) > 0 {
		cloned.ExcludedSubscriptions = append([]string(nil), m.cfg.ExcludedSubscriptions...)
	}
	if len(m.cfg.ExcludedNodes) > 0 {
		cloned.ExcludedNodes = append([]string(nil), m.cfg.ExcludedNodes...)
	}
	cloned.SetFilePath(m.cfg.FilePath())
	return &cloned
}

func (m *Manager) nodeIndexLocked(name string) int {
	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return -1
	}
	for idx, node := range cfg.Nodes {
		if node.Name == name {
			return idx
		}
	}
	return -1
}

func (m *Manager) uniqueNodeNameLocked(name string) string {
	base := strings.TrimSpace(name)
	if base == "" {
		base = "node"
	}
	if m.nodeIndexLocked(base) == -1 {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s-%d", base, suffix)
		if m.nodeIndexLocked(candidate) == -1 {
			return candidate
		}
	}
}

func (m *Manager) portInUseLocked(port uint16, currentName string) bool {
	if port == 0 {
		return false
	}
	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return false
	}
	for _, node := range cfg.Nodes {
		if node.Name == currentName {
			continue
		}
		if node.Port == port {
			return true
		}
	}
	return false
}

func (m *Manager) nextAvailablePortLocked() uint16 {
	base := m.cfg.MultiPort.BasePort
	if base == 0 {
		base = 24000
	}
	cfg := m.sourceConfigLocked()
	if cfg == nil {
		return base
	}
	used := make(map[uint16]struct{}, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		if node.Port > 0 {
			used[node.Port] = struct{}{}
		}
	}
	port := base
	for i := 0; i < 1<<16; i++ {
		if _, ok := used[port]; !ok && port != 0 {
			return port
		}
		port++
		if port == 0 {
			port = 1
		}
	}
	return base
}

func (m *Manager) prepareNodeLocked(node config.NodeConfig, currentName string) (config.NodeConfig, error) {
	sourceCfg := m.sourceConfigLocked()
	if sourceCfg == nil || m.cfg == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}
	node.Name = strings.TrimSpace(node.Name)
	node.URI = strings.TrimSpace(node.URI)

	if node.URI == "" {
		return config.NodeConfig{}, fmt.Errorf("%w: URI 不能为空", monitor.ErrInvalidNode)
	}

	// Extract name from URI if not provided
	if node.Name == "" {
		if currentName != "" {
			node.Name = currentName
		} else {
			node.Name = config.ExtractNodeName(node.URI)
		}
		// Fallback to auto-generated name
		if node.Name == "" {
			node.Name = fmt.Sprintf("node-%d", len(sourceCfg.Nodes)+1)
		}
	}

	// Check for name conflict (excluding current node when updating)
	if idx := m.nodeIndexLocked(node.Name); idx != -1 {
		if currentName == "" || sourceCfg.Nodes[idx].Name != currentName {
			return config.NodeConfig{}, fmt.Errorf("%w: 节点 %s 已存在", monitor.ErrNodeConflict, node.Name)
		}
	}
	if m.sharedCfg != nil {
		node.Port = 0
		node.Username = ""
		node.Password = ""
		return node, nil
	}

	// Handle multi-port mode specifics
	if m.cfg.Mode == "multi-port" {
		if node.Port == 0 {
			node.Port = m.nextAvailablePortLocked()
		} else if m.portInUseLocked(node.Port, currentName) {
			return config.NodeConfig{}, fmt.Errorf("%w: 端口 %d 已被占用", monitor.ErrNodeConflict, node.Port)
		}
		if node.Username == "" {
			node.Username = m.cfg.MultiPort.Username
			node.Password = m.cfg.MultiPort.Password
		}
	}

	return node, nil
}
