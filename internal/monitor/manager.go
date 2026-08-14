package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Proxy2API/internal/state"

	M "github.com/sagernet/sing/common/metadata"
)

// Config mirrors user settings needed by the monitoring server.
type Config struct {
	Enabled          bool
	StateStore       *state.Store
	Listen           string
	ProbeTarget      string
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	Password         string
	ProxyUsername    string // 代理池的用户名（用于导出）
	ProxyPassword    string // 代理池的密码（用于导出）
	ExternalIP       string // 外部 IP 地址，用于导出时替换 0.0.0.0
	SkipCertVerify   bool   // 全局跳过 SSL 证书验证
	ProbeConcurrency int    // 并发探测线程数（批量探测与周期健康检查共用）
	StickyNode       string // 粘性入口指定节点；空值默认选择最低延迟节点
	TrafficAPI       string // Project-specific sing-box Clash traffic endpoint.
}

// NodeInfo is static metadata about a proxy entry.
type NodeInfo struct {
	ID              string `json:"id"`
	Order           int    `json:"-"`
	Tag             string `json:"tag"`
	Name            string `json:"name"`
	URI             string `json:"uri"`
	Mode            string `json:"mode"`
	ListenAddress   string `json:"listen_address,omitempty"`
	Port            uint16 `json:"port,omitempty"`
	Username        string `json:"-"`
	Password        string `json:"-"`
	SubscriptionURL string `json:"-"`
	Source          string `json:"-"`
	Suppressed      bool   `json:"-"`
}

// TimelineEvent represents a single usage event for debug tracking.
type TimelineEvent struct {
	Time      time.Time `json:"time"`
	Success   bool      `json:"success"`
	LatencyMs int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}

const maxTimelineSize = 20

const probeTimeoutParts = 5

// Snapshot is a runtime view of a proxy node.
type Snapshot struct {
	NodeInfo
	IP                string          `json:"ip,omitempty"`
	Region            string          `json:"region,omitempty"`
	Country           string          `json:"country,omitempty"`
	FailureCount      int             `json:"failure_count"` // client request connection failures
	SuccessCount      int64           `json:"success_count"` // client request connection successes
	Blacklisted       bool            `json:"blacklisted"`
	BlacklistedUntil  time.Time       `json:"blacklisted_until"`
	ActiveConnections int32           `json:"active_connections"`
	LastError         string          `json:"last_error,omitempty"`
	LastFailure       time.Time       `json:"last_failure,omitempty"`
	LastSuccess       time.Time       `json:"last_success,omitempty"`
	LastProbeLatency  time.Duration   `json:"last_probe_latency,omitempty"`
	LastLatencyMs     int64           `json:"last_latency_ms"`
	Available         bool            `json:"available"`
	InitialCheckDone  bool            `json:"initial_check_done"`
	Suppressed        bool            `json:"suppressed,omitempty"`
	Timeline          []TimelineEvent `json:"timeline,omitempty"`
}

// ProbeResult contains connectivity information discovered by a probe.
type ProbeResult struct {
	Latency           time.Duration
	ConnectivityOK    bool
	ConnectivityError string
	IP                string
	Region            string
	Country           string
	TraceOK           bool
	TraceError        string
	TraceAttempts     int
}

type probeFunc func(ctx context.Context, report func(ProbeResult)) (ProbeResult, error)
type releaseFunc func()

type EntryHandle struct {
	ref *entry
}

type entry struct {
	info              NodeInfo
	ip                string
	region            string
	country           string
	failure           int
	success           int64
	timeline          []TimelineEvent
	blacklist         bool
	until             time.Time
	availabilityEpoch uint64
	consecutiveFails  int
	lastError         string
	lastFail          time.Time
	lastOK            time.Time
	lastProbe         time.Duration
	active            atomic.Int32
	probe             probeFunc
	release           releaseFunc
	blacklistFn       func(time.Duration)
	initialCheckDone  bool
	available         bool
	suppressed        bool
	store             *state.Store
	mu                sync.RWMutex
}

// Manager aggregates all node states for the UI/API.
type Manager struct {
	cfg              Config
	probeDst         M.Socksaddr
	probeHost        string // probe target hostname (TLS SNI when probeTLS is true)
	probePath        string // HTTP request path, including the query string
	probeTLS         bool   // whether the probe target uses HTTPS
	probeTLSInsecure bool   // global HTTPS certificate verification setting
	probeReady       bool
	probeConcurrency int
	probeInterval    time.Duration
	probeTimeout     time.Duration
	stickyNode       string
	entryExits       map[uint16]string
	probeScheduleCh  chan struct{}
	periodicOnce     sync.Once
	mu               sync.RWMutex
	nodes            map[string]*entry
	ctx              context.Context
	cancel           context.CancelFunc
	logger           Logger
	stateStore       *state.Store
	restoredNodes    map[string]state.NodeRecord
	recoveredCount   atomic.Int32
	asyncMu          sync.Mutex
	asyncStopped     bool
	asyncWG          sync.WaitGroup

	// Sweep progress for the WebUI. probeSweepActive is 1 while probeAllNodes
	// runs; the counters let the dashboard show a live "初始化探测中 3200/8363".
	probeSweepActive atomic.Int32
	probeSweepTotal  atomic.Int32
	probeSweepDone   atomic.Int32
	probeSweepOK     atomic.Int32
	probeSweepFail   atomic.Int32

	// probeGate serializes health-check sweeps. probeAllNodes is triggered from
	// several places concurrently (boot, the configured ticker, and post-reload
	// ProbeAllNow); without this gate two sweeps overlap, corrupt the shared
	// progress counters, and — because each one clears probeSweepActive on
	// return — leave the flag flapping so the dashboard progress bar reappears
	// forever. The gate enforces single-flight and coalesces any trigger that
	// arrives mid-sweep into exactly one follow-up pass (so a reload's newly
	// registered nodes still get probed).
	probeGate        sync.Mutex
	sweepRunning     bool
	rerunRequested   bool
	rerunPendingOnly bool
}

// ProbeSweepProgress reports the current health-check sweep progress. active is
// true only while a sweep is running.
func (m *Manager) ProbeSweepProgress() (active bool, done, total, ok, failed int) {
	return m.probeSweepActive.Load() == 1,
		int(m.probeSweepDone.Load()),
		int(m.probeSweepTotal.Load()),
		int(m.probeSweepOK.Load()),
		int(m.probeSweepFail.Load())
}

// Logger interface for logging
type Logger interface {
	Info(args ...any)
	Warn(args ...any)
}

// clampProbeConcurrency is the single source of truth for the periodic probe
// worker count: 0/unset → 32 default, then bounded to [8, 1024]. The high
// ceiling lets large inventories (thousands of nodes) finish the initial sweep
// in minutes instead of ~an hour; fd use is ~2× the worker count, well within a
// raised nofile limit. Used by every write path (NewManager, SetProbeConcurrency)
// so batch and periodic probes can never disagree on the ceiling.
func clampProbeConcurrency(n int) int {
	if n <= 0 {
		n = 32
	}
	if n < 8 {
		n = 8
	}
	if n > 1024 {
		n = 1024
	}
	return n
}

// resolveProbeTarget derives the destination and complete HTTP request target.
// Bare host[:port] values remain supported and use /generate_204 by default.
func resolveProbeTarget(probeTarget string, skipCertVerify bool) (dst M.Socksaddr, host, path string, useTLS, tlsInsecure, ready bool) {
	target := strings.TrimSpace(probeTarget)
	if target == "" {
		return M.Socksaddr{}, "", "", false, false, false
	}
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	parsed, err := url.Parse(target)
	if err != nil || parsed.Hostname() == "" {
		return M.Socksaddr{}, "", "", false, false, false
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return M.Socksaddr{}, "", "", false, false, false
	}
	host = parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	portNumber := parsePort(port)
	if portNumber == 0 {
		return M.Socksaddr{}, "", "", false, false, false
	}

	path = parsed.EscapedPath()
	if path == "" {
		path = "/generate_204"
	}
	if parsed.RawQuery != "" {
		path += "?" + parsed.RawQuery
	}
	useTLS = scheme == "https"
	return M.ParseSocksaddrHostPort(host, portNumber), host, path, useTLS, skipCertVerify, true
}

// NewManager constructs a manager and pre-validates the probe target.
func NewManager(cfg Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 5 * time.Minute
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 110 * time.Second
	}
	m := &Manager{
		cfg:              cfg,
		nodes:            make(map[string]*entry),
		ctx:              ctx,
		cancel:           cancel,
		probeConcurrency: clampProbeConcurrency(cfg.ProbeConcurrency),
		probeInterval:    cfg.ProbeInterval,
		probeTimeout:     cfg.ProbeTimeout,
		stickyNode:       strings.TrimSpace(cfg.StickyNode),
		entryExits:       make(map[uint16]string),
		probeScheduleCh:  make(chan struct{}, 1),
		stateStore:       cfg.StateStore,
		restoredNodes:    make(map[string]state.NodeRecord),
	}
	if cfg.StateStore != nil {
		restored, err := cfg.StateStore.LoadNodes()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("load node runtime state: %w", err)
		}
		m.restoredNodes = restored
	}
	m.probeDst, m.probeHost, m.probePath, m.probeTLS, m.probeTLSInsecure, m.probeReady = resolveProbeTarget(cfg.ProbeTarget, cfg.SkipCertVerify)
	return m, nil
}

// SetLogger sets the logger for the manager.
func (m *Manager) SetLogger(logger Logger) {
	m.logger = logger
}

// SetProbeConcurrency updates the worker limit used by periodic health checks.
// Called when the live config changes so WebUI edits apply after a reload.
func (m *Manager) SetProbeConcurrency(n int) {
	n = clampProbeConcurrency(n)
	m.mu.Lock()
	m.probeConcurrency = n
	m.mu.Unlock()
}

// ProbeConcurrency returns the current periodic-probe worker limit (clamped).
func (m *Manager) ProbeConcurrency() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.probeConcurrency
}

// SetProbeSchedule updates the automatic sweep interval and per-node timeout.
func (m *Manager) SetProbeSchedule(interval, timeout time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if timeout <= 0 {
		timeout = 110 * time.Second
	}
	m.mu.Lock()
	m.probeInterval = interval
	m.probeTimeout = timeout
	m.mu.Unlock()
	select {
	case m.probeScheduleCh <- struct{}{}:
	default:
	}
}

func (m *Manager) probeSchedule() (time.Duration, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.probeInterval, m.probeTimeout
}

// ProbeAttemptTimeout returns the timeout for one primary request or one
// Trace attempt. The configured per-node budget is split into five equal
// parts: one primary request plus four Trace attempts.
func (m *Manager) ProbeAttemptTimeout() time.Duration {
	_, total := m.probeSchedule()
	if total <= 0 {
		total = 110 * time.Second
	}
	return total / probeTimeoutParts
}

// ProbeAfterRelease verifies a node after its blacklist expires or is manually
// cleared. The release path marks the node unavailable before this starts, so it
// cannot re-enter routing until both required checks pass.
func (m *Manager) ProbeAfterRelease(tag string) {
	_, timeout := m.probeSchedule()
	if !m.beginAsyncWork() {
		return
	}
	go func() {
		defer m.asyncWG.Done()
		ctx, cancel := context.WithTimeout(m.ctx, timeout)
		defer cancel()

		_, err := m.ProbeWithResult(ctx, tag)
		if err != nil && m.logger != nil {
			m.logger.Warn("post-release probe failed for ", tag, ": ", err)
		}
	}()
}

// SetStickyNode selects the only node the sticky listener may use. An empty tag
// restores lowest-latency selection for new sticky bindings.
func (m *Manager) SetStickyNode(tag string) {
	m.mu.Lock()
	m.stickyNode = strings.TrimSpace(tag)
	m.mu.Unlock()
}

func (m *Manager) StickyNode() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stickyNode
}

// RecordEntryExit records the most recent successful upstream selected by a
// shared listener. The key is the configured listener port, so WebUI labels
// automatically follow settings changes after a reload.
func (m *Manager) RecordEntryExit(port uint16, tag string) {
	tag = strings.TrimSpace(tag)
	if port == 0 || tag == "" {
		return
	}
	m.mu.Lock()
	m.entryExits[port] = tag
	m.mu.Unlock()
}

func (m *Manager) EntryExits() map[uint16]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[uint16]string, len(m.entryExits))
	for port, tag := range m.entryExits {
		result[port] = tag
	}
	return result
}

// SetSubscriptionEnabled suppresses or restores every registered node owned
// by one subscription without deleting its runtime state.
func (m *Manager) SetSubscriptionEnabled(rawURL string, enabled bool) {
	m.mu.RLock()
	entries := make([]*entry, 0)
	for _, e := range m.nodes {
		e.mu.RLock()
		owned := e.info.SubscriptionURL == rawURL
		e.mu.RUnlock()
		if owned {
			entries = append(entries, e)
		}
	}
	m.mu.RUnlock()
	for _, e := range entries {
		e.mu.Lock()
		e.suppressed = !enabled
		e.persistLocked(true)
		e.mu.Unlock()
	}
}

func (m *Manager) HasSubscriptionNodes(rawURL string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.nodes {
		e.mu.RLock()
		owned := e.info.SubscriptionURL == rawURL
		e.mu.RUnlock()
		if owned {
			return true
		}
	}
	return false
}

// SetProbeTarget re-derives the probe destination and strict-TLS decision from
// the live config so WebUI changes to probe_target / skip_cert_verify take
// effect after a reload without a full process restart. The monitor Manager is
// a long-lived singleton, so without this the startup-time target/TLS mode
// would persist until the process restarts.
func (m *Manager) SetProbeTarget(probeTarget string, skipCertVerify bool) {
	dst, host, path, useTLS, tlsInsecure, ready := resolveProbeTarget(probeTarget, skipCertVerify)
	m.mu.Lock()
	m.probeDst, m.probeHost, m.probePath = dst, host, path
	m.probeTLS, m.probeTLSInsecure, m.probeReady = useTLS, tlsInsecure, ready
	m.mu.Unlock()
}

// StartPeriodicHealthCheck starts a background goroutine that periodically checks all nodes.
// interval: how often to check (e.g., 30 * time.Second)
// timeout: timeout for each probe (e.g., 10 * time.Second)
func (m *Manager) StartPeriodicHealthCheck(interval, timeout time.Duration) {
	m.SetProbeSchedule(interval, timeout)
	m.periodicOnce.Do(func() {
		if !m.beginAsyncWork() {
			return
		}
		go func() {
			defer m.asyncWG.Done()
			// Startup only arms the timer. Initial probing is manual; automatic
			// probing begins after the first configured interval elapses.
			currentInterval, _ := m.probeSchedule()
			timer := time.NewTimer(currentInterval)
			defer timer.Stop()

			for {
				select {
				case <-m.ctx.Done():
					return
				case <-timer.C:
					_, currentTimeout := m.probeSchedule()
					m.probeAllNodes(currentTimeout)
					currentInterval, _ = m.probeSchedule()
					timer.Reset(currentInterval)
				case <-m.probeScheduleCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					currentInterval, _ = m.probeSchedule()
					timer.Reset(currentInterval)
				}
			}
		}()
	})

	if m.logger != nil {
		m.logger.Info("periodic health check started, interval: ", interval)
	}
}

// ProbeAllNow triggers a one-time health check on all nodes (e.g. after reload).
func (m *Manager) ProbeAllNow(timeout time.Duration) {
	if !m.beginAsyncWork() {
		return
	}
	defer m.asyncWG.Done()
	m.probeNodes(timeout, false)
}

// ProbePendingNow checks only nodes without a restored/previous health result.
// Subscription refreshes use it so unchanged nodes keep their project-local
// state instead of being needlessly re-probed after every reload.
func (m *Manager) ProbePendingNow(timeout time.Duration) {
	if !m.beginAsyncWork() {
		return
	}
	defer m.asyncWG.Done()
	m.probeNodes(timeout, true)
}

func (m *Manager) beginAsyncWork() bool {
	m.asyncMu.Lock()
	defer m.asyncMu.Unlock()
	if m.asyncStopped {
		return false
	}
	m.asyncWG.Add(1)
	return true
}

// probeAllNodes runs a health-check sweep, but only one at a time. If a sweep is
// already in flight, the request is coalesced: exactly one additional sweep runs
// after the current one finishes, so triggers that arrive mid-sweep (e.g. a
// reload registering new nodes) are honored without stacking up overlapping
// sweeps that would corrupt the shared progress counters and wedge the WebUI
// progress bar "active" flag on.
func (m *Manager) probeAllNodes(timeout time.Duration) {
	m.probeNodes(timeout, false)
}

func (m *Manager) probeNodes(timeout time.Duration, pendingOnly bool) {
	m.probeGate.Lock()
	if m.sweepRunning {
		// A sweep is running: request one follow-up pass and return. The running
		// sweep will pick this up when it drains the gate.
		hadRerun := m.rerunRequested
		m.rerunRequested = true
		if !hadRerun {
			m.rerunPendingOnly = pendingOnly
		}
		if !pendingOnly {
			m.rerunPendingOnly = false
		}
		m.probeGate.Unlock()
		return
	}
	m.sweepRunning = true
	m.probeGate.Unlock()

	currentPendingOnly := pendingOnly
	for {
		m.runProbeSweep(timeout, currentPendingOnly)

		m.probeGate.Lock()
		if m.rerunRequested {
			m.rerunRequested = false
			currentPendingOnly = m.rerunPendingOnly
			m.rerunPendingOnly = false
			m.probeGate.Unlock()
			continue // A trigger arrived mid-sweep; run exactly one more pass.
		}
		m.sweepRunning = false
		m.probeGate.Unlock()
		return
	}
}

// runProbeSweep checks all registered nodes concurrently. It is only ever
// invoked by probeAllNodes, which guarantees single-flight execution, so the
// shared probeSweep* progress counters are never written by two sweeps at once.
func (m *Manager) runProbeSweep(timeout time.Duration, pendingOnly bool) {
	m.mu.RLock()
	entries := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		e.mu.RLock()
		suppressed := e.suppressed
		initialCheckDone := e.initialCheckDone
		e.mu.RUnlock()
		if !suppressed && (!pendingOnly || !initialCheckDone) {
			entries = append(entries, e)
		}
	}
	m.mu.RUnlock()

	if len(entries) == 0 {
		return
	}

	if m.logger != nil {
		m.logger.Info("starting health check for ", len(entries), " nodes")
	}

	// Publish sweep progress for the WebUI. Reset counters, mark active, and
	// clear the active flag when the sweep returns.
	m.probeSweepTotal.Store(int32(len(entries)))
	m.probeSweepDone.Store(0)
	m.probeSweepOK.Store(0)
	m.probeSweepFail.Store(0)
	m.probeSweepActive.Store(1)
	defer m.probeSweepActive.Store(0)

	m.mu.RLock()
	workerLimit := m.probeConcurrency
	m.mu.RUnlock()
	if workerLimit < 8 {
		workerLimit = 8
	}
	sem := make(chan struct{}, workerLimit)
	var wg sync.WaitGroup
	var availableCount atomic.Int32
	var failedCount atomic.Int32

	for _, e := range entries {
		e.mu.RLock()
		probeFn := e.probe
		tag := e.info.Tag
		e.mu.RUnlock()

		if probeFn == nil {
			// A healthy node must pass both connectivity and Trace checks. Without
			// a probe function neither can be verified, so record a completed failure
			// instead of leaving the node unchecked or optimistically available.
			probeErr := errors.New("probe not available for this node")
			e.applyProbeResult(ProbeResult{}, probeErr, e.currentAvailabilityEpoch())
			failedCount.Add(1)
			m.probeSweepFail.Add(1)
			m.probeSweepDone.Add(1)
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(entry *entry, probe probeFunc, tag string) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(m.ctx, timeout)
			defer cancel()

			availabilityEpoch := entry.currentAvailabilityEpoch()
			result, err := executeProbe(ctx, probe, entry.applyProbeProgress)

			entry.mu.RLock()
			uri := entry.info.URI
			entry.mu.RUnlock()
			if err != nil {
				failedCount.Add(1)
				m.probeSweepFail.Add(1)
			} else {
				availableCount.Add(1)
				m.probeSweepOK.Add(1)
			}
			entry.applyProbeResult(result, err, availabilityEpoch)
			m.probeSweepDone.Add(1)

			if err != nil && m.logger != nil {
				m.logger.Warn("probe failed: ", FormatProbeFailure(tag, uri, err))
			}
		}(e, probeFn, tag)
	}
	wg.Wait()

	if m.logger != nil {
		m.logger.Info("health check completed: ", availableCount.Load(), " available, ", failedCount.Load(), " failed")
	}
}

// Stop stops the periodic health check.
func (m *Manager) Stop() {
	m.asyncMu.Lock()
	m.asyncStopped = true
	if m.cancel != nil {
		m.cancel()
	}
	m.asyncMu.Unlock()
	m.asyncWG.Wait()
}

func parsePort(value string) uint16 {
	p, err := strconv.Atoi(value)
	if err != nil || p <= 0 || p > 65535 {
		return 80
	}
	return uint16(p)
}

// Register ensures a node is tracked and returns its entry.
func (m *Manager) Register(info NodeInfo) *EntryHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.nodes[info.Tag]
	if !ok {
		e = &entry{
			info:       info,
			suppressed: info.Suppressed,
			timeline:   make([]TimelineEvent, 0, maxTimelineSize),
			store:      m.stateStore,
		}
		if restored, found := m.restoredNodes[info.ID]; found {
			e.restore(restored)
			m.recoveredCount.Add(1)
		}
		m.nodes[info.Tag] = e
	} else {
		e.mu.Lock()
		e.info = info
		e.suppressed = info.Suppressed
		e.store = m.stateStore
		e.persistLocked(false)
		e.mu.Unlock()
	}
	return &EntryHandle{ref: e}
}

// ClearNodes removes all registered nodes. Call before re-registering
// during a config reload so stale entries don't persist in the dashboard.
func (m *Manager) ClearNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := make([]state.NodeRecord, 0, len(m.nodes))
	for _, e := range m.nodes {
		e.mu.Lock()
		if e.store != nil && e.info.ID != "" {
			records = append(records, e.stateRecordLocked())
		}
		e.store = nil
		e.mu.Unlock()
	}
	if m.stateStore != nil {
		if err := m.stateStore.SaveNodesNow(records); err != nil && m.logger != nil {
			m.logger.Warn("failed to persist node state before clearing monitor: ", err)
		}
		if restored, err := m.stateStore.LoadNodes(); err == nil {
			m.restoredNodes = restored
		} else if m.logger != nil {
			m.logger.Warn("failed to reload persisted node state: ", err)
		}
	}
	m.nodes = make(map[string]*entry)
	m.entryExits = make(map[uint16]string)
	m.recoveredCount.Store(0)
}

func (m *Manager) HasRecoveredNodes() bool {
	return m.recoveredCount.Load() > 0
}

// DestinationForProbe exposes the configured destination and HTTP target.
func (m *Manager) DestinationForProbe() (dest M.Socksaddr, host, path string, useTLS, tlsInsecure, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.probeReady {
		return M.Socksaddr{}, "", "", false, false, false
	}
	return m.probeDst, m.probeHost, m.probePath, m.probeTLS, m.probeTLSInsecure, true
}

// Snapshot returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that passed initial health check.
func (m *Manager) Snapshot() []Snapshot {
	return m.SnapshotFiltered(false)
}

// SnapshotVisible excludes nodes belonging to paused subscriptions.
func (m *Manager) SnapshotVisible() []Snapshot {
	all := m.SnapshotFiltered(false)
	visible := all[:0]
	for _, snap := range all {
		if !snap.Suppressed {
			visible = append(visible, snap)
		}
	}
	return visible
}

// SnapshotFiltered returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that have completed their initial
// health check and are currently available. This ensures the export function and
// the "healthy online" count in the WebUI use the same strict criterion: a node
// must be verified available, not merely "not yet proven unavailable".
func (m *Manager) SnapshotFiltered(onlyAvailable bool) []Snapshot {
	m.mu.RLock()
	list := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		list = append(list, e)
	}
	m.mu.RUnlock()
	snapshots := make([]Snapshot, 0, len(list))
	for _, e := range list {
		snap := e.snapshot()
		// When onlyAvailable is true, apply the same strict filter as the
		// "healthy online" statistic: InitialCheckDone && Available. This
		// excludes unchecked nodes (which the old logic optimistically included)
		// so export count matches the WebUI display.
		if onlyAvailable && (snap.Suppressed || !snap.InitialCheckDone || !snap.Available || snap.Blacklisted) {
			continue
		}
		snapshots = append(snapshots, snap)
	}
	// 按延迟排序（延迟小的在前面，未测试的排在最后）
	sort.Slice(snapshots, func(i, j int) bool {
		latencyI := snapshots[i].LastLatencyMs
		latencyJ := snapshots[j].LastLatencyMs
		// -1 表示未测试，排在最后
		if latencyI < 0 && latencyJ < 0 {
			return snapshots[i].Name < snapshots[j].Name // 都未测试时按名称排序
		}
		if latencyI < 0 {
			return false // i 未测试，排在后面
		}
		if latencyJ < 0 {
			return true // j 未测试，i 排在前面
		}
		if latencyI == latencyJ {
			return snapshots[i].Name < snapshots[j].Name // 延迟相同时按名称排序
		}
		return latencyI < latencyJ
	})
	return snapshots
}

// Probe triggers a manual health check.
// It updates the full availability state (available / initialCheckDone / lastOK /
// lastError) so that manual and batch probes are reflected in the dashboard and
// SnapshotFiltered results immediately, matching the behaviour of the periodic
// probeAllNodes loop.
func (m *Manager) Probe(ctx context.Context, tag string) (time.Duration, error) {
	result, err := m.ProbeWithResult(ctx, tag)
	return result.Latency, err
}

// ProbeWithResult triggers a manual health check and returns both latency and
// display-only metadata probe details.
func (m *Manager) ProbeWithResult(ctx context.Context, tag string) (ProbeResult, error) {
	e, err := m.entry(tag)
	if err != nil {
		return ProbeResult{}, err
	}
	if e.probe == nil {
		return ProbeResult{}, errors.New("probe not available for this node")
	}
	e.mu.RLock()
	suppressed := e.suppressed
	e.mu.RUnlock()
	if suppressed {
		return ProbeResult{}, errors.New("node belongs to a paused subscription")
	}

	availabilityEpoch := e.currentAvailabilityEpoch()
	result, err := executeProbe(ctx, e.probe, e.applyProbeProgress)

	e.applyProbeResult(result, err, availabilityEpoch)
	if err != nil {
		return result, err
	}
	return result, nil
}

// executeProbe enforces the outer deadline while retaining sub-probe progress.
// Some outbound protocols do not promptly honor context cancellation, so the
// probe still runs in a goroutine. Progress is merged synchronously before it is
// applied, which prevents an outer timeout from replacing a successful latency
// or location result with a zero-value ProbeResult.
func executeProbe(ctx context.Context, probe probeFunc, onProgress func(ProbeResult)) (ProbeResult, error) {
	type probeOutcome struct {
		result ProbeResult
		err    error
	}

	var progressMu sync.Mutex
	var latest ProbeResult
	report := func(update ProbeResult) {
		progressMu.Lock()
		latest = mergeProbeResult(latest, update)
		snapshot := latest
		progressMu.Unlock()
		if onProgress != nil {
			onProgress(snapshot)
		}
	}

	resCh := make(chan probeOutcome, 1)
	go func() {
		result, err := probe(ctx, report)
		resCh <- probeOutcome{result: result, err: err}
	}()

	select {
	case out := <-resCh:
		progressMu.Lock()
		result := mergeProbeResult(latest, out.result)
		progressMu.Unlock()
		return result, out.err
	case <-ctx.Done():
		progressMu.Lock()
		result := latest
		progressMu.Unlock()
		return result, ctx.Err()
	}
}

func mergeProbeResult(current, update ProbeResult) ProbeResult {
	if update.ConnectivityOK {
		current.ConnectivityOK = true
		current.ConnectivityError = ""
		current.Latency = update.Latency
	} else if update.ConnectivityError != "" {
		current.ConnectivityError = update.ConnectivityError
	}

	if update.TraceOK {
		current.TraceOK = true
		current.TraceError = ""
		current.IP = update.IP
		current.Region = update.Region
		current.Country = update.Country
	} else if update.TraceError != "" {
		current.TraceError = update.TraceError
	}
	if update.TraceAttempts > current.TraceAttempts {
		current.TraceAttempts = update.TraceAttempts
	}
	return current
}

// Release clears blacklist state for the given node.
func (m *Manager) Release(tag string) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	if e.release == nil {
		return errors.New("release not available for this node")
	}
	e.release()
	return nil
}

// ManualBlacklist manually blacklists a node for the given duration.
func (m *Manager) ManualBlacklist(tag string, duration time.Duration) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	e.mu.RLock()
	fn := e.blacklistFn
	e.mu.RUnlock()

	if fn != nil {
		// Blacklist in pool shared state (affects routing)
		fn(duration)
	}
	// Also mark in monitor state (affects UI display)
	e.blacklistUntil(time.Now().Add(duration))
	return nil
}

func (m *Manager) entry(tag string) (*entry, error) {
	m.mu.RLock()
	e, ok := m.nodes[tag]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("node %s not found", tag)
	}
	return e, nil
}

func (e *entry) snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	latencyMs := int64(-1)
	if e.lastProbe > 0 {
		latencyMs = e.lastProbe.Milliseconds()
		if latencyMs == 0 {
			latencyMs = 1
		}
	}

	var timelineCopy []TimelineEvent
	if len(e.timeline) > 0 {
		timelineCopy = make([]TimelineEvent, len(e.timeline))
		copy(timelineCopy, e.timeline)
	}

	return Snapshot{
		NodeInfo:          e.info,
		IP:                e.ip,
		Region:            e.region,
		Country:           e.country,
		FailureCount:      e.failure,
		SuccessCount:      e.success,
		Blacklisted:       e.blacklist,
		BlacklistedUntil:  e.until,
		ActiveConnections: e.active.Load(),
		LastError:         e.lastError,
		LastFailure:       e.lastFail,
		LastSuccess:       e.lastOK,
		LastProbeLatency:  e.lastProbe,
		LastLatencyMs:     latencyMs,
		Available:         e.available,
		InitialCheckDone:  e.initialCheckDone,
		Suppressed:        e.suppressed,
		Timeline:          timelineCopy,
	}
}

func (e *entry) restore(record state.NodeRecord) {
	e.ip = record.IP
	e.region = record.Region
	e.country = record.Country
	e.failure = record.FailureCount
	e.success = record.SuccessCount
	e.consecutiveFails = record.ConsecutiveFails
	e.blacklist = record.Blacklisted
	e.until = record.BlacklistedUntil
	e.lastError = record.LastError
	e.lastFail = record.LastFailure
	e.lastOK = record.LastSuccess
	e.lastProbe = record.LastProbeLatency
	e.initialCheckDone = record.InitialCheckDone
	e.available = record.Available && !record.Blacklisted
	e.timeline = make([]TimelineEvent, 0, maxTimelineSize)
	start := 0
	if len(record.Timeline) > maxTimelineSize {
		start = len(record.Timeline) - maxTimelineSize
	}
	for _, event := range record.Timeline[start:] {
		e.timeline = append(e.timeline, TimelineEvent{
			Time: event.Time, Success: event.Success,
			LatencyMs: event.LatencyMS, Error: event.Error,
		})
	}
}

func (e *entry) stateRecordLocked() state.NodeRecord {
	timeline := make([]state.TimelineEvent, 0, len(e.timeline))
	for _, event := range e.timeline {
		timeline = append(timeline, state.TimelineEvent{
			Time: event.Time, Success: event.Success,
			LatencyMS: event.LatencyMs, Error: event.Error,
		})
	}
	return state.NodeRecord{
		ID: e.info.ID, Name: e.info.Name, URI: e.info.URI,
		Source: e.info.Source, SubscriptionURL: e.info.SubscriptionURL,
		Port: e.info.Port, Username: e.info.Username, Password: e.info.Password,
		Disabled: e.suppressed, Order: e.info.Order, Active: true,
		IP: e.ip, Region: e.region, Country: e.country,
		FailureCount: e.failure, SuccessCount: e.success,
		ConsecutiveFails: e.consecutiveFails,
		Blacklisted:      e.blacklist, BlacklistedUntil: e.until,
		LastError: e.lastError, LastFailure: e.lastFail, LastSuccess: e.lastOK,
		LastProbeLatency: e.lastProbe, InitialCheckDone: e.initialCheckDone,
		Available: e.available, Timeline: timeline, LastSeen: time.Now().UTC(),
	}
}

func (e *entry) persistLocked(critical bool) {
	if e.store == nil || e.info.ID == "" {
		return
	}
	record := e.stateRecordLocked()
	if critical {
		if err := e.store.SaveNodeNow(record); err != nil {
			fmt.Printf("state: persist node %s: %v\n", e.info.Tag, err)
		}
		return
	}
	e.store.QueueNode(record)
}

func (e *entry) currentAvailabilityEpoch() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.availabilityEpoch
}

func (e *entry) applyProbeResult(result ProbeResult, err error, availabilityEpoch uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.persistLocked(true)
	e.initialCheckDone = true
	e.applyProbeProgressLocked(result)

	healthy := err == nil && result.ConnectivityOK && result.TraceOK
	// Probes performed during a blacklist may refresh latency and exit metadata,
	// but they must never make the node routable or clear the blacklist. Only
	// expiry or a manual release can do that, followed by a fresh full probe.
	if availabilityEpoch != e.availabilityEpoch {
		return
	}
	e.available = healthy && !e.blacklist
	if healthy {
		e.lastOK = time.Now()
		e.lastError = ""
		return
	}

	e.lastFail = time.Now()
	if err != nil {
		e.lastError = err.Error()
	} else {
		// Defensive fallback for custom probe functions returning an incomplete
		// result without an error.
		e.lastError = "probe incomplete: generate_204 and trace must both succeed"
	}
}

func (e *entry) applyProbeProgress(result ProbeResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.applyProbeProgressLocked(result)
	e.persistLocked(false)
}

func (e *entry) applyProbeProgressLocked(result ProbeResult) {
	// Each successful sub-probe updates only the data it owns. Failed checks do
	// not erase the last known-good value from the other check.
	if result.ConnectivityOK {
		e.lastProbe = result.Latency
	}
	if result.TraceOK {
		if result.IP != "" {
			e.ip = result.IP
		}
		if result.Region != "" {
			e.region = result.Region
		}
		if result.Country != "" {
			e.country = result.Country
		}
	}

	var progressErrors []string
	if result.ConnectivityError != "" {
		progressErrors = append(progressErrors, "generate_204 failed: "+result.ConnectivityError)
	}
	if result.TraceError != "" {
		progressErrors = append(progressErrors, "trace failed: "+result.TraceError)
	}
	if len(progressErrors) > 0 {
		e.initialCheckDone = true
		e.available = false
		e.lastFail = time.Now()
		e.lastError = strings.Join(progressErrors, "; ")
	}
}

func (e *entry) recordFailure(err error, consecutive int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.persistLocked(false)
	errStr := err.Error()
	e.failure++
	e.consecutiveFails = consecutive
	e.lastError = errStr
	e.lastFail = time.Now()
	e.appendTimelineLocked(false, 0, errStr)
}

func (e *entry) recordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.persistLocked(false)
	e.success++
	e.consecutiveFails = 0
	e.lastOK = time.Now()
	e.appendTimelineLocked(true, 0, "")
}

func (e *entry) appendTimelineLocked(success bool, latencyMs int64, errStr string) {
	evt := TimelineEvent{
		Time:      time.Now(),
		Success:   success,
		LatencyMs: latencyMs,
		Error:     errStr,
	}
	if len(e.timeline) >= maxTimelineSize {
		copy(e.timeline, e.timeline[1:])
		e.timeline[len(e.timeline)-1] = evt
	} else {
		e.timeline = append(e.timeline, evt)
	}
}

func (e *entry) blacklistUntil(until time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.persistLocked(true)
	e.availabilityEpoch++
	e.blacklist = true
	e.until = until
	e.consecutiveFails = 0
	e.available = false
}

func (e *entry) clearBlacklist() {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.persistLocked(true)
	e.availabilityEpoch++
	e.blacklist = false
	e.until = time.Time{}
	e.consecutiveFails = 0
	e.available = false
}

func (e *entry) incActive() {
	e.active.Add(1)
}

func (e *entry) decActive() {
	e.active.Add(-1)
}

func (e *entry) setProbe(fn probeFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.probe = fn
}

func (e *entry) setRelease(fn releaseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.release = fn
}

// RecordFailure updates failure counters and the pool's consecutive failure count.
func (h *EntryHandle) RecordFailure(err error, consecutive int) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordFailure(err, consecutive)
}

// RecordSuccess updates the last success timestamp.
func (h *EntryHandle) RecordSuccess() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordSuccess()
}

// Blacklist marks the node unavailable until the given deadline.
func (h *EntryHandle) Blacklist(until time.Time) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.blacklistUntil(until)
}

// ClearBlacklist removes the blacklist flag and keeps the node unavailable
// until the release-triggered full probe completes successfully.
func (h *EntryHandle) ClearBlacklist() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.clearBlacklist()
}

func (h *EntryHandle) RestoredPoolState() (consecutive int, blacklisted bool, until time.Time) {
	if h == nil || h.ref == nil {
		return 0, false, time.Time{}
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	return h.ref.consecutiveFails, h.ref.blacklist, h.ref.until
}

// IncActive increments the active connection counter.
func (h *EntryHandle) IncActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.incActive()
}

// DecActive decrements the active connection counter.
func (h *EntryHandle) DecActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.decActive()
}

// SetProbe assigns a probe function. The probe reports each completed sub-check
// so successful partial data is persisted even if the overall deadline expires.
func (h *EntryHandle) SetProbe(fn func(ctx context.Context, report func(ProbeResult)) (ProbeResult, error)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setProbe(fn)
}

// SetRelease assigns a release function.
func (h *EntryHandle) SetRelease(fn func()) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setRelease(fn)
}

// SetBlacklistFn assigns a manual blacklist function.
func (h *EntryHandle) SetBlacklistFn(fn func(time.Duration)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.blacklistFn = fn
	h.ref.mu.Unlock()
}

// MarkInitialCheckDone marks the initial health check as completed.
func (h *EntryHandle) MarkInitialCheckDone(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.initialCheckDone = true
	h.ref.available = available
	h.ref.persistLocked(false)
	h.ref.mu.Unlock()
}

// MarkAvailable updates the availability status.
func (h *EntryHandle) MarkAvailable(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.available = available
	h.ref.persistLocked(false)
	h.ref.mu.Unlock()
}

func (h *EntryHandle) Suppressed() bool {
	if h == nil || h.ref == nil {
		return false
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	return h.ref.suppressed
}

// Healthy reports whether the node has completed a probe and both required
// checks succeeded. Blacklist eligibility is maintained by the pool's shared
// state and is checked separately during member selection.
func (h *EntryHandle) Healthy() bool {
	if h == nil || h.ref == nil {
		return false
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	return h.ref.initialCheckDone && h.ref.available && !h.ref.suppressed
}

// LastLatency returns the last measured probe latency.
// Returns 0 if no measurement is available yet.
func (h *EntryHandle) LastLatency() time.Duration {
	if h == nil || h.ref == nil {
		return 0
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	if !h.ref.available {
		return 0
	}
	return h.ref.lastProbe
}
