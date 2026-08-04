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

	M "github.com/sagernet/sing/common/metadata"
)

// Config mirrors user settings needed by the monitoring server.
type Config struct {
	Enabled          bool
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
}

// NodeInfo is static metadata about a proxy entry.
type NodeInfo struct {
	Tag           string `json:"tag"`
	Name          string `json:"name"`
	URI           string `json:"uri"`
	Mode          string `json:"mode"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          uint16 `json:"port,omitempty"`
}

// TimelineEvent represents a single usage event for debug tracking.
type TimelineEvent struct {
	Time      time.Time `json:"time"`
	Success   bool      `json:"success"`
	LatencyMs int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}

const maxTimelineSize = 20

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
	Timeline          []TimelineEvent `json:"timeline,omitempty"`
}

// ProbeResult contains connectivity information discovered by a probe.
type ProbeResult struct {
	Latency       time.Duration
	IP            string
	Region        string
	Country       string
	TraceError    string
	TraceAttempts int
}

type probeFunc func(ctx context.Context) (ProbeResult, error)
type releaseFunc func()

type EntryHandle struct {
	ref *entry
}

type entry struct {
	info             NodeInfo
	ip               string
	region           string
	country          string
	failure          int
	success          int64
	timeline         []TimelineEvent
	blacklist        bool
	until            time.Time
	lastError        string
	lastFail         time.Time
	lastOK           time.Time
	lastProbe        time.Duration
	active           atomic.Int32
	probe            probeFunc
	release          releaseFunc
	blacklistFn      func(time.Duration)
	initialCheckDone bool
	available        bool
	mu               sync.RWMutex
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
	probeScheduleCh  chan struct{}
	periodicOnce     sync.Once
	mu               sync.RWMutex
	nodes            map[string]*entry
	ctx              context.Context
	cancel           context.CancelFunc
	logger           Logger

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
	probeGate      sync.Mutex
	sweepRunning   bool
	rerunRequested bool
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
		probeScheduleCh:  make(chan struct{}, 1),
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
		go func() {
			// Run one sweep immediately, then use the live schedule.
			_, currentTimeout := m.probeSchedule()
			m.probeAllNodes(currentTimeout)

			currentInterval, _ := m.probeSchedule()
			timer := time.NewTimer(currentInterval)
			defer timer.Stop()

			for {
				select {
				case <-m.ctx.Done():
					return
				case <-timer.C:
					_, currentTimeout = m.probeSchedule()
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
	m.probeAllNodes(timeout)
}

// probeAllNodes runs a health-check sweep, but only one at a time. If a sweep is
// already in flight, the request is coalesced: exactly one additional sweep runs
// after the current one finishes, so triggers that arrive mid-sweep (e.g. a
// reload registering new nodes) are honored without stacking up overlapping
// sweeps that would corrupt the shared progress counters and wedge the WebUI
// progress bar "active" flag on.
func (m *Manager) probeAllNodes(timeout time.Duration) {
	m.probeGate.Lock()
	if m.sweepRunning {
		// A sweep is running: request one follow-up pass and return. The running
		// sweep will pick this up when it drains the gate.
		m.rerunRequested = true
		m.probeGate.Unlock()
		return
	}
	m.sweepRunning = true
	m.probeGate.Unlock()

	for {
		m.runProbeSweep(timeout)

		m.probeGate.Lock()
		if m.rerunRequested {
			m.rerunRequested = false
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
func (m *Manager) runProbeSweep(timeout time.Duration) {
	m.mu.RLock()
	entries := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		entries = append(entries, e)
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
			// No probe function (probe target not configured): the node cannot be
			// verified, so optimistically mark it checked+available — matching the
			// old per-pool startup probe's "no target → mark available" behavior.
			// Skipping it instead would leave initialCheckDone=false forever and
			// exclude it from export and the healthy-online count.
			e.mu.Lock()
			e.initialCheckDone = true
			e.available = true
			e.mu.Unlock()
			m.probeSweepOK.Add(1)
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

			// Race the probe against its deadline. Some sing-box protocol dials
			// block inside DialContext without honoring ctx, so a direct
			// probe(ctx) call could never return — wedging this worker's
			// semaphore slot and hanging the whole sweep (wg.Wait never returns;
			// the dashboard shows a stuck init and 0 available even though the
			// nodes are reachable). Run the probe in its own goroutine and select
			// on ctx.Done() so the worker always returns within timeout. The
			// buffered channel lets the stalled goroutine deliver its result
			// later (its connection watchdog force-closes on ctx.Done) without
			// blocking on send.
			type probeOutcome struct {
				result ProbeResult
				err    error
			}
			resCh := make(chan probeOutcome, 1)
			go func() {
				result, err := probe(ctx)
				resCh <- probeOutcome{result: result, err: err}
			}()

			var result ProbeResult
			var err error
			select {
			case out := <-resCh:
				result, err = out.result, out.err
			case <-ctx.Done():
				err = ctx.Err()
			}

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
			entry.applyProbeResult(result, err)
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
	if m.cancel != nil {
		m.cancel()
	}
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
			info:     info,
			timeline: make([]TimelineEvent, 0, maxTimelineSize),
		}
		m.nodes[info.Tag] = e
	} else {
		e.info = info
	}
	return &EntryHandle{ref: e}
}

// ClearNodes removes all registered nodes. Call before re-registering
// during a config reload so stale entries don't persist in the dashboard.
func (m *Manager) ClearNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = make(map[string]*entry)
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
		if onlyAvailable && (!snap.InitialCheckDone || !snap.Available || snap.Blacklisted) {
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
	if err != nil {
		return 0, err
	}
	return result.Latency, nil
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

	// Enforce the context deadline at this level. Some sing-box outbound
	// protocols block inside DialContext without honoring ctx cancellation, so a
	// probe could otherwise never return — which in batch mode occupies a
	// semaphore slot forever and freezes the whole run (wg.Wait never returns,
	// WebUI stuck at "N/M"). Run the probe in its own goroutine and race it
	// against ctx: if ctx fires first we return a timeout error and let the
	// stuck goroutine unwind on its own (its conn watchdog force-closes on
	// ctx.Done). The result channel is buffered so that late goroutine never
	// blocks on send.
	type probeOutcome struct {
		result ProbeResult
		err    error
	}
	resCh := make(chan probeOutcome, 1)
	go func() {
		result, err := e.probe(ctx)
		resCh <- probeOutcome{result: result, err: err}
	}()

	var result ProbeResult
	select {
	case out := <-resCh:
		result, err = out.result, out.err
	case <-ctx.Done():
		err = ctx.Err()
	}

	e.applyProbeResult(result, err)
	if err != nil {
		return result, err
	}
	return result, nil
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
		Timeline:          timelineCopy,
	}
}

func (e *entry) applyProbeResult(result ProbeResult, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initialCheckDone = true
	if err != nil {
		e.lastError = err.Error()
		e.lastFail = time.Now()
		e.available = false
		return
	}
	e.lastOK = time.Now()
	e.lastProbe = result.Latency
	if result.IP != "" {
		e.ip = result.IP
	}
	if result.Region != "" {
		e.region = result.Region
	}
	if result.Country != "" {
		e.country = result.Country
	}
	e.available = true
}

func (e *entry) recordFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	errStr := err.Error()
	e.failure++
	e.lastError = errStr
	e.lastFail = time.Now()
	e.appendTimelineLocked(false, 0, errStr)
}

func (e *entry) recordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success++
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
	e.blacklist = true
	e.until = until
	e.mu.Unlock()
}

func (e *entry) clearBlacklist() {
	e.mu.Lock()
	e.blacklist = false
	e.until = time.Time{}
	e.mu.Unlock()
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

// RecordFailure updates failure counters.
func (h *EntryHandle) RecordFailure(err error) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordFailure(err)
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

// ClearBlacklist removes the blacklist flag.
func (h *EntryHandle) ClearBlacklist() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.clearBlacklist()
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

// SetProbe assigns a probe function.
func (h *EntryHandle) SetProbe(fn func(ctx context.Context) (ProbeResult, error)) {
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
	h.ref.mu.Unlock()
}

// MarkAvailable updates the availability status.
func (h *EntryHandle) MarkAvailable(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.available = available
	h.ref.mu.Unlock()
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
