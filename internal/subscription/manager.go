package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"Proxy2API/internal/boxmgr"
	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/state"
)

// Logger defines logging interface.
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

func WithStateStore(store *state.Store) Option {
	return func(m *Manager) { m.stateStore = store }
}

// WithFetchCoordinator shares remote subscription responses with other
// project runtimes in the same process.
func WithFetchCoordinator(coordinator *FetchCoordinator) Option {
	return func(m *Manager) { m.fetchCoordinator = coordinator }
}

func WithFetchOwner(owner string) Option {
	return func(m *Manager) { m.fetchOwner = owner }
}

// Manager handles periodic subscription refresh.
type Manager struct {
	mu sync.RWMutex

	baseCfg    *config.Config
	boxMgr     *boxmgr.Manager
	logger     Logger
	httpClient *http.Client // Custom HTTP client with connection pooling

	status            monitor.SubscriptionStatus
	ctx               context.Context
	cancel            context.CancelFunc
	refreshMu         sync.Mutex // prevents concurrent refreshes
	loopMu            sync.Mutex
	loopWG            sync.WaitGroup
	stopped           bool
	manualRefresh     chan struct{}
	items             map[string]monitor.SubscriptionInfo
	nodeCache         map[string][]config.NodeConfig
	stateStore        *state.Store
	fetchCoordinator  *FetchCoordinator
	fetchOwner        string
	unsubscribeFetch  func()
	fetchCallbackMu   sync.Mutex
	fetchCallbackWG   sync.WaitGroup
	fetchCallbacksOff bool

	// Track nodes.txt content hash to detect modifications
	lastSubHash      string    // Hash of nodes.txt content after last subscription refresh
	lastNodesModTime time.Time // Last known modification time of nodes.txt
}

const (
	maxSubscriptionBodySize = 10 * 1024 * 1024
)

// New creates a SubscriptionManager.
func New(cfg *config.Config, boxMgr *boxmgr.Manager, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// Create optimized HTTP client with connection pooling
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second, // Overall timeout
	}

	m := &Manager{
		baseCfg:       cfg,
		boxMgr:        boxMgr,
		ctx:           ctx,
		cancel:        cancel,
		manualRefresh: make(chan struct{}, 1),
		httpClient:    httpClient,
		items:         make(map[string]monitor.SubscriptionInfo),
		nodeCache:     make(map[string][]config.NodeConfig),
	}
	m.reconcileSubscriptionStateLocked(cfg.Subscriptions)
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	m.loadNodeCache()
	m.seedNodeCacheFromConfig()
	m.restorePersistentState()
	return m
}

func (m *Manager) restorePersistentState() {
	if m.stateStore == nil {
		return
	}
	stored, found, err := m.stateStore.LoadSubscriptionState()
	if err != nil {
		m.logger.Warnf("恢复订阅状态失败: %v", err)
		return
	}
	if !found {
		return
	}
	configured := make(map[string]struct{}, len(m.baseCfg.Subscriptions))
	for _, rawURL := range m.baseCfg.Subscriptions {
		configured[rawURL] = struct{}{}
	}
	m.status.LastRefresh = stored.LastRefresh
	m.status.NextRefresh = stored.NextRefresh
	m.status.NodeCount = stored.NodeCount
	m.status.LastError = stored.LastError
	m.status.RefreshCount = stored.RefreshCount
	for _, item := range stored.Items {
		if _, ok := configured[item.URL]; !ok {
			continue
		}
		enabled := m.baseCfg.SubscriptionEnabled(item.URL)
		status := item.Status
		included := item.Included
		if !enabled {
			status = "disabled"
			included = false
		} else if status == "disabled" {
			status = "pending"
		}
		m.items[item.URL] = monitor.SubscriptionInfo{
			ID: item.ID, URL: item.URL, Name: item.Name, Status: status,
			NodeCount: item.NodeCount, Included: included, Enabled: enabled,
			UploadBytes: item.UploadBytes, DownloadBytes: item.DownloadBytes,
			UsedBytes: item.UsedBytes, TotalBytes: item.TotalBytes,
			RemainingBytes: item.RemainingBytes, ExpiresAt: item.ExpiresAt,
			LastRefresh: item.LastRefresh, LastError: item.LastError,
		}
	}
	for rawURL, definitions := range stored.NodeCache {
		if _, ok := configured[rawURL]; !ok {
			continue
		}
		// A recovered runtime catalog was committed only after sing-box started,
		// so its per-URL nodes are newer than a possibly interrupted subscription
		// snapshot. Paused URLs have no active catalog entries and still restore
		// from the subscription snapshot below.
		if m.baseCfg.RecoveredSubscriptionURL(rawURL) && m.baseCfg.SubscriptionEnabled(rawURL) {
			continue
		}
		nodes := make([]config.NodeConfig, 0, len(definitions))
		for _, definition := range definitions {
			nodes = append(nodes, config.NodeConfig{
				StateKey: definition.ID,
				Name:     definition.Name, URI: definition.URI, Port: definition.Port,
				Username: definition.Username, Password: definition.Password,
				Source: config.NodeSourceSubscription, SubscriptionURL: rawURL,
				Disabled: definition.Disabled,
			})
		}
		m.nodeCache[rawURL] = nodes
	}
	m.reconcileSubscriptionStateLocked(m.baseCfg.Subscriptions)
}

func (m *Manager) persistentState() state.SubscriptionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := state.SubscriptionState{
		LastRefresh: m.status.LastRefresh, NextRefresh: m.status.NextRefresh,
		NodeCount: m.status.NodeCount, LastError: m.status.LastError,
		RefreshCount: m.status.RefreshCount,
		Items:        make([]state.SubscriptionInfo, 0, len(m.baseCfg.Subscriptions)),
		NodeCache:    make(map[string][]state.NodeDefinition, len(m.nodeCache)),
	}
	for _, rawURL := range m.baseCfg.Subscriptions {
		item := m.items[rawURL]
		result.Items = append(result.Items, state.SubscriptionInfo{
			ID: item.ID, URL: rawURL, Name: item.Name, Status: item.Status,
			NodeCount: item.NodeCount, Included: item.Included, Enabled: item.Enabled,
			UploadBytes: item.UploadBytes, DownloadBytes: item.DownloadBytes,
			UsedBytes: item.UsedBytes, TotalBytes: item.TotalBytes,
			RemainingBytes: item.RemainingBytes, ExpiresAt: item.ExpiresAt,
			LastRefresh: item.LastRefresh, LastError: item.LastError,
		})
		definitions := make([]state.NodeDefinition, 0, len(m.nodeCache[rawURL]))
		for _, node := range m.nodeCache[rawURL] {
			definitions = append(definitions, state.NodeDefinition{
				ID:   node.StateID(),
				Name: node.Name, URI: node.URI, Port: node.Port,
				Username: node.Username, Password: node.Password,
				Source: string(node.Source), SubscriptionURL: rawURL,
				Disabled: node.Disabled,
			})
		}
		result.NodeCache[rawURL] = definitions
	}
	return result
}

func (m *Manager) persistState(critical bool) {
	if m.stateStore == nil {
		return
	}
	value := m.persistentState()
	if critical {
		if err := m.stateStore.SaveSubscriptionStateNow(value); err != nil {
			m.logger.Warnf("保存订阅状态失败: %v", err)
		}
		return
	}
	m.stateStore.QueueSubscriptionState(value)
}

func (m *Manager) subscriptionCachePath() string {
	if configPath := m.baseCfg.FilePath(); configPath != "" {
		return filepath.Join(filepath.Dir(configPath), config.SubscriptionCacheFileName)
	}
	if m.baseCfg.NodesFile != "" {
		return filepath.Join(filepath.Dir(m.baseCfg.NodesFile), config.SubscriptionCacheFileName)
	}
	return ""
}

func (m *Manager) loadNodeCache() {
	path := m.subscriptionCachePath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			m.logger.Warnf("读取订阅缓存失败: %v", err)
		}
		return
	}
	var cached map[string][]config.NodeConfig
	if err := json.Unmarshal(data, &cached); err != nil {
		m.logger.Warnf("解析订阅缓存失败: %v", err)
		return
	}
	configured := make(map[string]struct{}, len(m.baseCfg.Subscriptions))
	for _, rawURL := range m.baseCfg.Subscriptions {
		configured[rawURL] = struct{}{}
	}
	for rawURL, nodes := range cached {
		if _, ok := configured[rawURL]; !ok {
			continue
		}
		for idx := range nodes {
			nodes[idx].Source = config.NodeSourceSubscription
			nodes[idx].SubscriptionURL = rawURL
			nodes[idx].Disabled = false
		}
		m.nodeCache[rawURL] = nodes
	}
}

func (m *Manager) seedNodeCacheFromConfig() {
	configured := make(map[string]struct{}, len(m.baseCfg.Subscriptions))
	for _, rawURL := range m.baseCfg.Subscriptions {
		configured[rawURL] = struct{}{}
	}
	fromConfig := make(map[string][]config.NodeConfig, len(configured))
	for _, node := range m.baseCfg.Nodes {
		if node.SubscriptionURL == "" {
			continue
		}
		if _, known := configured[node.SubscriptionURL]; !known {
			continue
		}
		node.Disabled = false
		fromConfig[node.SubscriptionURL] = append(fromConfig[node.SubscriptionURL], node)
	}
	for rawURL, nodes := range fromConfig {
		// Prefer the exact local startup set for future pause/resume operations.
		m.nodeCache[rawURL] = nodes
	}
	for _, rawURL := range m.baseCfg.Subscriptions {
		if !m.baseCfg.RecoveredSubscriptionURL(rawURL) || !m.baseCfg.SubscriptionEnabled(rawURL) {
			continue
		}
		if _, present := fromConfig[rawURL]; !present {
			// A committed empty set must replace a stale compatibility cache.
			delete(m.nodeCache, rawURL)
		}
	}
}

func cloneSubscriptionNodeCache(source map[string][]config.NodeConfig) map[string][]config.NodeConfig {
	cloned := make(map[string][]config.NodeConfig, len(source))
	for rawURL, nodes := range source {
		cloned[rawURL] = append([]config.NodeConfig(nil), nodes...)
	}
	return cloned
}

func (m *Manager) saveNodeCache(cache map[string][]config.NodeConfig) {
	path := m.subscriptionCachePath()
	if path == "" {
		return
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		m.logger.Warnf("编码订阅缓存失败: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		m.logger.Warnf("写入订阅缓存失败: %v", err)
	}
}

func subscriptionID(rawURL string) string {
	sum := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(sum[:8])
}

func subscriptionName(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.Hostname() != "" {
		return parsed.Hostname()
	}
	return "订阅"
}

func newSubscriptionInfo(rawURL string) monitor.SubscriptionInfo {
	return monitor.SubscriptionInfo{
		ID:      subscriptionID(rawURL),
		URL:     rawURL,
		Name:    subscriptionName(rawURL),
		Status:  "pending",
		Enabled: true,
	}
}

// reconcileSubscriptionStateLocked keeps runtime status/cache aligned with config URLs.
// The caller must hold m.mu when the manager is already visible to other goroutines.
func (m *Manager) reconcileSubscriptionStateLocked(urls []string) {
	active := make(map[string]struct{}, len(urls))
	for _, rawURL := range urls {
		active[rawURL] = struct{}{}
		if _, ok := m.items[rawURL]; !ok {
			m.items[rawURL] = newSubscriptionInfo(rawURL)
		}
		info := m.items[rawURL]
		info.Enabled = m.baseCfg.SubscriptionEnabled(rawURL)
		if !info.Enabled {
			info.Status = "disabled"
			info.Included = false
		}
		m.items[rawURL] = info
	}
	for rawURL := range m.items {
		if _, ok := active[rawURL]; !ok {
			delete(m.items, rawURL)
			delete(m.nodeCache, rawURL)
		}
	}
}

// Start begins the periodic refresh loop without fetching immediately. Manual
// refresh remains available through RefreshNow regardless of the auto flag.
func (m *Manager) Start() {
	if m.fetchCoordinator != nil && m.fetchOwner != "" && m.unsubscribeFetch == nil {
		m.unsubscribeFetch = m.fetchCoordinator.Subscribe(m.fetchOwner, func(rawURL string) {
			if !m.beginFetchCallback() {
				return
			}
			defer m.fetchCallbackWG.Done()
			m.applySharedFetch(rawURL)
		})
	}
	if len(m.baseCfg.Subscriptions) == 0 {
		m.logger.Infof("no subscriptions configured, refresh disabled")
		m.persistState(false)
		return
	}

	interval := m.baseCfg.SubscriptionRefresh.Interval
	m.logger.Infof("starting subscription manager, auto-refresh=%v, interval=%s", m.baseCfg.SubscriptionRefresh.Enabled, interval)

	m.loopMu.Lock()
	if !m.stopped {
		m.startRefreshLoopLocked(interval)
	}
	m.loopMu.Unlock()
	m.persistState(false)
}

// startRefreshLoopLocked starts one timer loop. The caller must hold loopMu.
func (m *Manager) startRefreshLoopLocked(interval time.Duration) {
	m.loopWG.Add(1)
	go func() {
		defer m.loopWG.Done()
		m.refreshLoop(interval)
	}()
}

// Stop stops the periodic refresh.
func (m *Manager) Stop() {
	m.loopMu.Lock()
	m.stopped = true
	if m.cancel != nil {
		m.cancel()
	}
	m.loopWG.Wait()
	m.loopMu.Unlock()
	if m.unsubscribeFetch != nil {
		m.unsubscribeFetch()
		m.unsubscribeFetch = nil
	}
	m.fetchCallbackMu.Lock()
	m.fetchCallbacksOff = true
	m.fetchCallbackMu.Unlock()
	// RefreshNow and synchronous configuration refreshes do not run inside the
	// timer goroutine. Shared-result callbacks are also drained before the box
	// manager and state store can be closed by the owning project runtime.
	m.refreshMu.Lock()
	m.refreshMu.Unlock()
	m.fetchCallbackWG.Wait()

	// Close idle connections
	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
	m.persistState(true)
}

func (m *Manager) beginFetchCallback() bool {
	m.fetchCallbackMu.Lock()
	defer m.fetchCallbackMu.Unlock()
	if m.fetchCallbacksOff {
		return false
	}
	m.fetchCallbackWG.Add(1)
	return true
}

// UpdateConfig hot-reloads subscription URLs and refresh settings without restart.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	m.updateConfig(urls, enabled, interval, false)
}

func (m *Manager) updateConfig(urls []string, enabled bool, interval time.Duration, triggerRefresh bool) {
	urls = append([]string(nil), urls...)
	m.mu.Lock()
	renamedFrom, renamedTo, renamed := detectSubscriptionRename(m.items, urls)
	renamedWasDisabled := false
	if renamed {
		renamedWasDisabled = !m.baseCfg.SubscriptionEnabled(renamedFrom) || !m.baseCfg.SubscriptionEnabled(renamedTo)
		if cached, ok := m.nodeCache[renamedFrom]; ok {
			m.nodeCache[renamedTo] = cached
			delete(m.nodeCache, renamedFrom)
		}
		if info, ok := m.items[renamedFrom]; ok {
			delete(m.items, renamedFrom)
			info.ID = subscriptionID(renamedTo)
			info.URL = renamedTo
			info.Name = subscriptionName(renamedTo)
			info.Enabled = !renamedWasDisabled
			if renamedWasDisabled {
				info.Status = "disabled"
				info.Included = false
			}
			m.items[renamedTo] = info
		}
	}
	m.baseCfg.Subscriptions = urls
	if renamed {
		m.baseCfg.SetSubscriptionEnabled(renamedFrom, true)
		m.baseCfg.SetSubscriptionEnabled(renamedTo, !renamedWasDisabled)
	}
	m.baseCfg.PruneDisabledSubscriptions()
	m.baseCfg.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		m.baseCfg.SubscriptionRefresh.Interval = interval
	}
	if enabled {
		m.status.NextRefresh = time.Now().Add(m.baseCfg.SubscriptionRefresh.Interval)
	} else {
		m.status.NextRefresh = time.Time{}
	}
	m.reconcileSubscriptionStateLocked(urls)
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.mu.Unlock()
	m.saveNodeCache(cacheSnapshot)
	m.persistState(true)

	// Persist to config.yaml
	if err := m.baseCfg.SaveSettings(); err != nil {
		m.logger.Errorf("保存订阅配置失败: %v", err)
	}

	// Restart the refresh loop with new settings
	m.loopMu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.loopWG.Wait()
	if !m.stopped {
		ctx, cancel := context.WithCancel(context.Background())
		m.mu.Lock()
		m.ctx = ctx
		m.cancel = cancel
		m.manualRefresh = make(chan struct{}, 1)
		m.mu.Unlock()
	}

	// Always start the refresh loop to handle the immediate refresh signal. An
	// empty URL list is also refreshed so stale subscription nodes are removed.
	m.logger.Infof("subscription config updated: %d URLs, enabled=%v, interval=%s", len(urls), enabled, m.baseCfg.SubscriptionRefresh.Interval)
	if !m.stopped {
		m.startRefreshLoopLocked(m.baseCfg.SubscriptionRefresh.Interval)
	}
	m.loopMu.Unlock()

	if triggerRefresh {
		// Always trigger an immediate fetch after an asynchronous config update,
		// regardless of the auto-refresh flag.
		select {
		case m.manualRefresh <- struct{}{}:
			m.logger.Infof("triggered immediate refresh after config update")
		default:
			// A refresh is already pending
		}
	}
}

func detectSubscriptionRename(items map[string]monitor.SubscriptionInfo, urls []string) (string, string, bool) {
	desired := make(map[string]struct{}, len(urls))
	for _, rawURL := range urls {
		desired[rawURL] = struct{}{}
	}
	removed := ""
	for rawURL := range items {
		if _, exists := desired[rawURL]; exists {
			continue
		}
		if removed != "" {
			return "", "", false
		}
		removed = rawURL
	}
	added := ""
	for _, rawURL := range urls {
		if _, exists := items[rawURL]; exists {
			continue
		}
		if added != "" {
			return "", "", false
		}
		added = rawURL
	}
	return removed, added, removed != "" && added != ""
}

// UpdateConfigAndRefresh updates subscription config and synchronously waits for
// the first refresh to complete before returning. This ensures the caller (WebUI API)
// can confirm the update took effect.
func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	return m.updateConfigAndRefresh(urls, enabled, interval, nil)
}

// UpdateConfigAndRefreshSelected updates subscription config but fetches only
// the specified subscriptions. Cached nodes from every other subscription are
// retained when the combined node set is rebuilt.
func (m *Manager) UpdateConfigAndRefreshSelected(urls []string, enabled bool, interval time.Duration, refreshURLs []string) error {
	if len(refreshURLs) == 0 {
		return m.UpdateConfigAndRefresh(urls, enabled, interval)
	}
	return m.updateConfigAndRefresh(urls, enabled, interval, append([]string(nil), refreshURLs...))
}

func (m *Manager) updateConfigAndRefresh(urls []string, enabled bool, interval time.Duration, refreshURLs []string) error {
	m.updateConfig(urls, enabled, interval, false)
	if len(refreshURLs) > 0 {
		m.doRefreshSelectedWithForce(refreshURLs, true)
	} else {
		m.doRefresh(true)
	}
	if status := m.Status(); status.LastError != "" {
		return fmt.Errorf("刷新失败: %s", status.LastError)
	}
	return nil
}

// SetSubscriptionEnabled pauses or restores one subscription using its local
// cache. It never fetches the remote subscription.
func (m *Manager) SetSubscriptionEnabled(rawURL string, enabled bool) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	m.mu.Lock()
	found := false
	for _, configuredURL := range m.baseCfg.Subscriptions {
		if configuredURL == rawURL {
			found = true
			break
		}
	}
	if !found {
		m.mu.Unlock()
		return fmt.Errorf("订阅不存在")
	}
	if enabled && len(m.nodeCache[rawURL]) == 0 {
		m.mu.Unlock()
		return fmt.Errorf("订阅没有本地节点缓存，请先手动更新")
	}
	if m.baseCfg.SubscriptionEnabled(rawURL) == enabled {
		m.mu.Unlock()
		return nil
	}
	m.baseCfg.SetSubscriptionEnabled(rawURL, enabled)
	m.reconcileSubscriptionStateLocked(m.baseCfg.Subscriptions)
	nodes := m.cachedNodesForConfigLocked()
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.mu.Unlock()

	if err := m.baseCfg.SaveSettings(); err != nil {
		return fmt.Errorf("保存订阅状态: %w", err)
	}
	m.saveNodeCache(cacheSnapshot)
	if err := m.writeNodesToFile(m.getNodesFilePath(), nodes); err != nil {
		return fmt.Errorf("保存订阅节点缓存: %w", err)
	}

	monitorMgr := m.boxMgr.MonitorManager()
	if monitorMgr != nil && monitorMgr.HasSubscriptionNodes(rawURL) {
		monitorMgr.SetSubscriptionEnabled(rawURL, enabled)
		if enabled && m.boxMgr.AutomaticHealthChecksEnabled() {
			go monitorMgr.ProbeAllNow(m.baseCfg.ProbeTimeoutOrDefault())
		}
	} else if enabled {
		// After a restart, paused nodes are not registered in the active box. Add
		// them back from the persistent per-subscription cache without fetching.
		portMap := m.boxMgr.CurrentPortMap()
		newCfg := m.createNewConfig(nodes)
		if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
			return fmt.Errorf("应用订阅状态: %w", err)
		}
	}

	m.mu.Lock()
	m.status.NodeCount = countEnabledNodes(nodes)
	m.status.LastError = ""
	m.mu.Unlock()
	m.persistState(true)
	return nil
}

// RefreshNow triggers an immediate refresh.
func (m *Manager) RefreshNow() error {
	m.doRefresh(true)
	if status := m.Status(); status.LastError != "" {
		return fmt.Errorf("刷新失败: %s", status.LastError)
	}
	return nil
}

// Status returns the current refresh status.
func (m *Manager) Status() monitor.SubscriptionStatus {
	m.mu.RLock()
	status := m.status
	m.mu.RUnlock()

	// Check if nodes have been modified since last refresh
	status.NodesModified = m.CheckNodesModified()
	return status
}

// Subscriptions returns per-subscription state in configured order.
func (m *Manager) Subscriptions() []monitor.SubscriptionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]monitor.SubscriptionInfo, 0, len(m.baseCfg.Subscriptions))
	for _, rawURL := range m.baseCfg.Subscriptions {
		info, ok := m.items[rawURL]
		if !ok {
			info = newSubscriptionInfo(rawURL)
		}
		info.Enabled = m.baseCfg.SubscriptionEnabled(rawURL)
		if !info.Enabled {
			info.Status = "disabled"
			info.Included = false
		}
		if info.NodeCount == 0 {
			info.NodeCount = len(m.nodeCache[rawURL])
		}
		result = append(result, info)
	}
	return result
}

// refreshLoop runs the periodic refresh.
func (m *Manager) refreshLoop(interval time.Duration) {
	m.mu.RLock()
	autoEnabled := m.baseCfg.SubscriptionRefresh.Enabled
	loopCtx := m.ctx
	manualRefresh := m.manualRefresh
	m.mu.RUnlock()

	firstDelay := interval
	if autoEnabled {
		m.mu.RLock()
		nextRefresh := m.status.NextRefresh
		m.mu.RUnlock()
		if !nextRefresh.IsZero() {
			firstDelay = time.Until(nextRefresh)
			if firstDelay < 0 {
				firstDelay = 0
			}
		}
	}
	timer := time.NewTimer(firstDelay)
	defer timer.Stop()

	if autoEnabled {
		// A runtime restart may have changed this project's interval. Rebase the
		// next refresh on the current project setting while retaining LastRefresh
		// and the rest of this project's historical status.
		m.mu.Lock()
		m.status.NextRefresh = time.Now().Add(interval)
		m.mu.Unlock()
		m.persistState(false)
	}

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-timer.C:
			// Only do periodic refresh when auto-refresh is enabled
			if !autoEnabled {
				timer.Reset(interval)
				continue
			}
			m.doRefresh()
			m.mu.Lock()
			m.status.NextRefresh = time.Now().Add(interval)
			m.mu.Unlock()
			m.persistState(false)
			timer.Reset(interval)
		case <-manualRefresh:
			// Always honor manual/immediate refresh regardless of enabled flag
			m.doRefresh(true)
			if autoEnabled {
				timer.Reset(interval)
				m.mu.Lock()
				m.status.NextRefresh = time.Now().Add(interval)
				m.mu.Unlock()
				m.persistState(false)
			}
		}
	}
}

// doRefresh performs a single refresh operation. The optional argument is
// true for an explicit/manual refresh and false for a timer tick.
func (m *Manager) doRefresh(force ...bool) {
	forceFetch := len(force) > 0 && force[0]
	m.doRefreshSelectedWithForce(nil, forceFetch)
}

// doRefreshSelected refreshes only the supplied URLs. A nil list preserves the
// existing full-refresh behavior used by timers and manual refresh requests.
func (m *Manager) doRefreshSelected(refreshURLs []string) {
	m.doRefreshSelectedWithForce(refreshURLs, false)
}

func (m *Manager) doRefreshSelectedWithForce(refreshURLs []string, forceFetch bool) {
	// Serialize refreshes so a configuration update that arrives during an
	// automatic refresh still gets its own completed pass.
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	m.mu.RLock()
	previousCache := cloneSubscriptionNodeCache(m.nodeCache)
	m.mu.RUnlock()
	committed := false

	m.mu.Lock()
	m.status.IsRefreshing = true
	m.mu.Unlock()

	defer func() {
		if !committed {
			m.restoreNodeCache(previousCache)
		}
		m.mu.Lock()
		m.status.IsRefreshing = false
		m.status.RefreshCount++
		m.mu.Unlock()
		m.persistState(true)
	}()

	m.logger.Infof("starting subscription refresh")

	var nodes []config.NodeConfig
	var err error
	if refreshURLs == nil {
		// Timed and manual refreshes still fetch every active subscription.
		nodes, err = m.fetchAllSubscriptions(forceFetch)
	} else {
		m.logger.Infof("refreshing %d selected subscription(s)", len(refreshURLs))
		nodes, err = m.fetchSubscriptions(refreshURLs, forceFetch)
	}
	if err != nil {
		m.logger.Errorf("获取订阅失败: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	activeNodeCount := countEnabledNodes(nodes)
	m.logger.Infof("prepared %d active nodes (%d cached total) from subscriptions", activeNodeCount, len(nodes))

	// Write subscription nodes to nodes.txt
	nodesFilePath := m.getNodesFilePath()
	if err := m.writeNodesToFile(nodesFilePath, nodes); err != nil {
		m.logger.Errorf("写入 nodes.txt 失败: %v", err)
		m.mu.Lock()
		m.status.LastError = fmt.Sprintf("写入 nodes.txt 失败: %v", err)
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}
	m.logger.Infof("written %d nodes to %s", len(nodes), nodesFilePath)

	// Update hash and mod time after writing
	newHash := m.computeNodesHash(nodes)
	m.mu.Lock()
	m.lastSubHash = newHash
	if info, err := os.Stat(nodesFilePath); err == nil {
		m.lastNodesModTime = info.ModTime()
	} else {
		m.lastNodesModTime = time.Now()
	}
	m.status.NodesModified = false
	m.mu.Unlock()

	// Get current port mapping to preserve existing node ports
	portMap := m.boxMgr.CurrentPortMap()

	// Create new config with updated nodes
	newCfg := m.createNewConfig(nodes)
	m.mu.RLock()
	allSubscriptionsPaused := len(m.baseCfg.Subscriptions) > 0 && len(m.baseCfg.ActiveSubscriptions()) == 0
	pausedURLs := append([]string(nil), m.baseCfg.DisabledSubscriptions...)
	m.mu.RUnlock()
	if activeNodeCount == 0 && allSubscriptionsPaused {
		if monitorMgr := m.boxMgr.MonitorManager(); monitorMgr != nil {
			for _, rawURL := range pausedURLs {
				monitorMgr.SetSubscriptionEnabled(rawURL, false)
			}
		}
		m.mu.Lock()
		m.status.LastRefresh = time.Now()
		m.status.NodeCount = 0
		m.status.LastError = ""
		m.mu.Unlock()
		committed = true
		m.logger.Infof("subscription refresh completed, no active subscription nodes")
		return
	}

	// Trigger BoxManager reload with port preservation
	if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
		m.logger.Errorf("重新加载失败: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	m.status.LastRefresh = time.Now()
	m.status.NodeCount = activeNodeCount
	m.status.LastError = ""
	m.mu.Unlock()
	committed = true

	m.logger.Infof("subscription refresh completed, %d nodes active", activeNodeCount)
}

func (m *Manager) restoreNodeCache(snapshot map[string][]config.NodeConfig) {
	m.mu.Lock()
	m.nodeCache = cloneSubscriptionNodeCache(snapshot)
	nodes := m.cachedNodesForConfigLocked()
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.mu.Unlock()
	m.saveNodeCache(cacheSnapshot)
	if err := m.writeNodesToFile(m.getNodesFilePath(), nodes); err != nil {
		m.logger.Warnf("刷新回滚后恢复订阅兼容文件失败: %v", err)
	}
}

// getNodesFilePath returns the path to nodes.txt.
func (m *Manager) getNodesFilePath() string {
	if m.baseCfg.NodesFile != "" {
		return m.baseCfg.NodesFile
	}
	return filepath.Join(filepath.Dir(m.baseCfg.FilePath()), "nodes.txt")
}

// writeNodesToFile writes nodes to a file (one URI per line).
func (m *Manager) writeNodesToFile(path string, nodes []config.NodeConfig) error {
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// computeNodesHash computes a hash of node URIs for change detection.
func (m *Manager) computeNodesHash(nodes []config.NodeConfig) string {
	var uris []string
	for _, node := range nodes {
		uris = append(uris, node.URI)
	}
	content := strings.Join(uris, "\n")
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

// CheckNodesModified checks if nodes.txt has been modified since last refresh.
// Uses file modification time as a fast path to avoid unnecessary file reads.
func (m *Manager) CheckNodesModified() bool {
	m.mu.RLock()
	lastHash := m.lastSubHash
	lastMod := m.lastNodesModTime
	m.mu.RUnlock()

	if lastHash == "" {
		return false // No previous refresh, can't determine modification
	}

	nodesFilePath := m.getNodesFilePath()

	// Fast path: check modification time first
	info, err := os.Stat(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't stat
	}
	modTime := info.ModTime()
	if !modTime.After(lastMod) {
		return false // File hasn't been modified
	}

	// Slow path: file was modified, compute hash
	data, err := os.ReadFile(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't read
	}

	// Parse nodes from file content
	var nodes []config.NodeConfig
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if config.IsProxyURI(line) {
			nodes = append(nodes, config.NodeConfig{URI: line})
		}
	}

	currentHash := m.computeNodesHash(nodes)
	changed := currentHash != lastHash

	// Update cached mod time
	m.mu.Lock()
	m.lastNodesModTime = modTime
	m.mu.Unlock()

	return changed
}

// MarkNodesModified updates the modification status.
func (m *Manager) MarkNodesModified() {
	m.mu.Lock()
	m.status.NodesModified = true
	m.mu.Unlock()
}

type subscriptionFetchResult struct {
	url   string
	nodes []config.NodeConfig
	info  monitor.SubscriptionInfo
	err   error
}

func parseSubscriptionUserInfo(header string, info *monitor.SubscriptionInfo) {
	for _, field := range strings.Split(header, ";") {
		parts := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(parts) != 2 {
			continue
		}
		value, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(parts[1]), "\""), 10, 64)
		if err != nil || value < 0 {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(parts[0])) {
		case "upload":
			info.UploadBytes = value
		case "download":
			info.DownloadBytes = value
		case "total":
			info.TotalBytes = value
		case "expire":
			info.ExpiresAt = value
		}
	}
	info.UsedBytes = info.UploadBytes + info.DownloadBytes
	if info.TotalBytes > info.UsedBytes {
		info.RemainingBytes = info.TotalBytes - info.UsedBytes
	}
}

func (m *Manager) fetchSubscription(ctx context.Context, rawURL string, timeout time.Duration, forceFetch bool) subscriptionFetchResult {
	if m.fetchCoordinator != nil {
		m.mu.RLock()
		maxAge := m.baseCfg.SubscriptionRefresh.Interval
		m.mu.RUnlock()
		if maxAge <= 0 {
			maxAge = time.Hour
		}
		return m.fetchCoordinator.Fetch(ctx, m.fetchOwner, rawURL, maxAge, forceFetch, func(fetchCtx context.Context) subscriptionFetchResult {
			return m.fetchSubscriptionDirect(fetchCtx, rawURL, timeout)
		})
	}
	return m.fetchSubscriptionDirect(ctx, rawURL, timeout)
}

// applySharedFetch imports a response obtained by another project, updates
// this project's local cache, and reloads its runtime without another network
// request. The project's own status and state store remain independent.
func (m *Manager) applySharedFetch(rawURL string) {
	if m.fetchCoordinator == nil {
		return
	}
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	// Read the cache only after taking the project refresh gate. If two global
	// updates arrive close together, every queued callback then imports the
	// latest response instead of allowing an older response to win last.
	result, ok := m.fetchCoordinator.Get(rawURL)
	if !ok || result.err != nil {
		return
	}
	m.mu.RLock()
	configured := false
	for _, configuredURL := range m.baseCfg.ActiveSubscriptions() {
		if configuredURL == rawURL {
			configured = true
			break
		}
	}
	m.mu.RUnlock()
	if !configured {
		return
	}
	if m.boxMgr == nil {
		return
	}
	m.mu.RLock()
	alreadyApplied := m.sharedFetchAlreadyAppliedLocked(result)
	m.mu.RUnlock()
	if alreadyApplied {
		return
	}

	m.mu.RLock()
	previousCache := cloneSubscriptionNodeCache(m.nodeCache)
	previousItems := cloneSubscriptionItems(m.items)
	m.mu.RUnlock()

	// Match fetchSubscriptions: an expired or exhausted subscription must drop
	// its old nodes, while an active response replaces only this URL's cache.
	m.mu.Lock()
	if result.info.Status == "active" {
		m.nodeCache[rawURL] = append([]config.NodeConfig(nil), result.nodes...)
	} else if result.info.Status == "expired" || result.info.Status == "quota_exhausted" {
		delete(m.nodeCache, rawURL)
	} else {
		m.mu.Unlock()
		return
	}
	m.items[rawURL] = result.info
	nodes := m.cachedNodesForConfigLocked()
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.status.IsRefreshing = true
	m.mu.Unlock()
	m.saveNodeCache(cacheSnapshot)
	if err := m.writeNodesToFile(m.getNodesFilePath(), nodes); err != nil {
		m.logger.Warnf("写入共享订阅缓存失败: %v", err)
		m.restoreSharedFetchState(previousCache, previousItems, err)
		return
	}

	newHash := m.computeNodesHash(nodes)
	portMap := m.boxMgr.CurrentPortMap()
	if err := m.boxMgr.ReloadWithPortMap(m.createNewConfig(nodes), portMap); err != nil {
		m.logger.Warnf("应用共享订阅刷新结果失败: %v", err)
		m.restoreSharedFetchState(previousCache, previousItems, err)
		return
	}
	if info, err := os.Stat(m.getNodesFilePath()); err == nil {
		m.mu.Lock()
		m.lastNodesModTime = info.ModTime()
		m.mu.Unlock()
	}
	m.mu.Lock()
	m.lastSubHash = newHash
	m.status.IsRefreshing = false
	m.status.LastRefresh = time.Now()
	m.status.NodeCount = countEnabledNodes(nodes)
	m.status.LastError = ""
	m.status.NodesModified = false
	m.status.RefreshCount++
	m.mu.Unlock()
	m.persistState(true)
}

func cloneSubscriptionItems(source map[string]monitor.SubscriptionInfo) map[string]monitor.SubscriptionInfo {
	cloned := make(map[string]monitor.SubscriptionInfo, len(source))
	for rawURL, info := range source {
		cloned[rawURL] = info
	}
	return cloned
}

func (m *Manager) sharedFetchAlreadyAppliedLocked(result subscriptionFetchResult) bool {
	current, ok := m.items[result.url]
	if !ok || current.Status != result.info.Status || !current.LastRefresh.Equal(result.info.LastRefresh) {
		return false
	}
	switch result.info.Status {
	case "active":
		return equalSubscriptionNodes(m.nodeCache[result.url], result.nodes)
	case "expired", "quota_exhausted":
		return len(m.nodeCache[result.url]) == 0
	default:
		return false
	}
}

func equalSubscriptionNodes(left, right []config.NodeConfig) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (m *Manager) restoreSharedFetchState(cache map[string][]config.NodeConfig, items map[string]monitor.SubscriptionInfo, cause error) {
	m.mu.Lock()
	m.nodeCache = cloneSubscriptionNodeCache(cache)
	m.items = cloneSubscriptionItems(items)
	nodes := m.cachedNodesForConfigLocked()
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.status.IsRefreshing = false
	m.status.LastRefresh = time.Now()
	m.status.NodeCount = countEnabledNodes(nodes)
	m.status.LastError = cause.Error()
	m.status.RefreshCount++
	m.mu.Unlock()
	m.saveNodeCache(cacheSnapshot)
	if err := m.writeNodesToFile(m.getNodesFilePath(), nodes); err != nil {
		m.logger.Warnf("恢复共享订阅状态失败: %v", err)
	}
	m.persistState(true)
}

func (m *Manager) fetchSubscriptionDirect(ctx context.Context, rawURL string, timeout time.Duration) subscriptionFetchResult {
	info := newSubscriptionInfo(rawURL)
	info.LastRefresh = time.Now()
	result := subscriptionFetchResult{url: rawURL, info: info}

	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		result.err = fmt.Errorf("无效的订阅地址")
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		result.err = fmt.Errorf("创建订阅请求失败: %w", err)
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}
	config.ApplySubscriptionRequestHeaders(req)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		result.err = fmt.Errorf("获取订阅失败: %w", err)
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.err = fmt.Errorf("订阅返回状态码 %d", resp.StatusCode)
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}

	parseSubscriptionUserInfo(resp.Header.Get("Subscription-Userinfo"), &result.info)
	inactiveStatus := ""
	if result.info.ExpiresAt > 0 && result.info.ExpiresAt <= time.Now().Unix() {
		inactiveStatus = "expired"
	}
	if inactiveStatus == "" && result.info.TotalBytes > 0 && result.info.UsedBytes >= result.info.TotalBytes {
		inactiveStatus = "quota_exhausted"
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSubscriptionBodySize+1))
	if err != nil {
		readErr := fmt.Errorf("读取订阅响应失败: %w", err)
		if inactiveStatus != "" {
			result.info.Status = inactiveStatus
			result.info.LastError = readErr.Error()
			return result
		}
		result.err = readErr
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}
	if len(body) > maxSubscriptionBodySize {
		if inactiveStatus != "" {
			result.info.Status = inactiveStatus
			result.info.LastError = fmt.Sprintf("订阅内容超过 %d 字节", maxSubscriptionBodySize)
			return result
		}
		result.err = fmt.Errorf("订阅内容超过 %d 字节", maxSubscriptionBodySize)
		result.info.Status = "error"
		result.info.LastError = result.err.Error()
		return result
	}

	result.nodes, err = config.ParseSubscriptionContent(strings.TrimSpace(string(body)))
	if err != nil || len(result.nodes) == 0 {
		if err == nil {
			err = fmt.Errorf("订阅没有可用节点")
		}
		if inactiveStatus != "" {
			result.info.Status = inactiveStatus
			result.info.LastError = err.Error()
			return result
		}
		result.err = err
		result.info.Status = "error"
		result.info.LastError = err.Error()
		return result
	}
	result.info.NodeCount = len(result.nodes)
	if inactiveStatus != "" {
		result.info.Status = inactiveStatus
		return result
	}
	result.info.Status = "active"
	result.info.Included = true
	result.info.Enabled = true
	for idx := range result.nodes {
		result.nodes[idx].Source = config.NodeSourceSubscription
		result.nodes[idx].SubscriptionURL = rawURL
		result.nodes[idx].Disabled = false
	}
	return result
}

// cachedNodesForConfigLocked returns every cached subscription node. Paused
// subscriptions remain present but are marked disabled so the builder can keep
// their state without routing or probing through them. The caller must hold m.mu.
func (m *Manager) cachedNodesForConfigLocked() []config.NodeConfig {
	allNodes := make([]config.NodeConfig, 0)
	seen := make(map[string]struct{})
	for _, enabledPass := range []bool{true, false} {
		for _, rawURL := range m.baseCfg.Subscriptions {
			enabled := m.baseCfg.SubscriptionEnabled(rawURL)
			if enabled != enabledPass {
				continue
			}
			info := m.items[rawURL]
			info.Enabled = enabled
			info.NodeCount = len(m.nodeCache[rawURL])
			info.Included = enabled && info.NodeCount > 0
			if !enabled {
				info.Status = "disabled"
			}
			m.items[rawURL] = info
			for _, cached := range m.nodeCache[rawURL] {
				node := cached
				node.Source = config.NodeSourceSubscription
				node.SubscriptionURL = rawURL
				node.Disabled = !enabled
				if m.baseCfg.NodeExcluded(node) {
					continue
				}
				key := node.NodeKey()
				if _, duplicate := seen[key]; duplicate {
					continue
				}
				seen[key] = struct{}{}
				allNodes = append(allNodes, node)
			}
		}
	}
	return allNodes
}

func countEnabledNodes(nodes []config.NodeConfig) int {
	count := 0
	for _, node := range nodes {
		if !node.Disabled {
			count++
		}
	}
	return count
}

// fetchAllSubscriptions fetches every configured URL while retaining a separate
// cache and lifecycle state for each subscription.
func (m *Manager) fetchAllSubscriptions(forceFetch bool) ([]config.NodeConfig, error) {
	return m.fetchSubscriptions(nil, forceFetch)
}

// fetchSubscriptions fetches the requested active URLs. A nil request means
// all active subscriptions; non-nil requests are used for incremental config
// updates so existing remote subscriptions are not contacted again.
func (m *Manager) fetchSubscriptions(requestedURLs []string, forceFetch bool) ([]config.NodeConfig, error) {
	m.mu.RLock()
	urls := m.baseCfg.ActiveSubscriptions()
	timeout := m.baseCfg.SubscriptionRefresh.Timeout
	concurrency := m.baseCfg.SubscriptionRefresh.FetchConcurrency
	ctx := m.ctx
	m.mu.RUnlock()
	if requestedURLs != nil {
		active := make(map[string]struct{}, len(urls))
		for _, rawURL := range urls {
			active[rawURL] = struct{}{}
		}
		selected := make([]string, 0, len(requestedURLs))
		seen := make(map[string]struct{}, len(requestedURLs))
		for _, rawURL := range requestedURLs {
			if _, ok := active[rawURL]; !ok {
				continue
			}
			if _, ok := seen[rawURL]; ok {
				continue
			}
			seen[rawURL] = struct{}{}
			selected = append(selected, rawURL)
		}
		urls = selected
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if concurrency <= 0 {
		concurrency = 8
	}
	if concurrency > len(urls) && len(urls) > 0 {
		concurrency = len(urls)
	}
	if len(urls) == 0 {
		m.mu.Lock()
		nodes := m.cachedNodesForConfigLocked()
		m.mu.Unlock()
		return nodes, nil
	}

	results := make([]subscriptionFetchResult, len(urls))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, rawURL := range urls {
		wg.Add(1)
		go func(index int, subscriptionURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[index] = m.fetchSubscription(ctx, subscriptionURL, timeout, forceFetch)
		}(i, rawURL)
	}
	wg.Wait()

	failed := 0
	var lastErr error
	m.mu.Lock()
	for _, result := range results {
		if result.err != nil {
			failed++
			lastErr = result.err
			if cached := m.nodeCache[result.url]; len(cached) > 0 {
				result.nodes = append([]config.NodeConfig(nil), cached...)
				result.info.NodeCount = len(cached)
				result.info.Included = true
				m.logger.Warnf("订阅 %s 刷新失败；保留 %d 个缓存节点: %v", config.RedactURL(result.url), len(cached), result.err)
			}
		} else if result.info.Status == "active" {
			m.nodeCache[result.url] = append([]config.NodeConfig(nil), result.nodes...)
		} else {
			// Expired and quota-exhausted subscriptions must not retain stale nodes.
			delete(m.nodeCache, result.url)
			result.nodes = nil
			result.info.Included = false
		}
		m.items[result.url] = result.info
	}
	allNodes := m.cachedNodesForConfigLocked()
	cacheSnapshot := cloneSubscriptionNodeCache(m.nodeCache)
	m.mu.Unlock()
	m.saveNodeCache(cacheSnapshot)

	if requestedURLs != nil && failed > 0 && lastErr != nil {
		return allNodes, lastErr
	}
	if failed == len(results) && countEnabledNodes(allNodes) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return allNodes, nil
}

// createNewConfig creates a new config with updated nodes while preserving other settings.
func (m *Manager) createNewConfig(nodes []config.NodeConfig) *config.Config {
	// Start from the current running config so settings changed after the
	// subscription manager was created (for example sticky.fixed_node) survive a
	// later subscription refresh.
	var currentCfg *config.Config
	if m.boxMgr != nil {
		currentCfg = m.boxMgr.CurrentConfigSnapshot()
	}
	if currentCfg == nil {
		fallback := *m.baseCfg
		currentCfg = &fallback
	}
	newCfg := *currentCfg
	newCfg.Subscriptions = append([]string(nil), m.baseCfg.Subscriptions...)
	newCfg.DisabledSubscriptions = append([]string(nil), m.baseCfg.DisabledSubscriptions...)
	newCfg.SubscriptionRefresh = m.baseCfg.SubscriptionRefresh

	// Mark all subscription nodes with proper source
	for i := range nodes {
		nodes[i].Source = config.NodeSourceSubscription
	}

	// Preserve inline nodes from base config (nodes defined directly in config.yaml)
	var inlineNodes []config.NodeConfig
	for _, node := range currentCfg.Nodes {
		if node.Source == config.NodeSourceInline {
			inlineNodes = append(inlineNodes, node)
		}
	}

	// Merge inline nodes with subscription nodes: inline nodes first, then subscription nodes
	mergedNodes := make([]config.NodeConfig, 0, len(inlineNodes)+len(nodes))
	mergedNodes = append(mergedNodes, inlineNodes...)
	for _, node := range nodes {
		if node.Disabled {
			continue
		}
		mergedNodes = append(mergedNodes, node)
	}

	// Port and credential assignment is owned by NormalizeWithPortMap (invoked
	// via ReloadWithPortMap): it preserves the port of any node whose stable
	// identity is unchanged and assigns fresh, collision-free ports to the rest.
	// Pre-assigning sequential ports here would override that preservation and
	// could collide with a preserved port, so it is intentionally left to the
	// normalize step.

	// Process node names
	for i := range mergedNodes {
		mergedNodes[i].Name = strings.TrimSpace(mergedNodes[i].Name)
		mergedNodes[i].URI = strings.TrimSpace(mergedNodes[i].URI)

		// Auto-extract name from URI if not provided
		if mergedNodes[i].Name == "" {
			mergedNodes[i].Name = config.ExtractNodeName(mergedNodes[i].URI)
		}
		if mergedNodes[i].Name == "" {
			mergedNodes[i].Name = fmt.Sprintf("node-%d", i)
		}
	}

	newCfg.Nodes = mergedNodes
	return &newCfg
}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[subscription] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[订阅] 警告: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[订阅] 错误: "+format, args...)
}
