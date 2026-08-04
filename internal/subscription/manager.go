package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// Manager handles periodic subscription refresh.
type Manager struct {
	mu sync.RWMutex

	baseCfg    *config.Config
	boxMgr     *boxmgr.Manager
	logger     Logger
	httpClient *http.Client // Custom HTTP client with connection pooling

	status        monitor.SubscriptionStatus
	ctx           context.Context
	cancel        context.CancelFunc
	refreshMu     sync.Mutex // prevents concurrent refreshes
	manualRefresh chan struct{}
	items         map[string]monitor.SubscriptionInfo
	nodeCache     map[string][]config.NodeConfig

	// Track nodes.txt content hash to detect modifications
	lastSubHash      string    // Hash of nodes.txt content after last subscription refresh
	lastNodesModTime time.Time // Last known modification time of nodes.txt
}

const maxSubscriptionBodySize = 10 * 1024 * 1024

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
	return m
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
		ID:     subscriptionID(rawURL),
		URL:    rawURL,
		Name:   subscriptionName(rawURL),
		Status: "pending",
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
	}
	for rawURL := range m.items {
		if _, ok := active[rawURL]; !ok {
			delete(m.items, rawURL)
			delete(m.nodeCache, rawURL)
		}
	}
}

// Start begins the periodic refresh loop.
func (m *Manager) Start() {
	if len(m.baseCfg.Subscriptions) == 0 {
		m.logger.Infof("no subscriptions configured, refresh disabled")
		return
	}

	interval := m.baseCfg.SubscriptionRefresh.Interval
	m.logger.Infof("starting subscription manager, auto-refresh=%v, interval=%s", m.baseCfg.SubscriptionRefresh.Enabled, interval)

	go m.refreshLoop(interval)
	if m.baseCfg.SubscriptionRefresh.Enabled {
		select {
		case m.manualRefresh <- struct{}{}:
		default:
		}
	}
}

// Stop stops the periodic refresh.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}

	// Close idle connections
	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
}

// UpdateConfig hot-reloads subscription URLs and refresh settings without restart.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	m.updateConfig(urls, enabled, interval, true)
}

func (m *Manager) updateConfig(urls []string, enabled bool, interval time.Duration, triggerRefresh bool) {
	urls = append([]string(nil), urls...)
	m.mu.Lock()
	m.baseCfg.Subscriptions = urls
	m.baseCfg.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		m.baseCfg.SubscriptionRefresh.Interval = interval
	}
	m.reconcileSubscriptionStateLocked(urls)
	m.mu.Unlock()

	// Persist to config.yaml
	if err := m.baseCfg.SaveSettings(); err != nil {
		m.logger.Errorf("failed to save subscription config: %v", err)
	}

	// Restart the refresh loop with new settings
	if m.cancel != nil {
		m.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.ctx = ctx
	m.cancel = cancel
	m.manualRefresh = make(chan struct{}, 1)
	m.mu.Unlock()

	// Always start the refresh loop to handle the immediate refresh signal. An
	// empty URL list is also refreshed so stale subscription nodes are removed.
	m.logger.Infof("subscription config updated: %d URLs, enabled=%v, interval=%s", len(urls), enabled, m.baseCfg.SubscriptionRefresh.Interval)
	go m.refreshLoop(m.baseCfg.SubscriptionRefresh.Interval)

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

// UpdateConfigAndRefresh updates subscription config and synchronously waits for
// the first refresh to complete before returning. This ensures the caller (WebUI API)
// can confirm the update took effect.
func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	m.updateConfig(urls, enabled, interval, false)
	m.doRefresh()
	if status := m.Status(); status.LastError != "" {
		return fmt.Errorf("刷新失败: %s", status.LastError)
	}
	return nil
}

// RefreshNow triggers an immediate refresh.
func (m *Manager) RefreshNow() error {
	m.doRefresh()
	if status := m.Status(); status.LastError != "" {
		return fmt.Errorf("refresh failed: %s", status.LastError)
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

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if autoEnabled {
		// Update next refresh time only when auto-refresh is enabled
		m.mu.Lock()
		m.status.NextRefresh = time.Now().Add(interval)
		m.mu.Unlock()
	}

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			// Only do periodic refresh when auto-refresh is enabled
			if !autoEnabled {
				continue
			}
			m.doRefresh()
			m.mu.Lock()
			m.status.NextRefresh = time.Now().Add(interval)
			m.mu.Unlock()
		case <-manualRefresh:
			// Always honor manual/immediate refresh regardless of enabled flag
			m.doRefresh()
			if autoEnabled {
				ticker.Reset(interval)
				m.mu.Lock()
				m.status.NextRefresh = time.Now().Add(interval)
				m.mu.Unlock()
			}
		}
	}
}

// doRefresh performs a single refresh operation.
func (m *Manager) doRefresh() {
	// Serialize refreshes so a configuration update that arrives during an
	// automatic refresh still gets its own completed pass.
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	m.mu.Lock()
	m.status.IsRefreshing = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.status.IsRefreshing = false
		m.status.RefreshCount++
		m.mu.Unlock()
	}()

	m.logger.Infof("starting subscription refresh")

	// Fetch nodes from all subscriptions
	nodes, err := m.fetchAllSubscriptions()
	if err != nil {
		m.logger.Errorf("fetch subscriptions failed: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	m.logger.Infof("fetched %d nodes from subscriptions", len(nodes))

	// Write subscription nodes to nodes.txt
	nodesFilePath := m.getNodesFilePath()
	if err := m.writeNodesToFile(nodesFilePath, nodes); err != nil {
		m.logger.Errorf("failed to write nodes.txt: %v", err)
		m.mu.Lock()
		m.status.LastError = fmt.Sprintf("write nodes.txt: %v", err)
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

	// Trigger BoxManager reload with port preservation
	if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
		m.logger.Errorf("reload failed: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	m.status.LastRefresh = time.Now()
	m.status.NodeCount = len(nodes)
	m.status.LastError = ""
	m.mu.Unlock()

	m.logger.Infof("subscription refresh completed, %d nodes active", len(nodes))
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

func (m *Manager) fetchSubscription(ctx context.Context, rawURL string, timeout time.Duration) subscriptionFetchResult {
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
		result.err = err
		result.info.Status = "error"
		result.info.LastError = err.Error()
		return result
	}
	config.ApplySubscriptionRequestHeaders(req)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		result.err = err
		result.info.Status = "error"
		result.info.LastError = err.Error()
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
	return result
}

// fetchAllSubscriptions fetches every configured URL while retaining a separate
// cache and lifecycle state for each subscription.
func (m *Manager) fetchAllSubscriptions() ([]config.NodeConfig, error) {
	m.mu.RLock()
	urls := append([]string(nil), m.baseCfg.Subscriptions...)
	timeout := m.baseCfg.SubscriptionRefresh.Timeout
	concurrency := m.baseCfg.SubscriptionRefresh.FetchConcurrency
	ctx := m.ctx
	m.mu.RUnlock()
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
		return nil, nil
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
			results[index] = m.fetchSubscription(ctx, subscriptionURL, timeout)
		}(i, rawURL)
	}
	wg.Wait()

	allNodes := make([]config.NodeConfig, 0)
	seenNodes := make(map[string]struct{})
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
				m.logger.Warnf("subscription refresh failed for %s; retaining %d cached nodes: %v", config.RedactURL(result.url), len(cached), result.err)
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
		for _, node := range result.nodes {
			key := node.NodeKey()
			if _, exists := seenNodes[key]; exists {
				continue
			}
			seenNodes[key] = struct{}{}
			allNodes = append(allNodes, node)
		}
	}
	m.mu.Unlock()

	if failed == len(results) && len(allNodes) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return allNodes, nil
}

// createNewConfig creates a new config with updated nodes while preserving other settings.
func (m *Manager) createNewConfig(nodes []config.NodeConfig) *config.Config {
	// Deep copy base config
	newCfg := *m.baseCfg

	// Mark all subscription nodes with proper source
	for i := range nodes {
		nodes[i].Source = config.NodeSourceSubscription
	}

	// Preserve inline nodes from base config (nodes defined directly in config.yaml)
	var inlineNodes []config.NodeConfig
	for _, node := range m.baseCfg.Nodes {
		if node.Source == config.NodeSourceInline {
			inlineNodes = append(inlineNodes, node)
		}
	}

	// Merge inline nodes with subscription nodes: inline nodes first, then subscription nodes
	mergedNodes := make([]config.NodeConfig, 0, len(inlineNodes)+len(nodes))
	mergedNodes = append(mergedNodes, inlineNodes...)
	mergedNodes = append(mergedNodes, nodes...)

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
	log.Printf("[subscription] WARN: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[subscription] ERROR: "+format, args...)
}
