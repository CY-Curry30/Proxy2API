package config

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"Proxy2API/internal/state"

	"gopkg.in/yaml.v3"
)

const (
	SubscriptionUserAgent     = "clash-verge/v2.2.3"
	SubscriptionCacheFileName = ".subscription-cache.json"
)

// ApplySubscriptionRequestHeaders keeps every subscription fetch path on the
// same modern Clash-compatible response format. Some providers downgrade old
// ClashForWindows clients to a legacy subset that omits VLESS/Hysteria2.
func ApplySubscriptionRequestHeaders(req *http.Request) {
	req.Header.Set("User-Agent", SubscriptionUserAgent)
	req.Header.Set("Accept", "*/*")
}

// Config describes the high level settings for the proxy pool server.
type Config struct {
	Mode                  string                    `yaml:"mode"`
	Listener              ListenerConfig            `yaml:"listener"`
	MultiPort             MultiPortConfig           `yaml:"multi_port"`
	Pool                  PoolConfig                `yaml:"pool"`
	Sticky                StickyConfig              `yaml:"sticky"`
	Management            ManagementConfig          `yaml:"management"`
	Probe                 ProbeConfig               `yaml:"probe,omitempty"`
	SubscriptionRefresh   SubscriptionRefreshConfig `yaml:"subscription_refresh"`
	Log                   LogConfig                 `yaml:"log"`
	Nodes                 []NodeConfig              `yaml:"nodes"`
	NodesFile             string                    `yaml:"nodes_file"`                       // 节点文件路径，每行一个 URI
	Subscriptions         []string                  `yaml:"subscriptions"`                    // 订阅链接列表
	DisabledSubscriptions []string                  `yaml:"disabled_subscriptions,omitempty"` // 已暂停但保留缓存的订阅
	SelectedSubscriptions []string                  `yaml:"selected_subscriptions,omitempty"` // 项目选定的订阅子集，空表示全部选中
	ExcludedSubscriptions []string                  `yaml:"excluded_subscriptions,omitempty"` // 项目排除的共享订阅
	ExcludedNodes         []string                  `yaml:"excluded_nodes,omitempty"`         // 项目排除的共享节点稳定 ID
	ExternalIP            string                    `yaml:"external_ip"`                      // 外部 IP 地址，用于导出时替换 0.0.0.0
	LogLevel              string                    `yaml:"log_level"`
	SkipCertVerify        bool                      `yaml:"skip_cert_verify"` // 全局跳过 SSL 证书验证
	ClashAPIPort          uint16                    `yaml:"-" json:"-"`       // Runtime-only, assigned by the project registry.

	filePath              string `yaml:"-"` // 配置文件路径，用于保存
	recoveredStateCatalog bool   `yaml:"-"`
	recoveredCatalogURLs  map[string]struct{}
	sourcesShared         bool `yaml:"-"`
	sourcesOnly           bool `yaml:"-"`
	skipRuntimeRecovery   bool `yaml:"-"`
}

// LogConfig controls log output and rotation.
type LogConfig struct {
	Output     string `yaml:"output" json:"output"`           // 日志输出: "stdout", "file", 默认 "stdout"
	File       string `yaml:"file" json:"file"`               // 日志文件路径，默认 "logs/Proxy2API.log"
	MaxSize    int    `yaml:"max_size" json:"max_size"`       // 单个日志文件最大 MB，默认 50
	MaxBackups int    `yaml:"max_backups" json:"max_backups"` // 保留旧日志文件个数，默认 3
	MaxAge     int    `yaml:"max_age" json:"max_age"`         // 保留旧日志文件天数，默认 7
	Compress   bool   `yaml:"compress" json:"compress"`       // 是否压缩旧日志，默认 false
}

// ListenerConfig defines how the HTTP/SOCKS5 mixed proxy should listen for clients.
type ListenerConfig struct {
	Address  string `yaml:"address"`
	Port     uint16 `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// StickyConfig configures an optional dedicated sticky-session entry port.
// When enabled (pool/hybrid mode only), clients connecting to this port are
// pinned to a single upstream node by source IP, keeping the egress IP stable.
// The sticky port reuses the listener's address and credentials.
type StickyConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Port      uint16 `yaml:"port"`
	FixedNode string `yaml:"fixed_node,omitempty"` // 粘性入口指定节点；空值默认选择最低延迟节点
}

// PoolConfig configures scheduling + failure handling.
type PoolConfig struct {
	Mode              string        `yaml:"mode"`
	FailureThreshold  int           `yaml:"failure_threshold"`
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
	FixedNode         string        `yaml:"fixed_node,omitempty"` // Deprecated: migrated to sticky.fixed_node
	// RetryEnabled toggles automatic fail-over to another member when a dial fails.
	// nil/unset → default true. Use *bool so users can explicitly disable via YAML.
	RetryEnabled *bool `yaml:"retry_enabled,omitempty"`
	// RetryAttempts is the maximum total dial attempts per request (including the first).
	// Default 3. Values <= 0 are normalized to 3.
	// For pools with multiple members, each retry picks a different member when possible.
	// For single-member pools (e.g. per-node multi-port pools), retries dial the same member.
	RetryAttempts int `yaml:"retry_attempts,omitempty"`
}

// RetryEnabledOrDefault reports whether retry is enabled (default true).
func (p PoolConfig) RetryEnabledOrDefault() bool {
	if p.RetryEnabled == nil {
		return true
	}
	return *p.RetryEnabled
}

// MultiPortConfig defines address/credential defaults for multi-port mode.
type MultiPortConfig struct {
	Address  string `yaml:"address"`
	BasePort uint16 `yaml:"base_port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// ManagementConfig controls the monitoring HTTP endpoint.
type ManagementConfig struct {
	Enabled          *bool         `yaml:"enabled"`
	Listen           string        `yaml:"listen"`
	ProbeTarget      string        `yaml:"probe_target,omitempty"`      // Deprecated: use project-level probe.target.
	ProbeInterval    time.Duration `yaml:"probe_interval,omitempty"`    // Deprecated: use project-level probe.interval.
	ProbeTimeout     time.Duration `yaml:"probe_timeout,omitempty"`     // Deprecated: use project-level probe.timeout.
	Password         string        `yaml:"password"`                    // WebUI 访问密码，为空则不需要密码
	ProbeConcurrency int           `yaml:"probe_concurrency,omitempty"` // Deprecated: use project-level probe.concurrency.
}

// ProbeConfig controls health checks for one project runtime.
type ProbeConfig struct {
	Target      string        `yaml:"target" json:"target"`
	Interval    time.Duration `yaml:"interval" json:"interval"`
	Timeout     time.Duration `yaml:"timeout" json:"timeout"`
	Concurrency int           `yaml:"concurrency" json:"concurrency"`
}

// SubscriptionRefreshConfig controls subscription auto-refresh and reload settings.
type SubscriptionRefreshConfig struct {
	Enabled            bool          `yaml:"enabled"`              // 是否启用定时刷新
	Interval           time.Duration `yaml:"interval"`             // 刷新间隔，默认 1 小时
	Timeout            time.Duration `yaml:"timeout"`              // 获取订阅的超时时间
	HealthCheckTimeout time.Duration `yaml:"health_check_timeout"` // 新节点健康检查超时
	DrainTimeout       time.Duration `yaml:"drain_timeout"`        // 旧实例排空超时时间
	MinAvailableNodes  int           `yaml:"min_available_nodes"`  // 最少可用节点数，低于此值不切换
	FetchConcurrency   int           `yaml:"fetch_concurrency"`    // 订阅抓取并发数，默认 16，最大 32
}

// NodeSource indicates where a node configuration originated from.
type NodeSource string

const (
	NodeSourceInline       NodeSource = "inline"       // Defined directly in config.yaml nodes array
	NodeSourceFile         NodeSource = "nodes_file"   // Loaded from external nodes file
	NodeSourceSubscription NodeSource = "subscription" // Fetched from subscription URL
)

const (
	defaultSubscriptionFetchConcurrency = 16
	maxSubscriptionFetchConcurrency     = 32
	maxSubscriptionBodySize             = 10 * 1024 * 1024
)

var subscriptionInfoNameKeywords = []string{
	"剩余流量", "流量剩余", "已用流量", "流量已用", "总流量", "流量总量",
	"流量重置", "重置流量", "下次重置", "重置倒计时", "套餐剩余",
	"到期", "过期", "有效期",
	"官网", "官方网站", "官方网址", "备用网址", "订阅地址", "更新订阅",
	"续费订阅", "购买套餐", "联系客服", "客服中心", "用户中心", "使用教程",
	"节点公告", "建议", "官方群", "交流群", "tg频道", "telegram频道",
	"remainingtraffic", "trafficremaining", "usedtraffic", "trafficused",
	"totaltraffic", "traffictotal", "trafficreset", "resettraffic", "nextreset",
	"remainingbandwidth", "bandwidthremaining", "remainingdata", "dataremaining",
	"expire", "expiry", "expiration", "validuntil",
	"officialwebsite", "officialsite", "subscriptionurl", "updatesubscription",
	"renewsubscription", "customerservice", "contactsupport",
}

// SubscriptionFetchStats describes a subscription loading attempt.
type SubscriptionFetchStats struct {
	RequestedURLs int
	UniqueURLs    int
	Successful    int
	Failed        int
	Empty         int
	Nodes         int
	DedupedURLs   int
	DedupedNodes  int
	LastError     error
}

// SubscriptionFetchOptions controls concurrent subscription loading.
type SubscriptionFetchOptions struct {
	Timeout     time.Duration
	Concurrency int
	Client      *http.Client
	Loggerf     func(format string, args ...any)
}

// NodeConfig describes a single upstream proxy endpoint expressed as URI.
type NodeConfig struct {
	Name            string     `yaml:"name" json:"name"`
	URI             string     `yaml:"uri" json:"uri"`
	Port            uint16     `yaml:"port,omitempty" json:"port,omitempty"`
	Username        string     `yaml:"username,omitempty" json:"username,omitempty"`
	Password        string     `yaml:"password,omitempty" json:"password,omitempty"`
	Source          NodeSource `yaml:"-" json:"source,omitempty"` // Runtime only, not persisted
	SubscriptionURL string     `yaml:"-" json:"-"`                // Runtime owner used for per-subscription enable/disable
	Disabled        bool       `yaml:"-" json:"disabled,omitempty"`
	StateKey        string     `yaml:"-" json:"-"`
}

// NodeKey returns a stable identifier for the node, used to preserve port
// assignments across subscription refreshes and reloads.
//
// The identity deliberately ignores the parts of a proxy URI that subscription
// providers commonly mutate without changing the underlying server: the
// display name (#fragment) and query-parameter ordering. As long as the
// scheme, credentials, host, port and parameter set are unchanged, a node
// keeps the same key — and therefore the same proxy port.
func (n *NodeConfig) NodeKey() string {
	return stableNodeKey(n.URI)
}

// StateID is the non-reversible persistent identifier used by the runtime
// state database. It remains stable across display-name and query-order changes.
func (n *NodeConfig) StateID() string {
	return n.StateIDForOccurrence(0)
}

// StateIDForOccurrence distinguishes duplicate node definitions while keeping
// the first occurrence compatible with the ordinary stable node ID.
func (n *NodeConfig) StateIDForOccurrence(occurrence int) string {
	if n.StateKey != "" {
		return n.StateKey
	}
	key := n.NodeKey()
	if occurrence > 0 {
		key = fmt.Sprintf("%s\x00%d", key, occurrence)
	}
	return state.NodeID(key)
}

// stableNodeKey derives a port-stable identity from a proxy URI by stripping the
// volatile display name and canonicalizing query order. It never errors: on any
// parse failure it falls back to the raw URI minus its fragment, so the result
// is always at least as stable as the previous full-URI behavior.
func stableNodeKey(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return ""
	}

	// vmess:// is base64-encoded JSON rather than a standard URL; only strip an
	// appended fragment and keep the payload as the identity.
	if strings.HasPrefix(uri, "vmess://") {
		if idx := strings.Index(uri, "#"); idx != -1 {
			return strings.TrimSpace(uri[:idx])
		}
		return uri
	}

	u, err := url.Parse(uri)
	if err != nil {
		if idx := strings.LastIndex(uri, "#"); idx != -1 {
			return strings.TrimSpace(uri[:idx])
		}
		return uri
	}

	u.Fragment = "" // display name — volatile, not part of node identity
	if u.RawQuery != "" {
		// Encode() sorts keys, so reordered parameters yield the same key.
		u.RawQuery = u.Query().Encode()
	}
	return u.String()
}

// SubscriptionEnabled reports whether a configured subscription participates
// in refreshes and the active node pool.
func (c *Config) SubscriptionEnabled(rawURL string) bool {
	for _, disabledURL := range c.DisabledSubscriptions {
		if disabledURL == rawURL {
			return false
		}
	}
	return true
}

// ActiveSubscriptions returns configured subscriptions that are not paused.
func (c *Config) ActiveSubscriptions() []string {
	active := make([]string, 0, len(c.Subscriptions))
	for _, rawURL := range c.Subscriptions {
		if c.SubscriptionEnabled(rawURL) {
			active = append(active, rawURL)
		}
	}
	return active
}

// EffectiveSubscriptions returns the project's selected subscription URLs. When
// SelectedSubscriptions is empty, all configured subscriptions are included
// (backward compatible). Otherwise, only the selected subset is returned.
func (c *Config) EffectiveSubscriptions() []string {
	selected := make(map[string]struct{}, len(c.SelectedSubscriptions))
	for _, rawURL := range c.SelectedSubscriptions {
		selected[strings.TrimSpace(rawURL)] = struct{}{}
	}
	excluded := make(map[string]struct{}, len(c.ExcludedSubscriptions))
	for _, rawURL := range c.ExcludedSubscriptions {
		excluded[strings.TrimSpace(rawURL)] = struct{}{}
	}
	effective := make([]string, 0, len(c.Subscriptions))
	for _, rawURL := range c.Subscriptions {
		if len(c.SelectedSubscriptions) > 0 {
			if _, ok := selected[rawURL]; !ok {
				continue
			}
		}
		if _, excluded := excluded[rawURL]; excluded {
			continue
		}
		effective = append(effective, rawURL)
	}
	return effective
}

// EffectiveSubscriptionEnabled reports whether a subscription is both globally
// enabled and, when a per-project selection exists, actively selected.
func (c *Config) EffectiveSubscriptionEnabled(rawURL string) bool {
	if !c.SubscriptionEnabled(rawURL) {
		return false
	}
	if len(c.SelectedSubscriptions) > 0 {
		for _, selectedURL := range c.SelectedSubscriptions {
			if selectedURL != rawURL {
				continue
			}
			for _, excludedURL := range c.ExcludedSubscriptions {
				if excludedURL == rawURL {
					return false
				}
			}
			return true
		}
		return false
	}
	for _, excludedURL := range c.ExcludedSubscriptions {
		if excludedURL == rawURL {
			return false
		}
	}
	return true
}

// NodeExcluded reports whether a shared node is hidden from this project.
func (c *Config) NodeExcluded(node NodeConfig) bool {
	if node.NodeKey() == "" {
		return false
	}
	id := node.StateID()
	for _, excluded := range c.ExcludedNodes {
		if excluded == id {
			return true
		}
	}
	return false
}

// SetSubscriptionEnabled updates the persisted pause list without removing the
// subscription URL or its cached nodes.
func (c *Config) SetSubscriptionEnabled(rawURL string, enabled bool) {
	disabled := make([]string, 0, len(c.DisabledSubscriptions)+1)
	found := false
	for _, existing := range c.DisabledSubscriptions {
		if existing == rawURL {
			found = true
			if enabled {
				continue
			}
		}
		disabled = append(disabled, existing)
	}
	if !enabled && !found {
		disabled = append(disabled, rawURL)
	}
	c.DisabledSubscriptions = disabled
}

func (c *Config) normalizeDisabledSubscriptions() {
	configured := make(map[string]struct{}, len(c.Subscriptions))
	for _, rawURL := range c.Subscriptions {
		configured[rawURL] = struct{}{}
	}
	seen := make(map[string]struct{}, len(c.DisabledSubscriptions))
	filtered := make([]string, 0, len(c.DisabledSubscriptions))
	for _, rawURL := range c.DisabledSubscriptions {
		if _, ok := configured[rawURL]; !ok {
			continue
		}
		if _, duplicate := seen[rawURL]; duplicate {
			continue
		}
		seen[rawURL] = struct{}{}
		filtered = append(filtered, rawURL)
	}
	c.DisabledSubscriptions = filtered
}

// PruneDisabledSubscriptions removes pause entries for subscriptions that no
// longer exist. It is used by live subscription CRUD updates.
func (c *Config) PruneDisabledSubscriptions() {
	c.normalizeDisabledSubscriptions()
}

// Load reads YAML config from disk and applies defaults/validation.
func Load(path string) (*Config, error) {
	return load(path, true)
}

// LoadReadOnly reads and validates a config without updating its persisted
// node-port sidecar. It is intended for control-plane summaries of stopped
// projects, where a read request must not mutate project data.
func LoadReadOnly(path string) (*Config, error) {
	return load(path, false)
}

// LoadSettingsReadOnly reads and normalizes project YAML settings without
// restoring persisted ports or opening the runtime recovery database. It is
// safe to use while a running project owns the state database lock.
func LoadSettingsReadOnly(path string) (*Config, error) {
	cfg, err := decodeConfig(path)
	if err != nil {
		return nil, err
	}
	cfg.skipRuntimeRecovery = true
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadShared reads the standalone shared node/subscription catalog. Runtime
// settings in this file are intentionally not used by project runtimes.
func LoadShared(path string) (*Config, error) {
	cfg, err := decodeConfig(path)
	if err != nil {
		return nil, err
	}
	cfg.sourcesOnly = true
	cfg.skipRuntimeRecovery = true
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	for idx := range cfg.Nodes {
		cfg.Nodes[idx].Port = 0
		cfg.Nodes[idx].Username = ""
		cfg.Nodes[idx].Password = ""
	}
	return cfg, nil
}

// LoadProjectWithShared reads project-owned runtime settings and overlays the
// supplied shared source catalog.
func LoadProjectWithShared(path string, shared *Config) (*Config, error) {
	return loadProjectWithShared(path, shared, true)
}

// LoadProjectReadOnlyWithShared is the read-only counterpart of
// LoadProjectWithShared.
func LoadProjectReadOnlyWithShared(path string, shared *Config) (*Config, error) {
	return loadProjectWithShared(path, shared, false)
}

func load(path string, restorePersistedPorts bool) (*Config, error) {
	cfg, err := decodeConfig(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	if restorePersistedPorts {
		// Restore persisted proxy ports so a restart keeps the same port per node.
		if err := cfg.applyPersistedPorts(); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func decodeConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置失败: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	// Keep the historical bool representation while allowing omitted project
	// settings to use the product default of skipping certificate validation.
	var presence struct {
		SkipCertVerify *bool `yaml:"skip_cert_verify"`
	}
	if err := yaml.Unmarshal(data, &presence); err != nil {
		return nil, fmt.Errorf("解析配置默认值失败: %w", err)
	}
	if presence.SkipCertVerify == nil {
		cfg.SkipCertVerify = true
	}
	cfg.filePath = path

	// Resolve nodes_file path relative to config file directory
	if cfg.NodesFile != "" && !filepath.IsAbs(cfg.NodesFile) {
		configDir := filepath.Dir(path)
		cfg.NodesFile = filepath.Join(configDir, cfg.NodesFile)
	}
	return cfg, nil
}

// LoadProject reads project-owned runtime settings while sourcing node and
// subscription definitions from the shared/default config. Subscription
// caches and runtime state remain rooted beside the project config.
func LoadProject(path, sharedPath string) (*Config, error) {
	return loadProject(path, sharedPath, true)
}

// LoadProjectReadOnly builds the same effective project configuration without
// updating the project's node-port sidecar.
func LoadProjectReadOnly(path, sharedPath string) (*Config, error) {
	return loadProject(path, sharedPath, false)
}

func loadProject(path, sharedPath string, persistPorts bool) (*Config, error) {
	projectAbs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("解析项目配置路径失败: %w", err)
	}
	sharedAbs, err := filepath.Abs(sharedPath)
	if err != nil {
		return nil, fmt.Errorf("解析共享配置路径失败: %w", err)
	}
	if filepath.Clean(projectAbs) == filepath.Clean(sharedAbs) {
		if persistPorts {
			return Load(projectAbs)
		}
		return LoadReadOnly(projectAbs)
	}

	shared, err := LoadShared(sharedAbs)
	if err != nil {
		return nil, fmt.Errorf("加载共享节点源失败: %w", err)
	}
	return loadProjectWithShared(path, shared, persistPorts)
}

func loadProjectWithShared(path string, shared *Config, persistPorts bool) (*Config, error) {
	if shared == nil {
		return nil, errors.New("共享源配置不能为空")
	}
	projectAbs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("解析项目配置路径失败: %w", err)
	}
	sharedAbs, err := filepath.Abs(shared.FilePath())
	if err != nil {
		return nil, fmt.Errorf("解析共享配置路径失败: %w", err)
	}
	if filepath.Clean(projectAbs) == filepath.Clean(sharedAbs) {
		if persistPorts {
			return Load(projectAbs)
		}
		return LoadReadOnly(projectAbs)
	}
	project, err := decodeConfig(projectAbs)
	if err != nil {
		return nil, err
	}
	project.sourcesShared = true
	project.Subscriptions = append([]string(nil), shared.Subscriptions...)
	project.DisabledSubscriptions = append([]string(nil), shared.DisabledSubscriptions...)
	// Apply per-project subscription selection and exclusion filters. When both
	// lists are empty, the project uses all shared subscriptions for backward
	// compatibility.
	project.Subscriptions = project.EffectiveSubscriptions()
	project.Nodes = make([]NodeConfig, 0, len(shared.Nodes))
	for _, node := range shared.Nodes {
		if node.Source == NodeSourceSubscription {
			continue
		}
		node.Port = 0
		node.Username = ""
		node.Password = ""
		project.Nodes = append(project.Nodes, node)
	}
	// Without subscriptions, nodes_file belongs to the shared source catalog,
	// not to the project runtime.
	if len(project.Subscriptions) == 0 {
		project.NodesFile = ""
	}
	if err := project.normalize(); err != nil {
		return nil, err
	}

	// A new project has no local subscription cache yet. Seed missing URLs from
	// the shared cache so it can start with the current catalog without sharing
	// status, refresh timestamps, or subsequent cache writes. Only seed
	// subscriptions that are in the project's effective subscription list.
	projectSubscriptionURLs := make(map[string]struct{}, len(project.Subscriptions))
	for _, node := range project.Nodes {
		if node.Source == NodeSourceSubscription && node.SubscriptionURL != "" {
			projectSubscriptionURLs[node.SubscriptionURL] = struct{}{}
		}
	}
	effectiveURLs := make(map[string]struct{}, len(project.Subscriptions))
	for _, rawURL := range project.Subscriptions {
		effectiveURLs[rawURL] = struct{}{}
	}
	for _, node := range shared.Nodes {
		if node.Source != NodeSourceSubscription || node.SubscriptionURL == "" {
			continue
		}
		if _, present := projectSubscriptionURLs[node.SubscriptionURL]; present {
			continue
		}
		// Skip nodes from subscriptions not selected by this project
		if _, effective := effectiveURLs[node.SubscriptionURL]; !effective {
			continue
		}
		node.Port = 0
		node.Username = ""
		node.Password = ""
		project.Nodes = append(project.Nodes, node)
	}
	project.Nodes = dedupeNodesPreferSubscription(project.Nodes)
	if len(project.ExcludedNodes) > 0 {
		filtered := project.Nodes[:0]
		for _, node := range project.Nodes {
			if !project.NodeExcluded(node) {
				filtered = append(filtered, node)
			}
		}
		project.Nodes = filtered
	}
	if persistPorts {
		if err := project.applyPersistedPorts(); err != nil {
			return nil, err
		}
	}
	return project, nil
}

// ExtractNodeName extracts a human-readable name from a proxy URI.
// For standard URIs (vless://, ss://, trojan://), it extracts from the URL fragment (#name).
// For vmess:// URIs, it base64-decodes the payload and extracts the "ps" field.
func ExtractNodeName(uri string) string {
	uri = strings.TrimSpace(uri)

	// Handle vmess:// specially - it's base64-encoded JSON, not a standard URL
	if strings.HasPrefix(uri, "vmess://") {
		payload := strings.TrimPrefix(uri, "vmess://")
		// Remove any fragment that might be appended
		if idx := strings.Index(payload, "#"); idx != -1 {
			payload = payload[:idx]
		}
		payload = strings.TrimSpace(payload)
		// Try standard base64 first, then raw/URL-safe variants
		var decoded []byte
		var err error
		decoded, err = base64.StdEncoding.DecodeString(payload)
		if err != nil {
			decoded, err = base64.RawStdEncoding.DecodeString(payload)
		}
		if err != nil {
			decoded, err = base64.RawURLEncoding.DecodeString(payload)
		}
		if err == nil {
			var vmess struct {
				PS string `json:"ps"`
			}
			if json.Unmarshal(decoded, &vmess) == nil && vmess.PS != "" {
				return strings.TrimSpace(vmess.PS)
			}
		}
		return ""
	}

	// For standard URIs, extract from URL fragment (#name)
	if idx := strings.LastIndex(uri, "#"); idx != -1 && idx < len(uri)-1 {
		fragment := uri[idx+1:]
		if decoded, err := url.QueryUnescape(fragment); err == nil && decoded != "" {
			return strings.TrimSpace(decoded)
		}
		return strings.TrimSpace(fragment)
	}

	return ""
}

func (c *Config) normalize() error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	// Normalize mode name: support both multi-port and multi_port
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("不支持的运行模式 %q（可用值：pool、multi-port 或 hybrid）", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if c.Pool.Mode == "" {
		c.Pool.Mode = "sequential"
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
	}
	if c.Pool.RetryAttempts <= 0 {
		c.Pool.RetryAttempts = 3
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "0.0.0.0:9091"
	}
	if err := c.normalizeProbeConfig(); err != nil {
		return err
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}
	c.normalizeDisabledSubscriptions()

	// Subscription refresh defaults
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 2 * time.Minute
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = normalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)

	// Mark inline nodes with source
	for idx := range c.Nodes {
		c.Nodes[idx].Source = NodeSourceInline
	}

	// Load nodes from file if specified (but NOT if subscriptions exist - subscription takes priority)
	if c.NodesFile != "" && len(c.Subscriptions) == 0 {
		fileNodes, err := loadNodesFromFile(c.NodesFile)
		if err != nil {
			return fmt.Errorf("从节点文件 %q 加载节点失败: %w", c.NodesFile, err)
		}
		for idx := range fileNodes {
			fileNodes[idx].Source = NodeSourceFile
		}
		c.Nodes = append(c.Nodes, fileNodes...)
	}

	// Restore subscription nodes from local cache only. Startup must never fetch
	// remote subscriptions or rewrite nodes.txt; remote changes are applied only
	// by an explicit refresh or by the optional periodic refresh loop.
	if len(c.Subscriptions) > 0 {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
			c.NodesFile = nodesFilePath
		}
		cachedNodes, cacheErr := loadNodesFromFile(nodesFilePath)
		perSubscriptionCache, perSubscriptionCacheErr := loadSubscriptionNodeCache(
			filepath.Join(filepath.Dir(c.filePath), SubscriptionCacheFileName),
		)

		var subNodes []NodeConfig
		if perSubscriptionCacheErr == nil && len(perSubscriptionCache) > 0 {
			subNodes = c.mergeSubscriptionNodeCache(nil, perSubscriptionCache)
			log.Printf("✅ 已从本地分订阅缓存恢复 %d 个订阅节点", len(subNodes))
		} else if cacheErr == nil {
			// Legacy aggregate cache has no ownership metadata. It is safe to use as
			// the active startup set, except when every subscription is paused.
			subNodes = cachedNodes
			allSubscriptionsPaused := len(c.ActiveSubscriptions()) == 0
			for idx := range subNodes {
				subNodes[idx].Disabled = allSubscriptionsPaused
			}
			log.Printf("✅ 已从本地汇总缓存恢复 %d 个订阅节点", len(subNodes))
		} else {
			log.Printf("⚠️ 没有可用的本地订阅节点缓存，请在启动后手动更新")
		}

		for idx := range subNodes {
			subNodes[idx].Source = NodeSourceSubscription
		}
		for _, node := range subNodes {
			if node.Disabled {
				continue
			}
			c.Nodes = append(c.Nodes, node)
		}
		// A node may have been imported manually before its subscription was
		// configured. When the same stable endpoint is present in the subscription
		// cache, keep the subscription-owned definition instead of the stale inline
		// copy so the UI and runtime report the real source.
		c.Nodes = dedupeNodesPreferSubscription(c.Nodes)
	}

	if c.filePath != "" && !c.skipRuntimeRecovery {
		storedNodes, _, _, hasCatalog, catalogURLs, err := state.LoadRecoverySnapshot(c.filePath)
		if err != nil {
			return fmt.Errorf("加载运行恢复状态失败: %w", err)
		}
		configuredSubscriptions := make(map[string]bool, len(c.Subscriptions))
		for _, rawURL := range c.Subscriptions {
			configuredSubscriptions[rawURL] = c.SubscriptionEnabled(rawURL)
		}
		existing := make(map[string]struct{}, len(c.Nodes))
		for idx := range c.Nodes {
			existing[c.Nodes[idx].NodeKey()] = struct{}{}
		}
		appendRecovered := func(node NodeConfig) {
			key := node.NodeKey()
			if key == "" {
				return
			}
			if _, ok := existing[key]; ok {
				return
			}
			existing[key] = struct{}{}
			c.Nodes = append(c.Nodes, node)
		}
		if hasCatalog {
			// The active catalog is committed only after sing-box starts. It is the
			// authoritative subscription node set after an abnormal exit, including
			// an intentionally empty set.
			c.recoveredStateCatalog = true
			c.recoveredCatalogURLs = make(map[string]struct{}, len(catalogURLs))
			for _, rawURL := range catalogURLs {
				c.recoveredCatalogURLs[rawURL] = struct{}{}
			}
			kept := c.Nodes[:0]
			for _, node := range c.Nodes {
				_, wasCommitted := c.recoveredCatalogURLs[node.SubscriptionURL]
				if node.Source != NodeSourceSubscription || (node.SubscriptionURL != "" && !wasCommitted) {
					kept = append(kept, node)
				}
			}
			c.Nodes = kept
			existing = make(map[string]struct{}, len(c.Nodes))
			for idx := range c.Nodes {
				existing[c.Nodes[idx].NodeKey()] = struct{}{}
			}
			activeRecords := make([]state.NodeRecord, 0, len(storedNodes))
			for _, record := range storedNodes {
				if !record.Active || record.Source != string(NodeSourceSubscription) {
					continue
				}
				if _, configured := configuredSubscriptions[record.SubscriptionURL]; !configured {
					continue
				}
				activeRecords = append(activeRecords, record)
			}
			sort.SliceStable(activeRecords, func(i, j int) bool {
				if activeRecords[i].Order == activeRecords[j].Order {
					return activeRecords[i].ID < activeRecords[j].ID
				}
				return activeRecords[i].Order < activeRecords[j].Order
			})
			for _, record := range activeRecords {
				enabled := configuredSubscriptions[record.SubscriptionURL]
				if !enabled {
					continue
				}
				appendRecovered(NodeConfig{
					Name: record.Name, URI: record.URI, Port: record.Port,
					Username: record.Username, Password: record.Password,
					Source: NodeSourceSubscription, SubscriptionURL: record.SubscriptionURL,
					StateKey: record.ID,
				})
			}
		}
	}

	// portCursor is an int (not uint16) so the >65535 exhaustion guard fires
	// instead of wrapping to 0 and assigning unbindable low ports.
	portCursor := int(c.MultiPort.BasePort)
	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)

		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("节点 %d 缺少 URI", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		// Fallback to default name if still empty
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

		// Provisional port assignment only. The real, bind-checked, collision-safe
		// assignment is done once by applyPersistedPorts → NormalizeWithPortMap
		// right after normalize() returns. Probing IsPortAvailable per node here
		// would double the socket open/close work at startup (16k+ syscalls for
		// 8k nodes) for a result that is immediately discarded.
		if c.Nodes[idx].Port == 0 && (c.Mode == "multi-port" || c.Mode == "hybrid") {
			c.Nodes[idx].Port = uint16(portCursor)
			portCursor++
		} else if c.Nodes[idx].Port == 0 {
			c.Nodes[idx].Port = uint16(portCursor)
			portCursor++
		}

		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			if c.Nodes[idx].Username == "" {
				c.Nodes[idx].Username = c.MultiPort.Username
				c.Nodes[idx].Password = c.MultiPort.Password
			}
		}
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	// Log config defaults
	c.normalizeLogConfig()

	// Auto-fix port conflicts in hybrid mode (pool port vs multi-port)
	if c.Mode == "hybrid" {
		poolPort := c.Listener.Port
		usedPorts := make(map[uint16]bool)
		usedPorts[poolPort] = true
		for idx := range c.Nodes {
			usedPorts[c.Nodes[idx].Port] = true
		}
		for idx := range c.Nodes {
			if c.Nodes[idx].Port == poolPort {
				// Find next available port
				newPort := c.Nodes[idx].Port + 1
				for usedPorts[newPort] || !IsPortAvailable(c.MultiPort.Address, newPort) {
					newPort++
					if newPort > 65535 {
						return fmt.Errorf("节点 %q 与节点池端口 %d 冲突后没有可用端口", c.Nodes[idx].Name, poolPort)
					}
				}
				log.Printf("⚠️  节点 %q 的端口 %d 与节点池端口冲突，已重新分配到 %d", c.Nodes[idx].Name, poolPort, newPort)
				usedPorts[newPort] = true
				c.Nodes[idx].Port = newPort
			}
		}
	}

	if err := c.normalizeSticky(); err != nil {
		return err
	}

	return nil
}

// BuildPortMap creates a mapping from node URI to port for existing nodes.
// This is used to preserve port assignments when reloading configuration.
func (c *Config) BuildPortMap() map[string]uint16 {
	portMap := make(map[string]uint16)
	for _, node := range c.Nodes {
		if node.Port > 0 {
			portMap[node.NodeKey()] = node.Port
		}
	}
	return portMap
}

// nodePortMapFile is the sidecar file storing node→port assignments so they
// survive a process restart.
const nodePortMapFile = "node_ports.json"

// portMapPath returns the path of the port-map sidecar, located next to the
// main config file. It is empty when the config path is unknown.
func (c *Config) portMapPath() string {
	if c.filePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(c.filePath), nodePortMapFile)
}

// loadNodePortMap reads a previously saved stableNodeKey→port mapping. It
// returns nil on any error (missing file, unreadable, bad JSON); callers treat
// that as "no persisted ports", so a corrupt sidecar never blocks startup.
func loadNodePortMap(path string) map[string]uint16 {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m map[string]uint16
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}

// bridgeLegacyPortKeys upgrades a port-map sidecar written by an older version
// (keyed by the raw full node URI) to the current stable-key scheme. For each
// node whose stable key is absent but whose raw URI is present in the map, it
// copies the port under the stable key. This makes a one-time format upgrade
// transparent: ports are preserved instead of all being reassigned on the first
// boot after upgrading. It mutates saved in place and is a no-op once the
// sidecar has been rewritten with stable keys.
func bridgeLegacyPortKeys(nodes []NodeConfig, saved map[string]uint16) {
	for i := range nodes {
		stableKey := nodes[i].NodeKey()
		if _, ok := saved[stableKey]; ok {
			continue
		}
		legacyKey := strings.TrimSpace(nodes[i].URI)
		if port, ok := saved[legacyKey]; ok && port > 0 {
			saved[stableKey] = port
		}
	}
}

// SaveNodePortMap persists the current node→port assignments next to the config
// file so a restart can restore them. It is a no-op in pool mode, where nodes
// have no per-node ports.
func (c *Config) SaveNodePortMap() error {
	if c == nil {
		return errors.New("配置不能为空")
	}
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	path := c.portMapPath()
	if path == "" {
		return errors.New("配置文件路径未知")
	}
	data, err := json.MarshalIndent(c.BuildPortMap(), "", "  ")
	if err != nil {
		return fmt.Errorf("编码端口映射失败: %w", err)
	}
	if err := writeFileWithLock(path, data, 0o644); err != nil {
		return fmt.Errorf("写入端口映射 %q 失败: %w", path, err)
	}
	return nil
}

// applyPersistedPorts restores the on-disk node→port mapping so a restart keeps
// every node on the proxy port it previously used, then rewrites the mapping so
// the sidecar exists from first boot and drops entries for removed nodes.
//
// It clears the provisional ports assigned by normalize() first, making
// NormalizeWithPortMap the single, collision-safe authority: nodes whose stable
// identity matches a saved entry get their saved port, and the rest get fresh,
// non-conflicting ports. A corrupt or missing sidecar simply means "no saved
// ports" and the freshly assigned ports stand.
func (c *Config) applyPersistedPorts() error {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	// Keep the existing sidecar while the proxy starts without nodes. A later
	// subscription refresh may restore the same nodes, in which case their
	// previous ports should still be available for preservation.
	if len(c.Nodes) == 0 {
		return nil
	}
	// normalize() only assigned provisional ports (no bind checks). Run the
	// authoritative, bind-checked assignment exactly once here. A saved sidecar
	// supplies preserved ports; an empty/missing one means "assign all fresh".
	saved := loadNodePortMap(c.portMapPath())
	if len(saved) > 0 {
		// Migrate any legacy entries: sidecars written before stableNodeKey was
		// keyed by the raw full URI. Without this bridge, every node's stable key
		// would miss on the first post-upgrade boot and all proxy ports would be
		// reassigned at once. Map legacy raw-URI keys onto the new stable keys.
		bridgeLegacyPortKeys(c.Nodes, saved)
	}
	for i := range c.Nodes {
		c.Nodes[i].Port = 0
	}
	if err := c.NormalizeWithPortMap(saved); err != nil {
		return fmt.Errorf("恢复已保存端口失败: %w", err)
	}
	// Persisting is best-effort by design: the proxy runs correctly without the
	// sidecar; only a subsequent restart would re-derive ports. A write failure
	// is logged rather than fatal.
	if err := c.SaveNodePortMap(); err != nil {
		log.Printf("⚠️  保存节点端口失败: %v", err)
	}
	return nil
}

// NormalizeWithPortMap applies defaults and validation, preserving port assignments
// for nodes that exist in the provided port map.
func (c *Config) NormalizeWithPortMap(portMap map[string]uint16) error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("不支持的运行模式 %q（可用值：pool、multi-port 或 hybrid）", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if c.Pool.Mode == "" {
		c.Pool.Mode = "sequential"
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
	}
	if c.Pool.RetryAttempts <= 0 {
		c.Pool.RetryAttempts = 3
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "0.0.0.0:9091"
	}
	if err := c.normalizeProbeConfig(); err != nil {
		return err
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}
	c.normalizeDisabledSubscriptions()
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 2 * time.Minute
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = normalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)

	// Build set of ports already assigned from portMap
	usedPorts := make(map[uint16]bool)
	if c.Mode == "hybrid" {
		usedPorts[c.Listener.Port] = true
	}

	// First pass: assign ports from portMap for existing nodes
	preservedPorts := 0
	duplicatePortHits := 0
	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)
		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("节点 %d 缺少 URI", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

		// Check if this node has a preserved port from portMap. Guard against a
		// port that was already claimed by an earlier node sharing the same
		// stable key (e.g. a subscription listing the same server twice under
		// different display names): preserving it again would bind the same
		// proxy port twice (EADDRINUSE). Such a node is left at Port==0 so the
		// second pass assigns it a fresh, collision-free port.
		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			nodeKey := c.Nodes[idx].NodeKey()
			if existingPort, ok := portMap[nodeKey]; ok && existingPort > 0 {
				if usedPorts[existingPort] {
					duplicatePortHits++
				} else {
					c.Nodes[idx].Port = existingPort
					usedPorts[existingPort] = true
					preservedPorts++
				}
			}
		}
	}

	// Second pass: assign new ports for nodes without preserved ports. portCursor
	// is an int (not uint16) so the >65535 exhaustion guard actually fires:
	// a uint16 cursor would wrap to 0 and silently hand out unbindable low ports.
	portCursor := int(c.MultiPort.BasePort)
	newPorts := 0
	for idx := range c.Nodes {
		if c.Nodes[idx].Port == 0 && (c.Mode == "multi-port" || c.Mode == "hybrid") {
			// Find next available port that's not used
			for usedPorts[uint16(portCursor)] || !IsPortAvailable(c.MultiPort.Address, uint16(portCursor)) {
				portCursor++
				if portCursor > 65535 {
					return fmt.Errorf("从 %d 开始没有可用端口", c.MultiPort.BasePort)
				}
			}
			c.Nodes[idx].Port = uint16(portCursor)
			usedPorts[uint16(portCursor)] = true
			newPorts++
			portCursor++
		} else if c.Nodes[idx].Port == 0 {
			c.Nodes[idx].Port = uint16(portCursor)
			portCursor++
		}

		// Apply default credentials
		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			if c.Nodes[idx].Username == "" {
				c.Nodes[idx].Username = c.MultiPort.Username
				c.Nodes[idx].Password = c.MultiPort.Password
			}
		}
	}
	if c.Mode == "multi-port" || c.Mode == "hybrid" {
		log.Printf("✅ 端口规范化完成：保留=%d，新分配=%d，重复标识冲突=%d，节点总数=%d",
			preservedPorts, newPorts, duplicatePortHits, len(c.Nodes))
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	c.normalizeLogConfig()

	if err := c.normalizeSticky(); err != nil {
		return err
	}

	return nil
}

// normalizeSticky applies defaults and validation for the optional sticky entry port.
// Sticky sessions only apply to the shared pool entry, so they are disabled
// outside pool/hybrid mode. Must run after node ports are assigned.
func (c *Config) normalizeSticky() error {
	// Backward compatibility: older versions stored the selected node under
	// pool.fixed_node and applied it to the primary listener. The selection now
	// belongs exclusively to the sticky listener.
	if strings.TrimSpace(c.Sticky.FixedNode) == "" && strings.TrimSpace(c.Pool.FixedNode) != "" {
		c.Sticky.FixedNode = strings.TrimSpace(c.Pool.FixedNode)
	}
	c.Pool.FixedNode = ""
	c.Sticky.FixedNode = strings.TrimSpace(c.Sticky.FixedNode)

	if !c.Sticky.Enabled {
		return nil
	}
	if c.Mode != "pool" && c.Mode != "hybrid" {
		log.Printf("⚠️  已设置 sticky.enabled，但当前模式为 %q；粘性代理仅适用于 pool/hybrid 模式，已自动关闭", c.Mode)
		c.Sticky.Enabled = false
		return nil
	}
	if c.Sticky.Port == 0 {
		if c.Listener.Port >= 65535 {
			return fmt.Errorf("无法自动分配 sticky.port（监听端口为 %d），请显式设置 sticky.port", c.Listener.Port)
		}
		c.Sticky.Port = c.Listener.Port + 1
	}
	if c.Sticky.Port == c.Listener.Port {
		return fmt.Errorf("sticky.port %d 与监听端口冲突", c.Sticky.Port)
	}
	for idx := range c.Nodes {
		if c.Nodes[idx].Port == c.Sticky.Port {
			return fmt.Errorf("sticky.port %d 与节点 %q 的端口冲突", c.Sticky.Port, c.Nodes[idx].Name)
		}
	}
	return nil
}

// normalizeLogConfig applies defaults to the log config.
func (c *Config) normalizeLogConfig() {
	if c.Log.Output == "" {
		c.Log.Output = "stdout"
	}
	if c.Log.File == "" {
		c.Log.File = "logs/Proxy2API.log"
	}
	// Resolve relative log file path against config dir
	if c.filePath != "" && !filepath.IsAbs(c.Log.File) {
		c.Log.File = filepath.Join(filepath.Dir(c.filePath), c.Log.File)
	}
	if c.Log.MaxSize <= 0 {
		c.Log.MaxSize = 50
	}
	if c.Log.MaxBackups <= 0 {
		c.Log.MaxBackups = 3
	}
	if c.Log.MaxAge <= 0 {
		c.Log.MaxAge = 7
	}
}

func (c *Config) normalizeProbeConfig() error {
	if c.Probe.Target == "" {
		c.Probe.Target = c.Management.ProbeTarget
	}
	if c.Probe.Interval <= 0 {
		c.Probe.Interval = c.Management.ProbeInterval
	}
	if c.Probe.Timeout <= 0 {
		c.Probe.Timeout = c.Management.ProbeTimeout
	}
	if c.Probe.Concurrency <= 0 {
		c.Probe.Concurrency = c.Management.ProbeConcurrency
	}
	if c.Probe.Target == "" {
		c.Probe.Target = "http://cp.cloudflare.com/generate_204"
	}
	if c.Probe.Interval <= 0 {
		c.Probe.Interval = 5 * time.Minute
	}
	if c.Probe.Timeout <= 0 {
		c.Probe.Timeout = DefaultProbeTimeout
	} else if err := ValidateProbeTimeout(c.Probe.Timeout); err != nil {
		return err
	}
	if c.Probe.Concurrency <= 0 {
		c.Probe.Concurrency = 32
	}
	return nil
}

// ManagementEnabled reports whether the monitoring endpoint should run.
func (c *Config) ManagementEnabled() bool {
	if c.Management.Enabled == nil {
		return true
	}
	return *c.Management.Enabled
}

// ProbeConcurrencyOrDefault returns the configured probe concurrency clamped
// to a safe range (1-1024). When unset or invalid, a sensible default is used.
func (c *Config) ProbeConcurrencyOrDefault() int {
	v := c.Probe.Concurrency
	if v <= 0 {
		return 32
	}
	if v > 1024 {
		return 1024
	}
	return v
}

// ProbeIntervalOrDefault returns the interval between automatic full probes.
func (c *Config) ProbeIntervalOrDefault() time.Duration {
	if c.Probe.Interval <= 0 {
		return 5 * time.Minute
	}
	return c.Probe.Interval
}

// ProbeTimeoutOrDefault returns the timeout applied to each node probe.
func (c *Config) ProbeTimeoutOrDefault() time.Duration {
	if c.Probe.Timeout <= 0 {
		return DefaultProbeTimeout
	}
	return c.Probe.Timeout
}

// ProbeTargetOrDefault returns this project's health-check destination.
func (c *Config) ProbeTargetOrDefault() string {
	if strings.TrimSpace(c.Probe.Target) == "" {
		return "http://cp.cloudflare.com/generate_204"
	}
	return c.Probe.Target
}

const (
	DefaultProbeTimeout = 110 * time.Second
	MinimumProbeTimeout = 25 * time.Second
	probeTimeoutStep    = 5 * time.Second
)

// ValidateProbeTimeout verifies the total per-node probe budget. The probe
// implementation divides this budget into five equal attempts: one primary
// request and four Trace attempts.
func ValidateProbeTimeout(timeout time.Duration) error {
	if timeout < MinimumProbeTimeout {
		return fmt.Errorf("探测超时必须至少为 %s", MinimumProbeTimeout)
	}
	if timeout%probeTimeoutStep != 0 {
		return fmt.Errorf("探测超时必须是 %s 的倍数", probeTimeoutStep)
	}
	return nil
}

// loadNodesFromFile reads a nodes file where each line is a proxy URI
// Lines starting with # are comments, empty lines are ignored
func loadNodesFromFile(path string) ([]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseNodesFromContent(string(data))
}

// LoadNodesFromFile reads a nodes file where each non-comment line is a proxy URI.
func LoadNodesFromFile(path string) ([]NodeConfig, error) {
	return loadNodesFromFile(path)
}

func loadSubscriptionNodeCache(path string) (map[string][]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cached map[string][]NodeConfig
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, err
	}
	return cached, nil
}

// mergeSubscriptionNodeCache adds cached nodes for subscriptions that were not
// fetched and retains paused subscriptions as suppressed runtime members. Fresh
// enabled nodes always win when two subscriptions contain the same endpoint.
func (c *Config) mergeSubscriptionNodeCache(fetched []NodeConfig, cached map[string][]NodeConfig) []NodeConfig {
	merged := make([]NodeConfig, 0, len(fetched))
	seenNodes := make(map[string]struct{}, len(fetched))
	freshSubscriptions := make(map[string]struct{}, len(c.Subscriptions))
	appendNode := func(node NodeConfig, rawURL string, disabled bool) {
		node.Source = NodeSourceSubscription
		node.SubscriptionURL = rawURL
		node.Disabled = disabled
		key := node.NodeKey()
		if _, duplicate := seenNodes[key]; duplicate {
			return
		}
		seenNodes[key] = struct{}{}
		merged = append(merged, node)
	}

	for _, node := range fetched {
		if node.SubscriptionURL != "" {
			freshSubscriptions[node.SubscriptionURL] = struct{}{}
		}
		appendNode(node, node.SubscriptionURL, false)
	}
	for _, enabledPass := range []bool{true, false} {
		for _, rawURL := range c.Subscriptions {
			enabled := c.SubscriptionEnabled(rawURL)
			if enabled != enabledPass {
				continue
			}
			if enabled {
				if _, fresh := freshSubscriptions[rawURL]; fresh {
					continue
				}
			}
			for _, node := range cached[rawURL] {
				appendNode(node, rawURL, !enabled)
			}
		}
	}
	return merged
}

func normalizeSubscriptionFetchConcurrency(v int) int {
	if v <= 0 {
		return defaultSubscriptionFetchConcurrency
	}
	if v > maxSubscriptionFetchConcurrency {
		return maxSubscriptionFetchConcurrency
	}
	return v
}

func newSubscriptionHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   minDuration(timeout, 10*time.Second),
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   minDuration(timeout, 10*time.Second),
		ResponseHeaderTimeout: minDuration(timeout, 15*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if a < b {
		return a
	}
	return b
}

// RedactURL removes credentials and query data from a URL before logging.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<无效地址>"
	}
	u.User = nil
	if u.Path != "" && u.Path != "/" {
		u.Path = "/..."
		u.RawPath = ""
	}
	if u.RawQuery != "" {
		u.RawQuery = "redacted=1"
	}
	u.Fragment = ""
	return u.String()
}

func dedupeSubscriptionURLs(urls []string) (unique []string, deduped int) {
	seen := make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			deduped++
			continue
		}
		seen[raw] = struct{}{}
		unique = append(unique, raw)
	}
	return unique, deduped
}

func dedupeNodesByKey(nodes []NodeConfig) ([]NodeConfig, int) {
	if len(nodes) < 2 {
		return nodes, 0
	}
	seen := make(map[string]struct{}, len(nodes))
	out := nodes[:0]
	deduped := 0
	for _, node := range nodes {
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" {
			deduped++
			continue
		}
		key := node.NodeKey()
		if key == "" {
			key = node.URI
		}
		if _, ok := seen[key]; ok {
			deduped++
			continue
		}
		seen[key] = struct{}{}
		out = append(out, node)
	}
	return out, deduped
}

// dedupeNodesPreferSubscription removes duplicate stable endpoints while
// allowing a fetched subscription definition to take ownership of a matching
// inline/file definition. This prevents previously imported subscription nodes
// from being rendered as manual after a subscription is added.
func dedupeNodesPreferSubscription(nodes []NodeConfig) []NodeConfig {
	if len(nodes) < 2 {
		return nodes
	}

	seen := make(map[string]int, len(nodes))
	out := make([]NodeConfig, 0, len(nodes))
	for _, node := range nodes {
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" {
			continue
		}
		key := node.NodeKey()
		if key == "" {
			key = node.URI
		}
		if previous, ok := seen[key]; ok {
			if node.Source == NodeSourceSubscription && out[previous].Source != NodeSourceSubscription {
				out[previous] = node
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, node)
	}
	return out
}

// FetchSubscriptionNodes fetches subscription URLs concurrently, parses all
// supported subscription formats, and deduplicates URLs/nodes by stable identity.
func FetchSubscriptionNodes(ctx context.Context, urls []string, opts SubscriptionFetchOptions) ([]NodeConfig, SubscriptionFetchStats) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	concurrency := normalizeSubscriptionFetchConcurrency(opts.Concurrency)
	uniqueURLs, dedupedURLs := dedupeSubscriptionURLs(urls)
	stats := SubscriptionFetchStats{
		RequestedURLs: len(urls),
		UniqueURLs:    len(uniqueURLs),
		DedupedURLs:   dedupedURLs,
	}
	if len(uniqueURLs) == 0 {
		return nil, stats
	}

	client := opts.Client
	if client == nil {
		client = newSubscriptionHTTPClient(timeout)
	}

	type result struct {
		url   string
		nodes []NodeConfig
		err   error
	}
	jobs := make(chan string)
	results := make(chan result, len(uniqueURLs))

	workerCount := concurrency
	if workerCount > len(uniqueURLs) {
		workerCount = len(uniqueURLs)
	}
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for subURL := range jobs {
				nodes, err := fetchSubscriptionWithClient(ctx, client, subURL, timeout)
				results <- result{url: subURL, nodes: nodes, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, subURL := range uniqueURLs {
			select {
			case <-ctx.Done():
				return
			case jobs <- subURL:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	allNodes := make([]NodeConfig, 0)
	processed := 0
	for res := range results {
		processed++
		if res.err != nil {
			stats.Failed++
			stats.LastError = res.err
			if opts.Loggerf != nil {
				opts.Loggerf("⚠️ 加载订阅 %s 失败: %v（已跳过）", RedactURL(res.url), res.err)
			}
			continue
		}
		if len(res.nodes) == 0 {
			stats.Empty++
		} else {
			stats.Successful++
		}
		if opts.Loggerf != nil {
			opts.Loggerf("✅ Loaded %d nodes from subscription %s", len(res.nodes), RedactURL(res.url))
		}
		for idx := range res.nodes {
			res.nodes[idx].Source = NodeSourceSubscription
			res.nodes[idx].SubscriptionURL = res.url
		}
		allNodes = append(allNodes, res.nodes...)
	}
	if missing := len(uniqueURLs) - processed; missing > 0 {
		if err := ctx.Err(); err != nil {
			stats.Failed += missing
			stats.LastError = err
			if opts.Loggerf != nil {
				opts.Loggerf("⚠️ 订阅获取上下文已结束，仍有 %d 个订阅地址未处理: %v", missing, err)
			}
		}
	}

	stats.Nodes = len(allNodes)
	allNodes, stats.DedupedNodes = dedupeNodesByKey(allNodes)
	return allNodes, stats
}

// loadNodesFromSubscription fetches and parses nodes from a subscription URL
// Supports multiple formats: base64 encoded, plain text, clash yaml, etc.
func loadNodesFromSubscription(subURL string, timeout time.Duration) ([]NodeConfig, error) {
	return fetchSubscriptionWithClient(context.Background(), newSubscriptionHTTPClient(timeout), subURL, timeout)
}

func fetchSubscriptionWithClient(ctx context.Context, client *http.Client, subURL string, timeout time.Duration) ([]NodeConfig, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if ctx == nil {
		ctx = context.Background()
	}
	parsed, err := url.Parse(subURL)
	if err != nil {
		return nil, fmt.Errorf("解析订阅 URL 失败: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("不支持的订阅协议 %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("订阅 URL 缺少主机地址")
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, "GET", subURL, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	ApplySubscriptionRequestHeaders(req)

	resp, err := client.Do(req)
	if err != nil {
		return nil, redactSubscriptionError("fetch subscription", subURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("订阅返回状态码 %d", resp.StatusCode)
	}

	limitedReader := io.LimitReader(resp.Body, maxSubscriptionBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if len(body) > maxSubscriptionBodySize {
		return nil, fmt.Errorf("订阅响应超过 %d 字节", maxSubscriptionBodySize)
	}

	content := string(body)

	// Try to detect and parse different formats
	return parseSubscriptionContent(content)
}

func redactSubscriptionError(op, rawURL string, err error) error {
	if err == nil {
		return nil
	}
	redacted := RedactURL(rawURL)
	message := strings.ReplaceAll(err.Error(), rawURL, redacted)
	if parsed, parseErr := url.Parse(rawURL); parseErr == nil {
		message = strings.ReplaceAll(message, parsed.String(), redacted)
	}
	return fmt.Errorf("%s: %s", op, message)
}

// parseSubscriptionContent tries to parse subscription content in various formats (optimized)
func parseSubscriptionContent(content string) ([]NodeConfig, error) {
	content = strings.TrimSpace(strings.TrimPrefix(content, "\uFEFF"))
	var (
		nodes []NodeConfig
		err   error
	)

	// Detect Clash YAML by a line-anchored top-level "proxies:" key anywhere in
	// the document. The whole content is scanned (not just a 16 KB prefix): full
	// Clash configs often place proxies after large dns / rule / proxy-provider
	// sections, so a fixed-size window would misdetect them as base64/plaintext
	// and drop every node. Line-anchoring avoids matching a stray "proxies:"
	// inside a base64 blob.
	if strings.HasPrefix(content, "proxies:") || strings.Contains(content, "\nproxies:") {
		nodes, err = parseClashYAML(content)
		if err != nil {
			return nil, err
		}
		return filterSubscriptionInfoNodes(nodes), nil
	}

	// Check if it's base64 encoded (common for v2ray subscriptions). Providers
	// use both standard and URL-safe alphabets, with or without padding.
	if isBase64(content) {
		if decoded, decodeErr := decodeSubscriptionBase64(content); decodeErr == nil {
			content = string(decoded)
		}
	}

	// Parse as plain text (one URI per line)
	nodes, err = parseNodesFromContent(content)
	if err != nil {
		return nil, err
	}
	return filterSubscriptionInfoNodes(nodes), nil
}

// filterSubscriptionInfoNodes removes the non-proxy entries that providers
// commonly encode as working proxy URIs to display account status or ads.
func filterSubscriptionInfoNodes(nodes []NodeConfig) []NodeConfig {
	if len(nodes) == 0 {
		return nodes
	}

	filtered := make([]NodeConfig, 0, len(nodes))
	for _, node := range nodes {
		name := strings.TrimSpace(node.Name)
		if name == "" {
			name = ExtractNodeName(node.URI)
		}
		if isSubscriptionInfoName(name) {
			continue
		}
		filtered = append(filtered, node)
	}

	if removed := len(nodes) - len(filtered); removed > 0 {
		log.Printf("[订阅] 已过滤 %d 个流量或账号信息节点", removed)
	}
	return filtered
}

func isSubscriptionInfoName(name string) bool {
	compact := compactSubscriptionNodeName(name)
	if compact == "" {
		return false
	}

	// These phrases describe account state or provider navigation rather than
	// a proxy location. Separators and whitespace have already been removed.
	for _, keyword := range subscriptionInfoNameKeywords {
		if strings.Contains(compact, keyword) {
			return true
		}
	}

	// Providers also use terse labels such as "流量: 100 GB" or
	// "Traffic 20%". Require a value/status marker so names such as
	// "流量优化专线" remain valid proxy nodes.
	if strings.Contains(compact, "流量") &&
		(containsDigit(compact) || containsAny(compact, "%", "倍率", "用量", "无限", "不限")) {
		return true
	}
	if containsAny(compact, "traffic", "bandwidth", "quota") &&
		(containsDigit(compact) || containsAny(compact, "%", "left", "limit", "usage")) {
		return true
	}

	return false
}

func compactSubscriptionNodeName(name string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		switch r {
		case '-', '_', '|', '–', '—', ':', '：', '[', ']', '【', '】', '(', ')', '（', '）':
			return -1
		default:
			return unicode.ToLower(r)
		}
	}, strings.TrimSpace(name))
}

func containsDigit(value string) bool {
	for _, r := range value {
		if unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

// ParseSubscriptionContent parses subscription content in various formats (base64, plain text, Clash YAML).
// This is exported for use by the subscription manager.
func ParseSubscriptionContent(content string) ([]NodeConfig, error) {
	return parseSubscriptionContent(content)
}

// ParseNodeImportContent parses uploaded or pasted node configuration. It
// accepts Clash YAML, base64/plain subscriptions, Proxy2API config YAML with a
// top-level nodes field, YAML node arrays, and a single YAML node object.
func ParseNodeImportContent(content string) ([]NodeConfig, error) {
	content = strings.TrimSpace(strings.TrimPrefix(content, "\uFEFF"))
	if content == "" {
		return nil, errors.New("导入内容不能为空")
	}

	parsed, subscriptionErr := parseSubscriptionContent(content)
	if subscriptionErr == nil && len(parsed) > 0 {
		return parsed, nil
	}

	var wrapped struct {
		Nodes []NodeConfig `yaml:"nodes"`
	}
	if err := yaml.Unmarshal([]byte(content), &wrapped); err == nil && len(wrapped.Nodes) > 0 {
		return normalizeImportedNodeConfigs(wrapped.Nodes)
	}

	var list []NodeConfig
	if err := yaml.Unmarshal([]byte(content), &list); err == nil && len(list) > 0 {
		return normalizeImportedNodeConfigs(list)
	}

	var single NodeConfig
	if err := yaml.Unmarshal([]byte(content), &single); err == nil && strings.TrimSpace(single.URI) != "" {
		return normalizeImportedNodeConfigs([]NodeConfig{single})
	}

	if subscriptionErr != nil {
		return nil, subscriptionErr
	}
	return nil, errors.New("未解析到支持的节点，请检查 YAML 或节点 URI 格式")
}

func normalizeImportedNodeConfigs(nodes []NodeConfig) ([]NodeConfig, error) {
	result := make([]NodeConfig, 0, len(nodes))
	for _, node := range nodes {
		node.Name = strings.TrimSpace(node.Name)
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" || !IsProxyURI(node.URI) {
			continue
		}
		if node.Name == "" {
			node.Name = ExtractNodeName(node.URI)
		}
		result = append(result, node)
	}
	result = filterSubscriptionInfoNodes(result)
	if len(result) == 0 {
		return nil, errors.New("YAML 中没有支持的节点 URI")
	}
	return result, nil
}

// parseNodesFromContent parses nodes from plain text content (one URI per line)
func parseNodesFromContent(content string) ([]NodeConfig, error) {
	var nodes []NodeConfig
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check if it's a valid proxy URI
		if IsProxyURI(line) {
			nodes = append(nodes, NodeConfig{
				URI: line,
			})
		}
	}

	return nodes, nil
}

// isBase64 checks if a string looks like base64 encoded content (optimized version)
func isBase64(s string) bool {
	// Remove whitespace introduced by line-wrapped subscription responses.
	s = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(s))
	if len(s) == 0 {
		return false
	}

	// Quick check: if it contains proxy URI schemes, it's not base64
	if strings.Contains(s, "://") {
		return false
	}

	// Check character set - accept standard and URL-safe base64 alphabets.
	// This is much faster than trying to decode
	padding := false
	for _, c := range s {
		if c == '=' {
			padding = true
			continue
		}
		if padding {
			return false
		}
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '-' || c == '_') {
			return false
		}
	}

	// A four-byte quantum cannot have exactly one trailing byte.
	return len(s)%4 != 1
}

func decodeSubscriptionBase64(value string) ([]byte, error) {
	compact := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(value))
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	var lastErr error
	for _, encoding := range encodings {
		decoded, err := encoding.DecodeString(compact)
		if err == nil {
			return decoded, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// IsProxyURI checks if a string is a valid proxy URI
func IsProxyURI(s string) bool {
	schemes := []string{"vmess://", "vless://", "trojan://", "ss://", "shadowsocks://", "ssr://", "hysteria://", "hysteria2://", "hy2://", "tuic://", "socks5://", "socks5h://", "socks://", "http://", "https://", "anytls://"}
	lower := strings.ToLower(s)
	for _, scheme := range schemes {
		if strings.HasPrefix(lower, scheme) {
			return true
		}
	}
	return false
}

// clashConfig represents a minimal Clash configuration for parsing proxies
// flexInt handles YAML values that may be either int or quoted string.
type flexInt int

func (fi *flexInt) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var intVal int
	if err := unmarshal(&intVal); err == nil {
		*fi = flexInt(intVal)
		return nil
	}
	var strVal string
	if err := unmarshal(&strVal); err != nil {
		return fmt.Errorf("无法解析端口：应为整数或字符串")
	}
	parsed, err := strconv.Atoi(strVal)
	if err != nil {
		return fmt.Errorf("无法将端口 %q 解析为整数: %w", strVal, err)
	}
	*fi = flexInt(parsed)
	return nil
}

type clashConfig struct {
	Proxies []yaml.Node `yaml:"proxies"`
}

type clashProxy struct {
	Name              string                 `yaml:"name"`
	Type              string                 `yaml:"type"`
	Server            string                 `yaml:"server"`
	Port              flexInt                `yaml:"port"`
	Ports             string                 `yaml:"ports"`
	UUID              string                 `yaml:"uuid"`
	Username          string                 `yaml:"username"`
	Password          string                 `yaml:"password"`
	Cipher            string                 `yaml:"cipher"`
	AlterId           int                    `yaml:"alterId"`
	Network           string                 `yaml:"network"`
	TLS               bool                   `yaml:"tls"`
	SkipCertVerify    bool                   `yaml:"skip-cert-verify"`
	ServerName        string                 `yaml:"servername"`
	SNI               string                 `yaml:"sni"`
	Flow              string                 `yaml:"flow"`
	PacketEncoding    string                 `yaml:"packet-encoding"`
	UDP               bool                   `yaml:"udp"`
	WSOpts            *clashWSOptions        `yaml:"ws-opts"`
	GrpcOpts          *clashGrpcOptions      `yaml:"grpc-opts"`
	RealityOpts       *clashRealityOptions   `yaml:"reality-opts"`
	ClientFingerprint string                 `yaml:"client-fingerprint"`
	Obfs              string                 `yaml:"obfs"`
	ObfsPassword      string                 `yaml:"obfs-password"`
	MPort             string                 `yaml:"mport"`
	HopInterval       string                 `yaml:"hop-interval"`
	Plugin            string                 `yaml:"plugin"`
	PluginOpts        map[string]interface{} `yaml:"plugin-opts"`
	// TUIC-specific fields
	ALPN                 []string `yaml:"alpn"`
	CongestionController string   `yaml:"congestion-controller"`
	UDPRelayMode         string   `yaml:"udp-relay-mode"`
	// ShadowsocksR-specific fields
	Protocol      string `yaml:"protocol"`
	ProtocolParam string `yaml:"protocol-param"`
	ObfsParam     string `yaml:"obfs-param"`
	// Hysteria v1-specific fields
	AuthStr        string `yaml:"auth-str"`
	Auth           string `yaml:"auth"`
	UpMbps         int    `yaml:"up"`
	DownMbps       int    `yaml:"down"`
	PeerSNI        string `yaml:"peer"`
	RecvWindow     uint64 `yaml:"recv-window"`
	RecvWindowConn uint64 `yaml:"recv-window-conn"`
	DisableMTU     bool   `yaml:"disable_mtu_discovery"`
}

type clashWSOptions struct {
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
}

type clashGrpcOptions struct {
	GrpcServiceName string `yaml:"grpc-service-name"`
}

type clashRealityOptions struct {
	PublicKey string `yaml:"public-key"`
	ShortID   string `yaml:"short-id"`
}

// parseClashYAML parses Clash YAML format and converts to NodeConfig.
// Per-proxy decoding: a single malformed entry won't fail the whole subscription;
// failed proxies are logged and skipped (fixes #23).
func parseClashYAML(content string) ([]NodeConfig, error) {
	var clash clashConfig
	if err := yaml.Unmarshal([]byte(content), &clash); err != nil {
		return nil, fmt.Errorf("解析 Clash YAML 失败: %w", err)
	}

	var nodes []NodeConfig
	skipped := 0
	for i, raw := range clash.Proxies {
		var proxy clashProxy
		if err := raw.Decode(&proxy); err != nil {
			skipped++
			log.Printf("[订阅] 警告：跳过代理 #%d（解码失败）: %v", i, err)
			continue
		}
		uri := convertClashProxyToURI(proxy)
		if uri == "" {
			skipped++
			log.Printf("[订阅] 警告：跳过代理 %q（不支持的类型 %q）", proxy.Name, proxy.Type)
			continue
		}
		nodes = append(nodes, NodeConfig{
			Name: proxy.Name,
			URI:  uri,
		})
	}
	if skipped > 0 {
		log.Printf("[订阅] 已解析 %d 个节点，跳过 %d 个格式错误或不支持的条目", len(nodes), skipped)
	}

	return nodes, nil
}

// convertClashProxyToURI converts a Clash proxy config to a standard URI
func convertClashProxyToURI(p clashProxy) string {
	switch strings.ToLower(p.Type) {
	case "vmess":
		return buildVMessURI(p)
	case "vless":
		return buildVLESSURI(p)
	case "trojan":
		return buildTrojanURI(p)
	case "anytls":
		return buildAnyTLSURI(p)
	case "ss", "shadowsocks":
		return buildShadowsocksURI(p)
	case "hysteria2", "hy2":
		return buildHysteria2URI(p)
	case "tuic":
		return buildTUICURI(p)
	case "ssr", "shadowsocksr":
		return buildShadowsocksRURI(p)
	case "hysteria":
		return buildHysteriaURI(p)
	case "http", "https":
		return buildHTTPProxyURI(p)
	case "socks5", "socks":
		return buildSOCKSProxyURI(p)
	default:
		return ""
	}
}

func buildSOCKSProxyURI(p clashProxy) string {
	port := int(p.Port)
	if port == 0 {
		port = 1080
	}
	u := &url.URL{
		Scheme: "socks5",
		Host:   net.JoinHostPort(p.Server, strconv.Itoa(port)),
	}
	if p.Password != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	} else if p.Username != "" {
		u.User = url.User(p.Username)
	}
	if p.Name != "" {
		u.Fragment = p.Name
	}
	return u.String()
}

func buildHTTPProxyURI(p clashProxy) string {
	scheme := strings.ToLower(p.Type)
	if scheme == "http" && p.TLS {
		scheme = "https"
	}
	port := int(p.Port)
	if port == 0 {
		if scheme == "https" {
			port = 443
		} else {
			port = 8080
		}
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(p.Server, strconv.Itoa(port)),
	}
	if p.Password != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	} else if p.Username != "" {
		u.User = url.User(p.Username)
	}
	if p.Name != "" {
		u.Fragment = p.Name
	}
	return u.String()
}

func buildVMessURI(p clashProxy) string {
	params := url.Values{}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("vmess://%s@%s:%d%s#%s", p.UUID, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildVLESSURI(p clashProxy) string {
	params := url.Values{}
	params.Set("encryption", "none")

	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.Flow != "" {
		params.Set("flow", p.Flow)
	}
	if p.PacketEncoding != "" {
		params.Set("packetEncoding", p.PacketEncoding)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.RealityOpts != nil {
		params.Set("security", "reality")
		if p.RealityOpts.PublicKey != "" {
			params.Set("pbk", p.RealityOpts.PublicKey)
		}
		if p.RealityOpts.ShortID != "" {
			params.Set("sid", p.RealityOpts.ShortID)
		}
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.GrpcOpts != nil && p.GrpcOpts.GrpcServiceName != "" {
		params.Set("serviceName", p.GrpcOpts.GrpcServiceName)
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return fmt.Sprintf("vless://%s@%s:%d?%s#%s", p.UUID, p.Server, int(p.Port), params.Encode(), url.QueryEscape(p.Name))
}

func buildTrojanURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("trojan://%s@%s:%d%s#%s", p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildAnyTLSURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("anytls://%s@%s:%d%s#%s", p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildShadowsocksURI(p clashProxy) string {
	// Encode method:password in base64
	userInfo := base64.StdEncoding.EncodeToString([]byte(p.Cipher + ":" + p.Password))
	return fmt.Sprintf("ss://%s@%s:%d#%s", userInfo, p.Server, int(p.Port), url.QueryEscape(p.Name))
}

func buildHysteria2URI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfs-password", p.ObfsPassword)
		}
	}
	ports := strings.TrimSpace(p.Ports)
	if ports == "" {
		ports = strings.TrimSpace(p.MPort)
	}
	if ports != "" {
		params.Set("ports", normalizeHysteria2PortsValue(ports))
	}
	if p.HopInterval != "" {
		params.Set("hop_interval", p.HopInterval)
	}
	if p.UpMbps > 0 {
		params.Set("upMbps", strconv.Itoa(p.UpMbps))
	}
	if p.DownMbps > 0 {
		params.Set("downMbps", strconv.Itoa(p.DownMbps))
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	port := int(p.Port)
	if port <= 0 {
		port = 443
	}

	return fmt.Sprintf("hysteria2://%s@%s:%d%s#%s", p.Password, p.Server, port, query, url.QueryEscape(p.Name))
}

func normalizeHysteria2PortsValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	parts := strings.Split(value, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			normalized = append(normalized, part)
			continue
		}
		if strings.Count(part, "-") == 1 {
			normalized = append(normalized, strings.Replace(part, "-", ":", 1))
			continue
		}
		normalized = append(normalized, part)
	}

	return strings.Join(normalized, ",")
}

func buildTUICURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.CongestionController != "" {
		params.Set("congestion_control", p.CongestionController)
	}
	if p.UDPRelayMode != "" {
		params.Set("udp_relay_mode", p.UDPRelayMode)
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	// TUIC URI format: tuic://uuid:password@server:port?params#name
	return fmt.Sprintf("tuic://%s:%s@%s:%d%s#%s", p.UUID, p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

// buildShadowsocksRURI converts a Clash SSR proxy config to an SSR URI.
// Format: ssr://base64(host:port:protocol:method:obfs:base64(password)/?obfsparam=base64&protoparam=base64&remarks=base64)
func buildShadowsocksRURI(p clashProxy) string {
	passwordB64 := base64.URLEncoding.EncodeToString([]byte(p.Password))

	main := fmt.Sprintf("%s:%d:%s:%s:%s:%s",
		p.Server, int(p.Port),
		defaultStr(p.Protocol, "origin"),
		defaultStr(p.Cipher, "none"),
		defaultStr(p.Obfs, "plain"),
		passwordB64,
	)

	var params []string
	if p.ObfsParam != "" {
		params = append(params, "obfsparam="+base64.URLEncoding.EncodeToString([]byte(p.ObfsParam)))
	}
	if p.ProtocolParam != "" {
		params = append(params, "protoparam="+base64.URLEncoding.EncodeToString([]byte(p.ProtocolParam)))
	}
	if p.Name != "" {
		params = append(params, "remarks="+base64.URLEncoding.EncodeToString([]byte(p.Name)))
	}

	payload := main
	if len(params) > 0 {
		payload += "/?" + strings.Join(params, "&")
	}
	return "ssr://" + base64.URLEncoding.EncodeToString([]byte(payload))
}

// buildHysteriaURI converts a Clash Hysteria v1 proxy config to a hysteria:// URI.
// Format: hysteria://host:port?protocol=udp&auth=xxx&peer=sni&insecure=1&upmbps=N&downmbps=N&alpn=h3&obfs=xplus#name
func buildHysteriaURI(p clashProxy) string {
	params := url.Values{}
	params.Set("protocol", "udp")

	auth := p.AuthStr
	if auth == "" {
		auth = p.Auth
	}
	if auth == "" {
		auth = p.Password
	}
	if auth != "" {
		params.Set("auth", auth)
	}
	if p.ServerName != "" {
		params.Set("peer", p.ServerName)
	} else if p.SNI != "" {
		params.Set("peer", p.SNI)
	} else if p.PeerSNI != "" {
		params.Set("peer", p.PeerSNI)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.UpMbps > 0 {
		params.Set("upmbps", strconv.Itoa(p.UpMbps))
	}
	if p.DownMbps > 0 {
		params.Set("downmbps", strconv.Itoa(p.DownMbps))
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfsParam", p.ObfsPassword)
		}
	}

	return fmt.Sprintf("hysteria://%s:%d?%s#%s", p.Server, int(p.Port), params.Encode(), url.QueryEscape(p.Name))
}

func defaultStr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// FilePath returns the config file path.
func (c *Config) FilePath() string {
	if c == nil {
		return ""
	}
	return c.filePath
}

// SourcesShared reports whether node and subscription definitions are owned by
// the workspace's shared/default config instead of this project file.
func (c *Config) SourcesShared() bool {
	return c != nil && c.sourcesShared
}

// RecoveredStateCatalog reports that subscription nodes came from the last
// successfully started runtime catalog instead of a compatibility cache.
func (c *Config) RecoveredStateCatalog() bool {
	return c != nil && c.recoveredStateCatalog
}

// RecoveredSubscriptionURL reports whether a subscription belonged to the
// committed runtime catalog. It distinguishes a committed empty node set from
// a subscription added after the previous process stopped.
func (c *Config) RecoveredSubscriptionURL(rawURL string) bool {
	if c == nil || !c.recoveredStateCatalog {
		return false
	}
	_, ok := c.recoveredCatalogURLs[rawURL]
	return ok
}

// SetFilePath sets the config file path (used when creating config programmatically).
func (c *Config) SetFilePath(path string) {
	if c != nil {
		c.filePath = path
	}
}

// writeNodesToFile writes nodes to a file (one URI per line) with file locking.
func writeNodesToFile(path string, nodes []NodeConfig) error {
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	// Use file locking for safe concurrent writes
	return writeFileWithLock(path, []byte(content), 0o644)
}

// SaveNodes persists nodes to their appropriate locations based on source.
// - subscription/nodes_file nodes → nodes.txt (or configured nodes_file)
// - inline nodes → config.yaml nodes array
// Config.yaml structure (subscriptions, nodes_file) is preserved.
func (c *Config) SaveNodes() error {
	if c == nil {
		return errors.New("配置不能为空")
	}
	if c.sourcesOnly {
		return c.saveSharedNodes()
	}
	if c.sourcesShared {
		return errors.New("节点源由共享目录管理，请通过全局目录编辑")
	}
	if c.filePath == "" {
		return errors.New("配置文件路径未知")
	}

	// Check if config file is writable before attempting save
	if err := checkFileWritable(c.filePath); err != nil {
		return fmt.Errorf("配置文件不可写: %w（请检查文件权限和 Docker 卷挂载）", err)
	}

	// Separate nodes by source
	var inlineNodes []NodeConfig
	var fileNodes []NodeConfig

	for _, node := range c.Nodes {
		// Create a clean copy without runtime fields for saving
		cleanNode := NodeConfig{
			Name:     node.Name,
			URI:      node.URI,
			Port:     node.Port,
			Username: node.Username,
			Password: node.Password,
		}
		switch node.Source {
		case NodeSourceInline:
			inlineNodes = append(inlineNodes, cleanNode)
		case NodeSourceFile, NodeSourceSubscription:
			fileNodes = append(fileNodes, cleanNode)
		default:
			// Default to file nodes for unknown source
			fileNodes = append(fileNodes, cleanNode)
		}
	}

	// Write file-based nodes to nodes.txt
	if len(fileNodes) > 0 || c.NodesFile != "" {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
		}
		// Check writability before writing
		if err := checkFileWritable(nodesFilePath); err != nil {
			return fmt.Errorf("节点文件不可写: %w（请检查文件权限和 Docker 卷挂载）", err)
		}
		if err := writeNodesToFile(nodesFilePath, fileNodes); err != nil {
			return fmt.Errorf("写入节点文件 %q 失败: %w", nodesFilePath, err)
		}
		log.Printf("✅ 已保存 %d 个节点到 %s", len(fileNodes), nodesFilePath)
	}

	// Update config.yaml nodes array (including clearing it when all inline nodes are deleted)
	{
		// Read original config to preserve structure
		data, err := os.ReadFile(c.filePath)
		if err != nil {
			return fmt.Errorf("读取配置失败: %w", err)
		}
		var saveCfg Config
		if err := yaml.Unmarshal(data, &saveCfg); err != nil {
			return fmt.Errorf("解析配置失败: %w", err)
		}
		// Update only the inline nodes
		saveCfg.Nodes = inlineNodes

		newData, err := yaml.Marshal(&saveCfg)
		if err != nil {
			return fmt.Errorf("编码配置失败: %w", err)
		}
		// Use file locking for safe concurrent writes
		if err := writeFileWithLock(c.filePath, newData, 0o644); err != nil {
			return fmt.Errorf("写入配置失败: %w", err)
		}
		log.Printf("✅ 已保存 %d 个内联节点到 %s", len(inlineNodes), c.filePath)
	}

	return nil
}

// Save is deprecated, use SaveNodes instead.
// This method is kept for backward compatibility but now delegates to SaveNodes.
func (c *Config) Save() error {
	return c.SaveNodes()
}

// SaveSettings persists runtime settings without touching nodes.txt.
func (c *Config) SaveSettings() error {
	if c == nil {
		return errors.New("配置不能为空")
	}
	if c.filePath == "" {
		return errors.New("配置文件路径未知")
	}
	if c.sourcesOnly {
		return c.saveSharedSettings()
	}

	data, err := os.ReadFile(c.filePath)
	if err != nil {
		return fmt.Errorf("读取配置失败: %w", err)
	}
	var saveCfg Config
	if err := yaml.Unmarshal(data, &saveCfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	saveCfg.ExternalIP = c.ExternalIP
	saveCfg.Probe = c.Probe
	saveCfg.SkipCertVerify = c.SkipCertVerify
	saveCfg.Log = c.Log
	if !c.sourcesShared {
		saveCfg.Subscriptions = c.Subscriptions
		saveCfg.DisabledSubscriptions = c.DisabledSubscriptions
	}
	saveCfg.SelectedSubscriptions = c.SelectedSubscriptions
	saveCfg.ExcludedSubscriptions = c.ExcludedSubscriptions
	saveCfg.ExcludedNodes = c.ExcludedNodes
	saveCfg.SubscriptionRefresh = c.SubscriptionRefresh
	saveCfg.Mode = c.Mode
	saveCfg.Listener = c.Listener
	saveCfg.MultiPort = c.MultiPort
	saveCfg.Pool = c.Pool
	saveCfg.Sticky = c.Sticky
	saveCfg.Management = c.Management
	// Deprecated probe fields are accepted on load but new saves use the
	// project-level probe section exclusively.
	saveCfg.Management.ProbeTarget = ""
	saveCfg.Management.ProbeInterval = 0
	saveCfg.Management.ProbeTimeout = 0
	saveCfg.Management.ProbeConcurrency = 0

	newData, err := yaml.Marshal(&saveCfg)
	if err != nil {
		return fmt.Errorf("编码配置失败: %w", err)
	}

	// Use file locking for safe concurrent writes
	if err := writeFileWithLock(c.filePath, newData, 0o644); err != nil {
		return fmt.Errorf("写入配置失败: %w", err)
	}
	return nil
}

type sharedSourcesDocument struct {
	NodesFile             string       `yaml:"nodes_file,omitempty"`
	Subscriptions         []string     `yaml:"subscriptions,omitempty"`
	DisabledSubscriptions []string     `yaml:"disabled_subscriptions,omitempty"`
	Nodes                 []NodeConfig `yaml:"nodes,omitempty"`
}

func (c *Config) loadSharedDocument() (sharedSourcesDocument, error) {
	if c == nil || c.filePath == "" {
		return sharedSourcesDocument{}, errors.New("共享配置文件路径未知")
	}
	data, err := os.ReadFile(c.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return sharedSourcesDocument{}, nil
		}
		return sharedSourcesDocument{}, fmt.Errorf("读取共享配置失败: %w", err)
	}
	var doc sharedSourcesDocument
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return sharedSourcesDocument{}, fmt.Errorf("解析共享配置失败: %w", err)
	}
	return doc, nil
}

func (c *Config) saveSharedDocument(doc sharedSourcesDocument) error {
	if c == nil || c.filePath == "" {
		return errors.New("共享配置文件路径未知")
	}
	if doc.NodesFile == "" && c.NodesFile != "" {
		doc.NodesFile = relativeOrAbsolute(filepath.Dir(c.filePath), c.NodesFile)
	}
	data, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("编码共享配置失败: %w", err)
	}
	if err := writeFileWithLock(c.filePath, data, 0o644); err != nil {
		return fmt.Errorf("写入共享配置失败: %w", err)
	}
	return nil
}

func (c *Config) saveSharedSettings() error {
	doc, err := c.loadSharedDocument()
	if err != nil {
		return err
	}
	doc.Subscriptions = append([]string(nil), c.Subscriptions...)
	doc.DisabledSubscriptions = append([]string(nil), c.DisabledSubscriptions...)
	c.normalizeDisabledSubscriptions()
	doc.DisabledSubscriptions = append([]string(nil), c.DisabledSubscriptions...)
	return c.saveSharedDocument(doc)
}

func (c *Config) saveSharedNodes() error {
	doc, err := c.loadSharedDocument()
	if err != nil {
		return err
	}
	var inlineNodes []NodeConfig
	var fileNodes []NodeConfig
	for _, node := range c.Nodes {
		clean := NodeConfig{
			Name: node.Name, URI: node.URI,
			Source: node.Source, SubscriptionURL: node.SubscriptionURL,
		}
		switch node.Source {
		case NodeSourceInline:
			inlineNodes = append(inlineNodes, clean)
		case NodeSourceFile, NodeSourceSubscription:
			fileNodes = append(fileNodes, clean)
		default:
			inlineNodes = append(inlineNodes, clean)
		}
	}
	if len(fileNodes) > 0 || c.NodesFile != "" || doc.NodesFile != "" {
		nodesPath := c.NodesFile
		if nodesPath == "" {
			nodesPath = doc.NodesFile
			if nodesPath != "" && !filepath.IsAbs(nodesPath) {
				nodesPath = filepath.Join(filepath.Dir(c.filePath), nodesPath)
			}
		}
		if nodesPath == "" {
			nodesPath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
		}
		if err := writeNodesToFile(nodesPath, fileNodes); err != nil {
			return fmt.Errorf("写入共享节点文件失败: %w", err)
		}
		doc.NodesFile = relativeOrAbsolute(filepath.Dir(c.filePath), nodesPath)
	}
	doc.Nodes = inlineNodes
	return c.saveSharedDocument(doc)
}

// IsPortAvailable checks if a port is available for binding.
func IsPortAvailable(address string, port uint16) bool {
	addr := fmt.Sprintf("%s:%d", address, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

// checkFileWritable checks if a file is writable. Creates parent directories if needed.
func checkFileWritable(path string) error {
	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("创建目录 %q 失败: %w", dir, err)
	}

	// Check if file exists
	info, err := os.Stat(path)
	if err == nil {
		// File exists, check if writable
		if info.Mode().Perm()&0o200 == 0 {
			return fmt.Errorf("文件 %q 为只读（权限：%s）", path, info.Mode())
		}
		// Try to open for writing
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return fmt.Errorf("无法打开文件进行写入: %w", err)
		}
		f.Close()
		return nil
	}

	if !os.IsNotExist(err) {
		return fmt.Errorf("检查文件状态失败: %w", err)
	}

	// File doesn't exist, check if directory is writable
	testFile := filepath.Join(dir, ".write_test")
	f, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("目录 %q 不可写: %w", dir, err)
	}
	f.Close()
	os.Remove(testFile)
	return nil
}

// writeFileWithLock writes data to a file with exclusive locking.
func writeFileWithLock(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer f.Close()

	// Acquire exclusive lock
	if err := lockFile(f); err != nil {
		return fmt.Errorf("锁定文件失败: %w", err)
	}
	defer unlockFile(f)

	// Write data
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	// Ensure data is written to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("同步文件失败: %w", err)
	}

	return nil
}
