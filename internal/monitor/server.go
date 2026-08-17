package monitor

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Proxy2API/internal/config"
	"golang.org/x/sync/semaphore"
)

//go:embed assets/index.html
var embeddedFS embed.FS

// Session represents a user session with expiration.
type Session struct {
	Token     string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// NodeManager exposes config node CRUD and reload operations.
type NodeManager interface {
	ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error)
	CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error)
	ImportConfigNodes(ctx context.Context, nodes []config.NodeConfig) ([]config.NodeConfig, int, error)
	UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error)
	DeleteNode(ctx context.Context, name string) error
	TriggerReload(ctx context.Context) error
}

// Sentinel errors for node operations.
var (
	ErrNodeNotFound = errors.New("节点不存在")
	ErrNodeConflict = errors.New("节点名称或端口已存在")
	ErrInvalidNode  = errors.New("无效的节点配置")
)

// SubscriptionRefresher interface for subscription manager.
type SubscriptionRefresher interface {
	RefreshNow() error
	Status() SubscriptionStatus
	Subscriptions() []SubscriptionInfo
	UpdateConfig(urls []string, enabled bool, interval time.Duration)
	UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error
	UpdateConfigAndRefreshSelected(urls []string, enabled bool, interval time.Duration, refreshURLs []string) error
	SetSubscriptionEnabled(rawURL string, enabled bool) error
}

// SubscriptionStatus represents subscription refresh status.
type SubscriptionStatus struct {
	LastRefresh   time.Time `json:"last_refresh"`
	NextRefresh   time.Time `json:"next_refresh"`
	NodeCount     int       `json:"node_count"`
	LastError     string    `json:"last_error,omitempty"`
	RefreshCount  int       `json:"refresh_count"`
	IsRefreshing  bool      `json:"is_refreshing"`
	NodesModified bool      `json:"nodes_modified"` // True if nodes.txt was modified since last refresh
}

// SubscriptionInfo describes the latest state reported by one subscription.
type SubscriptionInfo struct {
	ID             string    `json:"id"`
	URL            string    `json:"url"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	NodeCount      int       `json:"node_count"`
	Included       bool      `json:"included"`
	Enabled        bool      `json:"enabled"`
	UploadBytes    int64     `json:"upload_bytes"`
	DownloadBytes  int64     `json:"download_bytes"`
	UsedBytes      int64     `json:"used_bytes"`
	TotalBytes     int64     `json:"total_bytes"`
	RemainingBytes int64     `json:"remaining_bytes"`
	ExpiresAt      int64     `json:"expires_at"`
	LastRefresh    time.Time `json:"last_refresh"`
	LastError      string    `json:"last_error,omitempty"`
}

// Server exposes HTTP endpoints for monitoring.
type Server struct {
	cfg         Config
	cfgMu       *sync.RWMutex  // 保护动态配置字段；项目视图共享所属项目的锁
	cfgSrc      *config.Config // 可持久化的配置对象
	sharedCfg   *config.Config
	sharedCfgMu *sync.RWMutex
	mgr         *Manager
	srv         *http.Server
	logger      *log.Logger
	projects    ProjectController
	projectID   string
	catalogOnly bool
	logBuffer   *LogBuffer

	// Session management
	sessionMu  sync.RWMutex
	sessions   map[string]*Session
	sessionTTL time.Duration

	// probeAllInFlight bounds batch "probe all" to a single concurrent run.
	// Without it, N simultaneous requests each spin up to `concurrency` probes,
	// multiplying total in-flight dials and starving host fd/memory limits.
	probeAllInFlight *atomic.Bool
	projectProbeMu   sync.Mutex
	projectProbeGate map[string]*atomic.Bool
	projectConfigMu  sync.Mutex
	projectConfigMap map[string]*sync.RWMutex

	subRefresher SubscriptionRefresher
	nodeMgr      NodeManager
}

// NewServer constructs a server; it can be nil when disabled.
func NewServer(cfg Config, mgr *Manager, logger *log.Logger) *Server {
	if !cfg.Enabled {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}

	s := &Server{
		cfg:              cfg,
		cfgMu:            &sync.RWMutex{},
		mgr:              mgr,
		logger:           logger,
		logBuffer:        SharedLogBuffer,
		sessions:         make(map[string]*Session),
		sessionTTL:       24 * time.Hour,
		probeAllInFlight: &atomic.Bool{},
		projectProbeGate: make(map[string]*atomic.Bool),
		projectConfigMap: make(map[string]*sync.RWMutex),
	}

	// Start session cleanup goroutine
	go s.cleanupExpiredSessions()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/auth", s.handleAuth)
	mux.HandleFunc("/api/projects", s.withAuth(s.handleProjects))
	mux.HandleFunc("/api/projects/", s.withAuth(s.handleProjectRoute))
	mux.HandleFunc("/api/system/settings", s.withAuth(s.handleSystemSettings))
	mux.HandleFunc("/api/settings", s.withAuth(s.withDefaultProject((*Server).handleSettings)))
	mux.HandleFunc("/api/nodes", s.withAuth(s.withDefaultProject((*Server).handleNodes)))
	mux.HandleFunc("/api/nodes/online", s.withAuth(s.withDefaultProject((*Server).handleOnlineNodes)))
	mux.HandleFunc("/api/nodes/by-port", s.withAuth(s.withDefaultProject((*Server).handleNodeByPort)))
	mux.HandleFunc("/api/nodes/config", s.withAuth(s.withDefaultProject((*Server).handleConfigNodes)))
	mux.HandleFunc("/api/nodes/config/", s.withAuth(s.withDefaultProject((*Server).handleConfigNodeItem)))
	mux.HandleFunc("/api/nodes/import", s.withAuth(s.withDefaultProject((*Server).handleNodeImport)))
	mux.HandleFunc("/api/nodes/probe-all", s.withAuth(s.withDefaultProject((*Server).handleProbeAll)))
	mux.HandleFunc("/api/nodes/", s.withAuth(s.withDefaultProject((*Server).handleNodeAction)))
	mux.HandleFunc("/api/debug", s.withAuth(s.withDefaultProject((*Server).handleDebug)))
	mux.HandleFunc("/api/export", s.withAuth(s.withDefaultProject((*Server).handleExport)))
	mux.HandleFunc("/api/subscription/status", s.withAuth(s.withDefaultProject((*Server).handleSubscriptionStatus)))
	mux.HandleFunc("/api/subscription/refresh", s.withAuth(s.withDefaultProject((*Server).handleSubscriptionRefresh)))
	mux.HandleFunc("/api/subscription/config", s.withAuth(s.withDefaultProject((*Server).handleSubscriptionConfig)))
	mux.HandleFunc("/api/subscriptions", s.withAuth(s.withDefaultProject((*Server).handleSubscriptions)))
	mux.HandleFunc("/api/subscriptions/settings", s.withAuth(s.withDefaultProject((*Server).handleSubscriptionSettings)))
	mux.HandleFunc("/api/subscriptions/refresh", s.withAuth(s.withDefaultProject((*Server).handleManagedSubscriptionRefresh)))
	mux.HandleFunc("/api/sticky/fixed-node", s.withAuth(s.withDefaultProject((*Server).handleStickyNode)))
	// Keep the old endpoint as a compatibility alias; its behavior now targets
	// the sticky listener instead of the primary pool listener.
	mux.HandleFunc("/api/pool/fixed-node", s.withAuth(s.withDefaultProject((*Server).handleStickyNode)))
	mux.HandleFunc("/api/reload", s.withAuth(s.withDefaultProject((*Server).handleReload)))
	mux.HandleFunc("/api/traffic", s.withAuth(s.withDefaultProject((*Server).handleTraffic)))
	mux.HandleFunc("/api/logs", s.withAuth(s.withDefaultProject((*Server).handleLogs)))
	s.srv = &http.Server{Addr: cfg.Listen, Handler: mux}
	return s
}

type scopedProjectHandler func(*Server, http.ResponseWriter, *http.Request)

// SetProjectController switches the server to multi-project routing. Existing
// top-level API paths continue to resolve through the configured default
// project.
func (s *Server) SetProjectController(controller ProjectController) {
	if s != nil {
		s.projects = controller
	}
}

func (s *Server) withDefaultProject(next scopedProjectHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.projects == nil {
			next(s, w, r)
			return
		}
		projectID := strings.TrimSpace(s.projects.DefaultProjectID())
		if projectID == "" {
			binding, err := s.projects.SharedCatalog()
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(w, map[string]any{"error": err.Error()})
				return
			}
			next(s.scopedProject(binding), w, r)
			return
		}
		binding, err := s.projects.Project(projectID)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		next(s.scopedProject(binding), w, r)
	}
}

func (s *Server) scopedProject(binding ProjectBinding) *Server {
	cfg := s.cfg
	cfg.StateStore = nil
	cfg.ProbeTarget = binding.Config.ProbeTargetOrDefault()
	cfg.ProbeInterval = binding.Config.ProbeIntervalOrDefault()
	cfg.ProbeTimeout = binding.Config.ProbeTimeoutOrDefault()
	cfg.ProbeConcurrency = binding.Config.ProbeConcurrencyOrDefault()
	cfg.ExternalIP = binding.Config.ExternalIP
	cfg.SkipCertVerify = binding.Config.SkipCertVerify
	cfg.StickyNode = binding.Config.Sticky.FixedNode
	cfg.TrafficAPI = fmt.Sprintf("http://127.0.0.1:%d/traffic", binding.Config.ClashAPIPort)
	if binding.Config.Mode == "multi-port" || binding.Config.Mode == "hybrid" {
		cfg.ProxyUsername = binding.Config.MultiPort.Username
		cfg.ProxyPassword = binding.Config.MultiPort.Password
	} else {
		cfg.ProxyUsername = binding.Config.Listener.Username
		cfg.ProxyPassword = binding.Config.Listener.Password
	}
	logBuffer := binding.LogBuffer
	if logBuffer == nil {
		logBuffer = SharedLogBuffer
	}
	return &Server{
		cfg:              cfg,
		cfgMu:            s.configLock(binding.ID),
		cfgSrc:           binding.Config,
		sharedCfg:        binding.SharedConfig,
		sharedCfgMu:      binding.SharedConfigMu,
		mgr:              binding.Monitor,
		logger:           s.logger,
		projects:         s.projects,
		projectID:        binding.ID,
		catalogOnly:      binding.CatalogOnly,
		logBuffer:        logBuffer,
		probeAllInFlight: s.probeGate(binding.ID),
		subRefresher:     binding.SubscriptionRefresher,
		nodeMgr:          binding.NodeManager,
	}
}

func (s *Server) ensureSharedSourceOwner(w http.ResponseWriter) bool {
	if s.projects == nil || s.sharedCfg != nil {
		return true
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	writeJSON(w, map[string]any{"error": "共享节点和订阅配置未初始化"})
	return false
}

func (s *Server) sharedSourceConfig() *config.Config {
	if s.sharedCfg != nil {
		return s.sharedCfg
	}
	return s.cfgSrc
}

func (s *Server) sharedSourceLock() *sync.RWMutex {
	if s.sharedCfgMu != nil {
		return s.sharedCfgMu
	}
	return s.cfgMu
}

func (s *Server) reloadSharedSources(ctx context.Context, response map[string]any) {
	if s.projects == nil {
		return
	}
	if err := s.projects.ReloadSharedSources(ctx); err != nil {
		response["shared_reload_error"] = err.Error()
		return
	}
	response["shared_reloaded"] = true
}

func (s *Server) configLock(projectID string) *sync.RWMutex {
	s.projectConfigMu.Lock()
	defer s.projectConfigMu.Unlock()
	if lock := s.projectConfigMap[projectID]; lock != nil {
		return lock
	}
	lock := &sync.RWMutex{}
	s.projectConfigMap[projectID] = lock
	return lock
}

func (s *Server) probeGate(projectID string) *atomic.Bool {
	s.projectProbeMu.Lock()
	defer s.projectProbeMu.Unlock()
	if gate := s.projectProbeGate[projectID]; gate != nil {
		return gate
	}
	gate := &atomic.Bool{}
	s.projectProbeGate[projectID] = gate
	return gate
}

func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "project management is not enabled"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, map[string]any{
			"default_project": s.projects.DefaultProjectID(),
			"items":           s.projects.ListProjects(),
			"port_hints":      s.projects.ProjectPortHints(),
		})
	case http.MethodPost:
		var request ProjectCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "invalid request body"})
			return
		}
		created, err := s.projects.CreateProject(r.Context(), request)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, created)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSystemSettings(w http.ResponseWriter, r *http.Request) {
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "project management is not enabled"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		settings := s.projects.SystemSettings()
		writeJSON(w, map[string]any{
			"management": map[string]any{
				"enabled":  settings.Management.Enabled,
				"listen":   settings.Management.Listen,
				"password": settings.Management.Password,
			},
			"log": settings.Log,
		})
	case http.MethodPut:
		var settings SystemSettings
		if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "invalid request body"})
			return
		}
		if err := s.projects.UpdateSystemSettings(r.Context(), settings); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "system settings saved", "need_restart": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleProjectRoute(w http.ResponseWriter, r *http.Request) {
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "project management is not enabled"})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	parts := strings.SplitN(path, "/", 2)
	projectID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(projectID) == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid project id"})
		return
	}
	if len(parts) == 1 || parts[1] == "" {
		s.handleProjectItem(w, r, projectID)
		return
	}
	actionOrAPI := parts[1]
	if actionOrAPI == "start" || actionOrAPI == "stop" || actionOrAPI == "reload" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var actionErr error
		switch actionOrAPI {
		case "start":
			actionErr = s.projects.StartProject(r.Context(), projectID)
		case "stop":
			actionErr = s.projects.StopProject(r.Context(), projectID)
		case "reload":
			actionErr = s.projects.ReloadProject(r.Context(), projectID)
		}
		if actionErr != nil {
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{"error": actionErr.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": actionOrAPI + " completed"})
		return
	}

	binding, err := s.projects.Project(projectID)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	scoped := s.scopedProject(binding)
	request := r.Clone(r.Context())
	clonedURL := *r.URL
	clonedURL.Path = "/api/" + actionOrAPI
	clonedURL.RawPath = ""
	request.URL = &clonedURL
	runtimeProjectMux(scoped).ServeHTTP(w, request)
}

func (s *Server) handleProjectItem(w http.ResponseWriter, r *http.Request, projectID string) {
	switch r.Method {
	case http.MethodGet:
		for _, project := range s.projects.ListProjects() {
			if project.ID == projectID {
				writeJSON(w, project)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]any{"error": "project not found"})
	case http.MethodPatch:
		var request ProjectUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "invalid request body"})
			return
		}
		updated, err := s.projects.UpdateProject(r.Context(), projectID, request)
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, updated)
	case http.MethodDelete:
		deleteData := false
		if raw := strings.TrimSpace(r.URL.Query().Get("delete_data")); raw != "" {
			var err error
			deleteData, err = strconv.ParseBool(raw)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": "delete_data must be true or false"})
				return
			}
		}
		result, err := s.projects.DeleteProjectWithData(r.Context(), projectID, deleteData)
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, result)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func runtimeProjectMux(s *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/nodes", s.handleNodes)
	mux.HandleFunc("/api/nodes/online", s.handleOnlineNodes)
	mux.HandleFunc("/api/nodes/by-port", s.handleNodeByPort)
	mux.HandleFunc("/api/nodes/config", s.handleConfigNodes)
	mux.HandleFunc("/api/nodes/config/", s.handleConfigNodeItem)
	mux.HandleFunc("/api/nodes/import", s.handleNodeImport)
	mux.HandleFunc("/api/nodes/probe-all", s.handleProbeAll)
	mux.HandleFunc("/api/nodes/", s.handleNodeAction)
	mux.HandleFunc("/api/debug", s.handleDebug)
	mux.HandleFunc("/api/export", s.handleExport)
	mux.HandleFunc("/api/subscription/status", s.handleSubscriptionStatus)
	mux.HandleFunc("/api/subscription/refresh", s.handleSubscriptionRefresh)
	mux.HandleFunc("/api/subscription/config", s.handleSubscriptionConfig)
	mux.HandleFunc("/api/subscriptions", s.handleSubscriptions)
	mux.HandleFunc("/api/subscriptions/settings", s.handleSubscriptionSettings)
	mux.HandleFunc("/api/subscriptions/refresh", s.handleManagedSubscriptionRefresh)
	mux.HandleFunc("/api/sticky/fixed-node", s.handleStickyNode)
	mux.HandleFunc("/api/pool/fixed-node", s.handleStickyNode)
	mux.HandleFunc("/api/reload", s.handleReload)
	mux.HandleFunc("/api/traffic", s.handleTraffic)
	mux.HandleFunc("/api/logs", s.handleLogs)
	return mux
}

// SetSubscriptionRefresher sets the subscription refresher for API endpoints.
func (s *Server) SetSubscriptionRefresher(sr SubscriptionRefresher) {
	if s != nil {
		s.subRefresher = sr
	}
}

// SetNodeManager enables config-node CRUD endpoints.
func (s *Server) SetNodeManager(nm NodeManager) {
	if s != nil {
		s.nodeMgr = nm
	}
}

// SetConfig binds the persistable config object for settings API.
func (s *Server) SetConfig(cfg *config.Config) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	// Preserve subscription config from previous cfgSrc if new config has none
	if cfg != nil && s.cfgSrc != nil {
		if len(cfg.Subscriptions) == 0 && len(s.cfgSrc.Subscriptions) > 0 {
			cfg.Subscriptions = s.cfgSrc.Subscriptions
		}
		if cfg.SubscriptionRefresh.Interval == 0 && s.cfgSrc.SubscriptionRefresh.Interval > 0 {
			cfg.SubscriptionRefresh = s.cfgSrc.SubscriptionRefresh
		}
	}
	s.cfgSrc = cfg
	if cfg != nil {
		s.cfg.ExternalIP = cfg.ExternalIP
		s.cfg.ProbeTarget = cfg.ProbeTargetOrDefault()
		s.cfg.ProbeInterval = cfg.ProbeIntervalOrDefault()
		s.cfg.ProbeTimeout = cfg.ProbeTimeoutOrDefault()
		s.cfg.SkipCertVerify = cfg.SkipCertVerify
		// Sync probe concurrency to the manager so periodic health checks pick
		// up WebUI changes after a reload (batch probes read it per request).
		if s.mgr != nil {
			s.mgr.SetStickyNode(cfg.Sticky.FixedNode)
			s.mgr.SetProbeConcurrency(cfg.ProbeConcurrencyOrDefault())
			s.mgr.SetProbeSchedule(cfg.ProbeIntervalOrDefault(), cfg.ProbeTimeoutOrDefault())
			// Re-derive the probe destination and strict-TLS mode so changes to
			// probe_target / skip_cert_verify take effect on the long-lived
			// manager without a full process restart.
			s.mgr.SetProbeTarget(cfg.ProbeTargetOrDefault(), cfg.SkipCertVerify)
		}
		// Sync proxy credentials based on mode
		if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
			s.cfg.ProxyUsername = cfg.MultiPort.Username
			s.cfg.ProxyPassword = cfg.MultiPort.Password
		} else {
			s.cfg.ProxyUsername = cfg.Listener.Username
			s.cfg.ProxyPassword = cfg.Listener.Password
		}
	}
}

// getSettings returns current dynamic settings (thread-safe).
func (s *Server) getSettings() (externalIP, probeTarget string, skipCertVerify bool, logCfg config.LogConfig) {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	logCfg = config.LogConfig{}
	if s.cfgSrc != nil {
		logCfg = s.cfgSrc.Log
	}
	return s.cfg.ExternalIP, s.cfg.ProbeTarget, s.cfg.SkipCertVerify, logCfg
}

// currentProbeConcurrency returns the probe concurrency from the live config,
// clamped to a safe range. Read per batch-probe request so WebUI changes apply
// after a reload without restarting the process.
func (s *Server) currentProbeConcurrency() int64 {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfgSrc != nil {
		return int64(s.cfgSrc.ProbeConcurrencyOrDefault())
	}
	return 32
}

func (s *Server) currentProbeTimeout() time.Duration {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfgSrc != nil {
		return s.cfgSrc.ProbeTimeoutOrDefault()
	}
	if s.cfg.ProbeTimeout > 0 {
		return s.cfg.ProbeTimeout
	}
	return 110 * time.Second
}

// updateSettings updates the in-memory dynamic settings. The settings handler
// persists once after applying the extended fields from the same request.
func (s *Server) updateSettings(externalIP, probeTarget string, skipCertVerify bool, logCfg *config.LogConfig) error {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	s.cfg.ExternalIP = externalIP
	s.cfg.ProbeTarget = probeTarget
	s.cfg.SkipCertVerify = skipCertVerify

	if s.cfgSrc == nil {
		return errors.New("配置存储未初始化")
	}

	s.cfgSrc.ExternalIP = externalIP
	s.cfgSrc.Probe.Target = probeTarget
	s.cfgSrc.SkipCertVerify = skipCertVerify

	if logCfg != nil {
		s.cfgSrc.Log.Output = logCfg.Output
		if logCfg.MaxSize > 0 {
			s.cfgSrc.Log.MaxSize = logCfg.MaxSize
		}
		if logCfg.MaxBackups > 0 {
			s.cfgSrc.Log.MaxBackups = logCfg.MaxBackups
		}
		if logCfg.MaxAge > 0 {
			s.cfgSrc.Log.MaxAge = logCfg.MaxAge
		}
		s.cfgSrc.Log.Compress = logCfg.Compress
	}

	return nil
}

// Start launches the HTTP server.
func (s *Server) Start(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	s.logger.Printf("Starting monitor server on %s", s.cfg.Listen)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Printf("❌ Monitor server error: %v", err)
		}
	}()
	// Give server a moment to start and check for immediate errors
	time.Sleep(100 * time.Millisecond)
	s.logger.Printf("✅ Monitor server started on http://%s", s.cfg.Listen)

	go func() {
		<-ctx.Done()
		s.Shutdown(context.Background())
	}()
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	_ = s.srv.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// 返回全量节点，前端据此按状态统计（健康/拉黑/异常）并展示可解封的拉黑节点。
	// 之前只返回 SnapshotFiltered(true)，导致 dashboard 的拉黑/异常计数恒为 0，
	// 且拉黑节点不出现在表格里、无法解封。
	allNodes := s.mgr.SnapshotVisible()
	totalNodes := len(allNodes)

	sweepActive, sweepDone, sweepTotal, sweepOK, sweepFail := s.mgr.ProbeSweepProgress()
	s.cfgMu.RLock()
	stickyEnabled := s.cfgSrc != nil && s.cfgSrc.Sticky.Enabled
	s.cfgMu.RUnlock()
	payload := map[string]any{
		"nodes":          allNodes,
		"total_nodes":    totalNodes,
		"sticky_node":    s.mgr.StickyNode(),
		"sticky_enabled": stickyEnabled,
		"entry_exits":    s.mgr.EntryExits(),
		"probe_sweep": map[string]any{
			"active":    sweepActive,
			"done":      sweepDone,
			"total":     sweepTotal,
			"available": sweepOK,
			"failed":    sweepFail,
		},
	}
	writeJSON(w, payload)
}

// handleOnlineNodes returns a compact, automation-friendly list of nodes that
// are currently verified online and eligible for routing.
func (s *Server) handleOnlineNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodes := make([]map[string]any, 0)
	for _, snap := range s.mgr.SnapshotFiltered(true) {
		nodes = append(nodes, map[string]any{
			"tag":                snap.Tag,
			"name":               snap.Name,
			"mode":               snap.Mode,
			"listen_address":     snap.ListenAddress,
			"port":               snap.Port,
			"latency_ms":         snap.LastLatencyMs,
			"ip":                 snap.IP,
			"region":             snap.Region,
			"country":            snap.Country,
			"active_connections": snap.ActiveConnections,
		})
	}
	writeJSON(w, map[string]any{
		"count":       len(nodes),
		"sticky_node": s.mgr.StickyNode(),
		"entry_exits": s.mgr.EntryExits(),
		"nodes":       nodes,
	})
}

// handleNodeByPort resolves the node currently associated with a proxy entry
// port. Shared pool entries use the most recent successful exit recorded by
// the pool; multi-port entries resolve directly from the node's assigned port.
func (s *Server) handleNodeByPort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	port, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("port")))
	if err != nil || port < 1 || port > 65535 {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "port must be an integer between 1 and 65535"})
		return
	}

	wantedPort := uint16(port)
	snapshots := s.mgr.SnapshotVisible()
	entryExits := s.mgr.EntryExits()

	// Shared pool and sticky entries record the most recent successful exit.
	if tag, ok := entryExits[wantedPort]; ok {
		for _, snap := range snapshots {
			if snap.Tag == tag {
				writeJSON(w, map[string]any{
					"port":        wantedPort,
					"tag":         snap.Tag,
					"source":      "entry_exit",
					"node":        snap,
					"entry_exits": entryExits,
				})
				return
			}
		}
	}

	// In multi-port or hybrid mode each node owns its assigned port. Do not
	// apply this fallback to pool mode, where every node reports the shared
	// listener port but no node is current until a request succeeds.
	for _, snap := range snapshots {
		if (snap.Mode == "multi-port" || snap.Mode == "hybrid") && snap.Port == wantedPort {
			writeJSON(w, map[string]any{
				"port":        wantedPort,
				"tag":         snap.Tag,
				"source":      "node_port",
				"node":        snap,
				"entry_exits": entryExits,
			})
			return
		}
	}

	// A shared port may not have served a successful request yet. Treat that
	// state as an empty result rather than an HTTP error for polling clients.
	writeJSON(w, map[string]any{})
}

func (s *Server) handleStickyNode(w http.ResponseWriter, r *http.Request) {
	if s.catalogOnly && r.Method != http.MethodGet {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": "无项目时不能修改节点运行状态"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, map[string]any{"tag": s.mgr.StickyNode()})
	case http.MethodPut, http.MethodDelete:
		tag := ""
		if r.Method == http.MethodPut {
			var req struct {
				Tag string `json:"tag"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": "请求格式错误"})
				return
			}
			tag = strings.TrimSpace(req.Tag)
		}
		if tag != "" {
			found := false
			for _, snap := range s.mgr.SnapshotVisible() {
				if snap.Tag == tag {
					found = true
					break
				}
			}
			if !found {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(w, map[string]any{"error": "节点不存在或属于已关闭订阅"})
				return
			}
		}

		s.cfgMu.Lock()
		if s.cfgSrc == nil {
			s.cfgMu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSON(w, map[string]any{"error": "配置管理未启用"})
			return
		}
		s.cfgSrc.Sticky.FixedNode = tag
		s.cfgSrc.Pool.FixedNode = ""
		if err := s.cfgSrc.SaveSettings(); err != nil {
			s.cfgMu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": fmt.Sprintf("保存固定节点失败: %v", err)})
			return
		}
		s.cfgMu.Unlock()
		s.mgr.SetStickyNode(tag)
		writeJSON(w, map[string]any{"tag": tag, "message": "固定出口已更新"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDebug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snapshots := s.mgr.SnapshotVisible()
	var totalCalls, totalSuccess int64
	debugNodes := make([]map[string]any, 0, len(snapshots))
	for _, snap := range snapshots {
		totalCalls += snap.SuccessCount + int64(snap.FailureCount)
		totalSuccess += snap.SuccessCount
		debugNodes = append(debugNodes, map[string]any{
			"tag":                snap.Tag,
			"name":               snap.Name,
			"mode":               snap.Mode,
			"port":               snap.Port,
			"ip":                 snap.IP,
			"region":             snap.Region,
			"country":            snap.Country,
			"failure_count":      snap.FailureCount,
			"success_count":      snap.SuccessCount,
			"active_connections": snap.ActiveConnections,
			"last_latency_ms":    snap.LastLatencyMs,
			"last_success":       snap.LastSuccess,
			"last_failure":       snap.LastFailure,
			"last_error":         snap.LastError,
			"blacklisted":        snap.Blacklisted,
			"timeline":           snap.Timeline,
		})
	}
	var successRate float64
	if totalCalls > 0 {
		successRate = float64(totalSuccess) / float64(totalCalls) * 100
	}
	writeJSON(w, map[string]any{
		"nodes":         debugNodes,
		"total_calls":   totalCalls,
		"total_success": totalSuccess,
		"success_rate":  successRate,
	})
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tag := parts[0]
	if tag == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	if s.catalogOnly && action != "probe" {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": "无项目时只允许探测节点，不能修改节点运行状态"})
		return
	}
	switch action {
	case "probe":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.currentProbeTimeout())
		defer cancel()
		result, err := s.mgr.ProbeWithResult(ctx, tag)
		latencyMs := int64(-1)
		if result.ConnectivityOK {
			latencyMs = result.Latency.Milliseconds()
		}
		if latencyMs == 0 && result.Latency > 0 {
			latencyMs = 1 // Round up sub-millisecond latencies to 1ms
		}
		payload := map[string]any{
			"message":            "探测成功",
			"healthy":            err == nil,
			"latency_ms":         latencyMs,
			"connectivity_ok":    result.ConnectivityOK,
			"connectivity_error": result.ConnectivityError,
			"trace_ok":           result.TraceOK,
			"trace_ip":           result.IP,
			"trace_region":       result.Region,
			"trace_country":      result.Country,
			"trace_error":        result.TraceError,
			"trace_attempts":     result.TraceAttempts,
		}
		if err != nil {
			payload["message"] = "探测未通过"
			payload["error"] = err.Error()
			w.WriteHeader(http.StatusBadRequest)
		}
		writeJSON(w, payload)
	case "release":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := s.mgr.Release(tag); err != nil {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"tag": tag, "message": "已解除拉黑"})
	case "blacklist":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// Prefer the configured pool.blacklist_duration over a hardcoded 24h default.
		defaultDuration := 24 * time.Hour
		s.cfgMu.RLock()
		if s.cfgSrc != nil && s.cfgSrc.Pool.BlacklistDuration > 0 {
			defaultDuration = s.cfgSrc.Pool.BlacklistDuration
		}
		s.cfgMu.RUnlock()
		var req struct {
			Duration string `json:"duration"` // e.g. "1h", "24h", "30m"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		if req.Duration == "" {
			req.Duration = defaultDuration.String()
		}
		duration, err := time.ParseDuration(req.Duration)
		if err != nil || duration <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "拉黑时长格式无效"})
			return
		}
		if err := s.mgr.ManualBlacklist(tag, duration); err != nil {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"tag": tag, "duration": duration.String(), "message": fmt.Sprintf("已拉黑 %s", duration)})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleProbeAll probes all nodes in batches and returns results via SSE
func (s *Server) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Only one batch probe-all may run at a time. A second concurrent request
	// would multiply total in-flight probes (each request bounds itself, but
	// not the others), so reject it cleanly instead.
	if !s.probeAllInFlight.CompareAndSwap(false, true) {
		busy, _ := json.Marshal(map[string]any{"type": "error", "message": "批量探测已在进行中，请稍候"})
		fmt.Fprintf(w, "data: %s\n\n", busy)
		flusher.Flush()
		return
	}
	defer s.probeAllInFlight.Store(false)

	// Get all nodes
	snapshots := s.mgr.SnapshotVisible()
	total := len(snapshots)
	if total == 0 {
		emptyData, _ := json.Marshal(map[string]any{"type": "complete", "total": 0, "success": 0, "failed": 0})
		fmt.Fprintf(w, "data: %s\n\n", emptyData)
		flusher.Flush()
		return
	}

	// Send start event
	startData, _ := json.Marshal(map[string]any{"type": "start", "total": total})
	fmt.Fprintf(w, "data: %s\n\n", startData)
	flusher.Flush()

	// Read concurrency from the live config so WebUI changes take effect after
	// a reload (no process restart required). A fresh semaphore is created per
	// request to avoid mutating a shared one while probes are in flight.
	concurrency := s.currentProbeConcurrency()
	sem := semaphore.NewWeighted(concurrency)

	// Create context with a timeout scaled to node count and concurrency so that
	// large inventories (e.g. thousands of nodes) are not cut off. Each probe
	// still has its own configured deadline; here we bound the total wall time as
	// ceil(total / concurrency) * perProbe + slack, which is the expected
	// completion time given the semaphore. We deliberately do NOT cap this below
	// the estimate: a shorter deadline would cancel still-queued probes and
	// report reachable nodes as failures (which can then blacklist them). The
	// client can still abort early by closing the SSE connection (r.Context()).
	perProbe := s.currentProbeTimeout()
	batches := (int64(total) + concurrency - 1) / concurrency
	totalTimeout := time.Duration(batches)*perProbe + 30*time.Second
	if totalTimeout < 2*time.Minute {
		totalTimeout = 2 * time.Minute
	}
	if totalTimeout > 30*time.Minute && s.logger != nil {
		s.logger.Printf("⚠️  批量探测节点数较多(%d, 并发%d)，预计最长耗时约 %s", total, concurrency, totalTimeout.Round(time.Second))
	}
	ctx, cancel := context.WithTimeout(r.Context(), totalTimeout)
	defer cancel()

	// Probe all nodes with semaphore control
	type probeResult struct {
		tag             string
		name            string
		latency         int64
		err             string
		connectivityErr string
		traceErr        string
		traceAttempts   int
	}
	results := make(chan probeResult, total)
	var wg sync.WaitGroup

	// Launch probes with semaphore control
	for _, snap := range snapshots {
		wg.Add(1)
		go func(snap Snapshot) {
			defer wg.Done()

			// Acquire semaphore permit
			if err := sem.Acquire(ctx, 1); err != nil {
				results <- probeResult{
					tag:  snap.Tag,
					name: snap.Name,
					err:  "probe cancelled: " + err.Error(),
				}
				return
			}
			defer sem.Release(1)

			// Execute probe
			probeCtx, probeCancel := context.WithTimeout(ctx, perProbe)
			defer probeCancel()

			probe, err := s.mgr.ProbeWithResult(probeCtx, snap.Tag)
			latency := int64(-1)
			if probe.ConnectivityOK {
				latency = probe.Latency.Milliseconds()
				if latency == 0 && probe.Latency > 0 {
					latency = 1
				}
			}
			if err != nil {
				results <- probeResult{
					tag:             snap.Tag,
					name:            snap.Name,
					latency:         latency,
					err:             err.Error(),
					connectivityErr: probe.ConnectivityError,
					traceErr:        probe.TraceError,
					traceAttempts:   probe.TraceAttempts,
				}
			} else {
				results <- probeResult{
					tag:             snap.Tag,
					name:            snap.Name,
					latency:         latency,
					err:             "",
					connectivityErr: probe.ConnectivityError,
					traceErr:        probe.TraceError,
					traceAttempts:   probe.TraceAttempts,
				}
			}
		}(snap)
	}

	// Wait for all probes to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	successCount := 0
	failedCount := 0
	traceSuccessCount := 0
	traceFailedCount := 0
	count := 0

	for result := range results {
		count++
		if result.err != "" {
			failedCount++
		} else {
			successCount++
		}
		if result.traceAttempts > 0 {
			if result.traceErr != "" {
				traceFailedCount++
			} else {
				traceSuccessCount++
			}
		}

		status := "success"
		if result.err != "" {
			status = "error"
		}

		eventPayload := map[string]any{
			"type":               "progress",
			"tag":                result.tag,
			"name":               result.name,
			"latency":            result.latency,
			"status":             status,
			"error":              result.err,
			"connectivity_error": result.connectivityErr,
			"trace_error":        result.traceErr,
			"trace_attempts":     result.traceAttempts,
			"current":            count,
			"total":              total,
			"progress":           float64(count) / float64(total) * 100,
		}
		eventData, _ := json.Marshal(eventPayload)
		fmt.Fprintf(w, "data: %s\n\n", eventData)
		flusher.Flush()
	}

	// Send complete event
	completeData, _ := json.Marshal(map[string]any{
		"type":          "complete",
		"total":         total,
		"success":       successCount,
		"failed":        failedCount,
		"trace_success": traceSuccessCount,
		"trace_failed":  traceFailedCount,
	})
	fmt.Fprintf(w, "data: %s\n\n", completeData)
	flusher.Flush()
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// withAuth 认证中间件，如果配置了密码则需要验证
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 如果没有配置密码，直接放行
		if s.cfg.Password == "" {
			next(w, r)
			return
		}

		// 检查 Cookie 中的 session token
		cookie, err := r.Cookie("session_token")
		if err == nil && s.validateSession(cookie.Value) {
			next(w, r)
			return
		}

		// 检查 Authorization header (Bearer token)
		authHeader := r.Header.Get("Authorization")
		if authHeader != "" {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if s.validateSession(token) {
				next(w, r)
				return
			}
		}

		// 未授权
		w.WriteHeader(http.StatusUnauthorized)
		writeJSON(w, map[string]any{"error": "未授权，请先登录"})
	}
}

// handleAuth 处理登录认证
func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	// 如果没有配置密码，直接返回成功（不需要token）
	if s.cfg.Password == "" {
		writeJSON(w, map[string]any{"message": "无需密码", "no_password": true})
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return
	}

	// 使用 constant-time 比较防止时序攻击
	if !secureCompareStrings(req.Password, s.cfg.Password) {
		// 添加随机延迟防止暴力破解
		time.Sleep(time.Duration(100+mathrand.Intn(200)) * time.Millisecond)
		w.WriteHeader(http.StatusUnauthorized)
		writeJSON(w, map[string]any{"error": "密码错误"})
		return
	}

	// 创建新会话
	session, err := s.createSession()
	if err != nil {
		s.logger.Printf("Failed to create session: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": "服务器错误"})
		return
	}

	// 设置 HttpOnly Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    session.Token,
		Path:     "/",
		HttpOnly: true,
		Secure:   false, // 生产环境应启用 HTTPS 并设为 true
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(s.sessionTTL.Seconds()),
	})

	writeJSON(w, map[string]any{
		"message": "登录成功",
		"token":   session.Token,
	})
}

// handleExport 导出所有可用代理池节点的代理 URI，每行一个。
// query 参数:
//   - scheme=http   (默认)
//   - scheme=socks5
//   - scheme=all    (同时导出 HTTP 和 SOCKS5)
//
// 在 pool/hybrid 模式下，还会导出 Pool 代理池入口。
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	scheme := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("scheme")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "socks5" && scheme != "all" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid scheme, use http/socks5/all"})
		return
	}

	// all=true 时导出全部节点（不论死活）；否则只导出初始检查通过的可用节点。
	includeAll := false
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("all"))) {
	case "1", "true", "yes":
		includeAll = true
	}
	var snapshots []Snapshot
	if includeAll {
		snapshots = s.mgr.SnapshotVisible()
	} else {
		snapshots = s.mgr.SnapshotFiltered(true)
	}
	var lines []string

	seen := make(map[string]bool)

	// 读取运行模式和监听配置
	s.cfgMu.RLock()
	mode := ""
	var listenerCfg config.ListenerConfig
	if s.cfgSrc != nil {
		mode = s.cfgSrc.Mode
		listenerCfg = s.cfgSrc.Listener
	}
	s.cfgMu.RUnlock()

	// Pool 代理池入口（pool 或 hybrid 模式）
	if (mode == "pool" || mode == "hybrid") && listenerCfg.Port > 0 {
		poolAddr := listenerCfg.Address
		if poolAddr == "" || poolAddr == "0.0.0.0" || poolAddr == "::" {
			if extIP, _, _, _ := s.getSettings(); extIP != "" {
				poolAddr = extIP
			}
		}
		var poolAuth string
		if listenerCfg.Username != "" && listenerCfg.Password != "" {
			poolAuth = fmt.Sprintf("%s:%s@", listenerCfg.Username, listenerCfg.Password)
		}
		lines = append(lines, "# Pool 代理池入口")
		poolHTTP := fmt.Sprintf("http://%s%s:%d", poolAuth, poolAddr, listenerCfg.Port)
		poolSocks := fmt.Sprintf("socks5://%s%s:%d", poolAuth, poolAddr, listenerCfg.Port)
		switch scheme {
		case "http":
			lines = append(lines, poolHTTP)
			seen[poolHTTP] = true
		case "socks5":
			lines = append(lines, poolSocks)
			seen[poolSocks] = true
		case "all":
			lines = append(lines, poolHTTP)
			seen[poolHTTP] = true
			lines = append(lines, poolSocks)
			seen[poolSocks] = true
		}
	}

	// Multi-port 独立节点
	if len(snapshots) > 0 && (mode == "hybrid" || mode == "multi-port" || mode == "") {
		lines = append(lines, "# Multi-port 独立节点")
	}
	for _, snap := range snapshots {
		// 只导出有监听地址和端口的节点
		if snap.ListenAddress == "" || snap.Port == 0 {
			continue
		}

		listenAddr := snap.ListenAddress
		if listenAddr == "0.0.0.0" || listenAddr == "::" {
			if extIP, _, _, _ := s.getSettings(); extIP != "" {
				listenAddr = extIP
			}
		}

		var authPart string
		if s.cfg.ProxyUsername != "" && s.cfg.ProxyPassword != "" {
			authPart = fmt.Sprintf("%s:%s@", s.cfg.ProxyUsername, s.cfg.ProxyPassword)
		}
		httpURI := fmt.Sprintf("http://%s%s:%d", authPart, listenAddr, snap.Port)
		socksURI := fmt.Sprintf("socks5://%s%s:%d", authPart, listenAddr, snap.Port)

		switch scheme {
		case "http":
			if !seen[httpURI] {
				lines = append(lines, httpURI)
				seen[httpURI] = true
			}
		case "socks5":
			if !seen[socksURI] {
				lines = append(lines, socksURI)
				seen[socksURI] = true
			}
		case "all":
			if !seen[httpURI] {
				lines = append(lines, httpURI)
				seen[httpURI] = true
			}
			if !seen[socksURI] {
				lines = append(lines, socksURI)
				seen[socksURI] = true
			}
		}
	}

	// 返回纯文本，每行一个 URI
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	filename := "proxy_pool.txt"
	if scheme == "socks5" {
		filename = "proxy_pool_socks5.txt"
	} else if scheme == "all" {
		filename = "proxy_pool_all.txt"
	}
	if includeAll {
		filename = "full_" + filename
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(strings.Join(lines, "\n")))
}

// handleSettings handles GET/PUT for dynamic settings (external_ip, probe_target, skip_cert_verify, log).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		extIP, probeTarget, skipCertVerify, logCfg := s.getSettings()

		// Read full config for extended fields
		s.cfgMu.RLock()
		cfg := s.cfgSrc
		s.cfgMu.RUnlock()

		resp := map[string]any{
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": skipCertVerify,
			"log": map[string]any{
				"output":      logCfg.Output,
				"file":        logCfg.File,
				"max_size":    logCfg.MaxSize,
				"max_backups": logCfg.MaxBackups,
				"max_age":     logCfg.MaxAge,
				"compress":    logCfg.Compress,
			},
		}
		if cfg != nil {
			resp["mode"] = cfg.Mode
			resp["listener"] = map[string]any{
				"address":  cfg.Listener.Address,
				"port":     cfg.Listener.Port,
				"username": cfg.Listener.Username,
				"password": cfg.Listener.Password,
			}
			resp["multi_port"] = map[string]any{
				"address":   cfg.MultiPort.Address,
				"base_port": cfg.MultiPort.BasePort,
				"username":  cfg.MultiPort.Username,
				"password":  cfg.MultiPort.Password,
			}
			resp["pool"] = map[string]any{
				"mode":               cfg.Pool.Mode,
				"failure_threshold":  cfg.Pool.FailureThreshold,
				"blacklist_duration": cfg.Pool.BlacklistDuration.String(),
			}
			resp["sticky"] = map[string]any{
				"enabled":    cfg.Sticky.Enabled,
				"port":       cfg.Sticky.Port,
				"fixed_node": cfg.Sticky.FixedNode,
			}
			resp["management"] = map[string]any{
				"listen":            cfg.Management.Listen,
				"password":          cfg.Management.Password,
				"probe_concurrency": cfg.ProbeConcurrencyOrDefault(),
				"probe_interval":    cfg.ProbeIntervalOrDefault().String(),
				"probe_timeout":     cfg.ProbeTimeoutOrDefault().String(),
			}
		}
		writeJSON(w, resp)
	case http.MethodPut:
		var req struct {
			ExternalIP     string `json:"external_ip"`
			ProbeTarget    string `json:"probe_target"`
			SkipCertVerify bool   `json:"skip_cert_verify"`
			Mode           string `json:"mode,omitempty"`
			Listener       *struct {
				Address  string `json:"address"`
				Port     uint16 `json:"port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"listener,omitempty"`
			MultiPort *struct {
				Address  string `json:"address"`
				BasePort uint16 `json:"base_port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"multi_port,omitempty"`
			Pool *struct {
				Mode              string `json:"mode"`
				FailureThreshold  int    `json:"failure_threshold"`
				BlacklistDuration string `json:"blacklist_duration"`
			} `json:"pool,omitempty"`
			Sticky *struct {
				Enabled bool   `json:"enabled"`
				Port    uint16 `json:"port"`
			} `json:"sticky,omitempty"`
			Management *struct {
				Listen           string `json:"listen"`
				Password         string `json:"password"`
				ProbeConcurrency int    `json:"probe_concurrency"`
				ProbeInterval    string `json:"probe_interval"`
				ProbeTimeout     string `json:"probe_timeout"`
			} `json:"management,omitempty"`
			Log *struct {
				Output     string `json:"output"`
				MaxSize    int    `json:"max_size"`
				MaxBackups int    `json:"max_backups"`
				MaxAge     int    `json:"max_age"`
				Compress   bool   `json:"compress"`
			} `json:"log"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}

		extIP := strings.TrimSpace(req.ExternalIP)
		probeTarget := strings.TrimSpace(req.ProbeTarget)
		var probeInterval, probeTimeout time.Duration
		if req.Management != nil {
			var err error
			if req.Management.ProbeInterval != "" {
				probeInterval, err = time.ParseDuration(req.Management.ProbeInterval)
				if err != nil || probeInterval <= 0 {
					w.WriteHeader(http.StatusBadRequest)
					writeJSON(w, map[string]any{"error": "探测间隔格式无效"})
					return
				}
			}
			if req.Management.ProbeTimeout != "" {
				probeTimeout, err = time.ParseDuration(req.Management.ProbeTimeout)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					writeJSON(w, map[string]any{"error": "探测超时格式无效"})
					return
				}
				if err := config.ValidateProbeTimeout(probeTimeout); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					writeJSON(w, map[string]any{"error": err.Error()})
					return
				}
			}
		}

		var logCfg *config.LogConfig
		if req.Log != nil && s.projects == nil {
			logCfg = &config.LogConfig{
				Output:     req.Log.Output,
				MaxSize:    req.Log.MaxSize,
				MaxBackups: req.Log.MaxBackups,
				MaxAge:     req.Log.MaxAge,
				Compress:   req.Log.Compress,
			}
		}

		if err := s.updateSettings(extIP, probeTarget, req.SkipCertVerify, logCfg); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}

		// Update extended settings, then persist the complete request once. Saving
		// before these assignments wrote stale values; ignoring this save error also
		// made the API report success even when config.yaml was unchanged.
		var saveErr error
		s.cfgMu.Lock()
		if s.cfgSrc != nil {
			if req.Mode != "" {
				s.cfgSrc.Mode = req.Mode
			}
			if req.Listener != nil {
				s.cfgSrc.Listener.Address = req.Listener.Address
				s.cfgSrc.Listener.Port = req.Listener.Port
				s.cfgSrc.Listener.Username = req.Listener.Username
				s.cfgSrc.Listener.Password = req.Listener.Password
			}
			if req.MultiPort != nil {
				s.cfgSrc.MultiPort.Address = req.MultiPort.Address
				s.cfgSrc.MultiPort.BasePort = req.MultiPort.BasePort
				s.cfgSrc.MultiPort.Username = req.MultiPort.Username
				s.cfgSrc.MultiPort.Password = req.MultiPort.Password
			}
			if req.Pool != nil {
				s.cfgSrc.Pool.Mode = req.Pool.Mode
				s.cfgSrc.Pool.FailureThreshold = req.Pool.FailureThreshold
				if req.Pool.BlacklistDuration != "" {
					if d, err := time.ParseDuration(req.Pool.BlacklistDuration); err == nil {
						s.cfgSrc.Pool.BlacklistDuration = d
					}
				}
			}
			if req.Sticky != nil {
				s.cfgSrc.Sticky.Enabled = req.Sticky.Enabled
				s.cfgSrc.Sticky.Port = req.Sticky.Port
			}
			if req.Management != nil {
				if s.projects == nil {
					s.cfgSrc.Management.Listen = req.Management.Listen
					s.cfgSrc.Management.Password = req.Management.Password
				}
				if req.Management.ProbeConcurrency > 0 {
					s.cfgSrc.Probe.Concurrency = req.Management.ProbeConcurrency
				}
				if probeInterval > 0 {
					s.cfgSrc.Probe.Interval = probeInterval
				}
				if probeTimeout > 0 {
					s.cfgSrc.Probe.Timeout = probeTimeout
				}
			}
			saveErr = s.cfgSrc.SaveSettings()
		} else {
			saveErr = errors.New("配置存储未初始化")
		}
		s.cfgMu.Unlock()
		if saveErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": fmt.Sprintf("保存配置失败: %v", saveErr)})
			return
		}

		writeJSON(w, map[string]any{
			"message":          "设置已保存",
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": req.SkipCertVerify,
			"need_reload":      true,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleSubscriptionStatus returns the current subscription refresh status.
func (s *Server) handleSubscriptionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.subRefresher == nil {
		writeJSON(w, map[string]any{
			"enabled": false,
			"message": "订阅刷新未启用",
		})
		return
	}

	status := s.subRefresher.Status()
	configured := len(s.subRefresher.Subscriptions()) > 0
	writeJSON(w, map[string]any{
		"enabled":        configured,
		"last_refresh":   status.LastRefresh,
		"next_refresh":   status.NextRefresh,
		"node_count":     status.NodeCount,
		"last_error":     status.LastError,
		"refresh_count":  status.RefreshCount,
		"is_refreshing":  status.IsRefreshing,
		"nodes_modified": status.NodesModified,
	})
}

// handleSubscriptionRefresh triggers an immediate subscription refresh.
func (s *Server) handleSubscriptionRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.subRefresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅刷新未启用"})
		return
	}

	if err := s.subRefresher.RefreshNow(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}

	status := s.subRefresher.Status()
	writeJSON(w, map[string]any{
		"message":    "刷新成功",
		"node_count": status.NodeCount,
	})
}

// handleSubscriptionConfig handles GET/PUT for subscription configuration.
func (s *Server) handleSubscriptionConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var urls []string
		var enabled bool
		var interval string
		sharedLock := s.sharedSourceLock()
		sharedLock.RLock()
		if shared := s.sharedSourceConfig(); shared != nil {
			urls = append([]string(nil), shared.Subscriptions...)
		}
		sharedLock.RUnlock()
		s.cfgMu.RLock()
		if s.cfgSrc != nil {
			enabled = s.cfgSrc.SubscriptionRefresh.Enabled
			interval = s.cfgSrc.SubscriptionRefresh.Interval.String()
		}
		s.cfgMu.RUnlock()
		writeJSON(w, map[string]any{
			"subscriptions": urls,
			"enabled":       enabled,
			"interval":      interval,
		})

	case http.MethodPut:
		if s.catalogOnly {
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{"error": "无项目时请通过订阅页面管理共享订阅，自动更新计划仅属于项目"})
			return
		}
		if !s.ensureSharedSourceOwner(w) {
			return
		}
		var req struct {
			Subscriptions []string `json:"subscriptions"`
			Enabled       bool     `json:"enabled"`
			Interval      string   `json:"interval"` // e.g. "1h", "30m"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}

		// Parse interval
		interval, err := time.ParseDuration(req.Interval)
		if err != nil || interval < 5*time.Minute {
			interval = 1 * time.Hour // default
		}

		previousURLs, _, _ := s.subscriptionConfigSnapshot()

		// Clean URLs
		var cleanURLs []string
		for _, u := range req.Subscriptions {
			u = strings.TrimSpace(u)
			if u != "" {
				cleanURLs = append(cleanURLs, u)
			}
		}

		// Persist shared URLs independently from this project's refresh schedule.
		sharedLock := s.sharedSourceLock()
		sharedLock.Lock()
		shared := s.sharedSourceConfig()
		if shared == nil {
			sharedLock.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSON(w, map[string]any{"error": "共享订阅配置未初始化"})
			return
		}
		shared.Subscriptions = cleanURLs
		shared.PruneDisabledSubscriptions()
		if err := shared.SaveSettings(); err != nil {
			sharedLock.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": fmt.Sprintf("保存共享订阅配置失败: %v", err)})
			return
		}
		sharedLock.Unlock()

		s.cfgMu.Lock()
		if s.cfgSrc != nil {
			s.cfgSrc.SubscriptionRefresh.Enabled = req.Enabled
			s.cfgSrc.SubscriptionRefresh.Interval = interval
			if err := s.cfgSrc.SaveSettings(); err != nil {
				s.cfgMu.Unlock()
				w.WriteHeader(http.StatusInternalServerError)
				writeJSON(w, map[string]any{"error": fmt.Sprintf("保存配置失败: %v", err)})
				return
			}
		}
		s.cfgMu.Unlock()

		// Hot-reload subscription manager and wait for refresh to complete
		if s.subRefresher != nil {
			refreshURLs := subscriptionAddedURLs(previousURLs, cleanURLs)
			var refreshErr error
			if len(refreshURLs) > 0 && !subscriptionListRemovesURLs(previousURLs, cleanURLs) {
				refreshErr = s.subRefresher.UpdateConfigAndRefreshSelected(cleanURLs, req.Enabled, interval, refreshURLs)
			} else {
				refreshErr = s.subRefresher.UpdateConfigAndRefresh(cleanURLs, req.Enabled, interval)
			}
			if refreshErr != nil {
				// Config was saved but refresh failed — report partial success
				writeJSON(w, map[string]any{
					"message":       fmt.Sprintf("订阅配置已保存，但刷新失败: %v", refreshErr),
					"subscriptions": cleanURLs,
					"enabled":       req.Enabled,
					"interval":      interval.String(),
					"refresh_error": refreshErr.Error(),
				})
				return
			}
		}
		if s.projects != nil && !s.catalogOnly {
			if err := s.projects.ReloadSharedSources(r.Context()); err != nil {
				writeJSON(w, map[string]any{
					"message":             "订阅配置已保存，但部分项目重载失败",
					"subscriptions":       cleanURLs,
					"shared_reload_error": err.Error(),
				})
				return
			}
		}

		status := SubscriptionStatus{}
		if s.subRefresher != nil {
			status = s.subRefresher.Status()
		}
		writeJSON(w, map[string]any{
			"message":       "订阅配置已更新并生效",
			"subscriptions": cleanURLs,
			"enabled":       req.Enabled,
			"interval":      interval.String(),
			"node_count":    status.NodeCount,
		})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func subscriptionAddedURLs(previous, current []string) []string {
	present := make(map[string]struct{}, len(previous))
	for _, rawURL := range previous {
		present[rawURL] = struct{}{}
	}
	seen := make(map[string]struct{}, len(current))
	added := make([]string, 0)
	for _, rawURL := range current {
		if _, ok := present[rawURL]; ok {
			continue
		}
		if _, ok := seen[rawURL]; ok {
			continue
		}
		seen[rawURL] = struct{}{}
		added = append(added, rawURL)
	}
	return added
}

func subscriptionListRemovesURLs(previous, current []string) bool {
	present := make(map[string]struct{}, len(current))
	for _, rawURL := range current {
		present[rawURL] = struct{}{}
	}
	for _, rawURL := range previous {
		if _, ok := present[rawURL]; !ok {
			return true
		}
	}
	return false
}

type subscriptionMutationRequest struct {
	URL         string `json:"url"`
	OriginalURL string `json:"original_url"`
	Enabled     *bool  `json:"enabled,omitempty"`
}

func validateSubscriptionURL(rawURL string) error {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("请输入有效的 HTTP/HTTPS 订阅地址")
	}
	return nil
}

func (s *Server) subscriptionConfigSnapshot() (urls []string, enabled bool, interval time.Duration) {
	sharedLock := s.sharedSourceLock()
	sharedLock.RLock()
	if shared := s.sharedSourceConfig(); shared != nil {
		urls = append([]string(nil), shared.Subscriptions...)
	}
	sharedLock.RUnlock()

	s.cfgMu.RLock()
	if s.cfgSrc != nil {
		enabled = s.cfgSrc.SubscriptionRefresh.Enabled
		interval = s.cfgSrc.SubscriptionRefresh.Interval
	}
	s.cfgMu.RUnlock()
	if interval <= 0 {
		interval = time.Hour
	}
	return
}

func (s *Server) applySubscriptionConfig(urls []string, enabled bool, interval time.Duration, refreshURLs ...string) error {
	urls = append([]string(nil), urls...)
	sharedLock := s.sharedSourceLock()
	sharedLock.Lock()
	shared := s.sharedSourceConfig()
	if shared == nil {
		sharedLock.Unlock()
		return errors.New("共享订阅配置未初始化")
	}
	shared.Subscriptions = urls
	shared.PruneDisabledSubscriptions()
	if err := shared.SaveSettings(); err != nil {
		sharedLock.Unlock()
		return fmt.Errorf("保存共享订阅配置失败: %w", err)
	}
	sharedLock.Unlock()

	s.cfgMu.Lock()
	if s.cfgSrc == nil {
		s.cfgMu.Unlock()
		return errors.New("配置管理未启用")
	}
	s.cfgSrc.Subscriptions = urls
	s.cfgSrc.SubscriptionRefresh.Enabled = enabled
	s.cfgSrc.SubscriptionRefresh.Interval = interval
	if err := s.cfgSrc.SaveSettings(); err != nil {
		s.cfgMu.Unlock()
		return fmt.Errorf("保存配置失败: %w", err)
	}
	s.cfgMu.Unlock()
	if s.subRefresher == nil {
		return errors.New("订阅管理器未启用")
	}
	var refreshErr error
	if len(refreshURLs) > 0 {
		refreshErr = s.subRefresher.UpdateConfigAndRefreshSelected(urls, enabled, interval, refreshURLs)
	} else {
		refreshErr = s.subRefresher.UpdateConfigAndRefresh(urls, enabled, interval)
	}
	if s.projects != nil && !s.catalogOnly {
		if reloadErr := s.projects.ReloadSharedSources(context.Background()); reloadErr != nil {
			return errors.Join(refreshErr, reloadErr)
		}
	}
	return refreshErr
}

func (s *Server) applySubscriptionSchedule(enabled bool, interval time.Duration) error {
	urls, _, _ := s.subscriptionConfigSnapshot()
	s.cfgMu.Lock()
	if s.cfgSrc == nil {
		s.cfgMu.Unlock()
		return errors.New("配置管理未启用")
	}
	s.cfgSrc.SubscriptionRefresh.Enabled = enabled
	s.cfgSrc.SubscriptionRefresh.Interval = interval
	if err := s.cfgSrc.SaveSettings(); err != nil {
		s.cfgMu.Unlock()
		return fmt.Errorf("保存配置失败: %w", err)
	}
	s.cfgMu.Unlock()
	if s.subRefresher != nil {
		s.subRefresher.UpdateConfig(urls, enabled, interval)
	}
	return nil
}

func (s *Server) subscriptionManagementPayload() map[string]any {
	urls, enabled, interval := s.subscriptionConfigSnapshot()
	items := make([]SubscriptionInfo, 0)
	status := SubscriptionStatus{}
	if s.subRefresher != nil {
		items = s.subRefresher.Subscriptions()
		status = s.subRefresher.Status()
	} else {
		for _, rawURL := range urls {
			sharedLock := s.sharedSourceLock()
			sharedLock.RLock()
			shared := s.sharedSourceConfig()
			subscriptionEnabled := shared == nil || shared.SubscriptionEnabled(rawURL)
			sharedLock.RUnlock()
			statusName := "pending"
			if !subscriptionEnabled {
				statusName = "disabled"
			}
			items = append(items, SubscriptionInfo{URL: rawURL, Status: statusName, Enabled: subscriptionEnabled})
		}
	}
	return map[string]any{
		"items":         items,
		"enabled":       enabled,
		"interval":      interval.String(),
		"last_refresh":  status.LastRefresh,
		"next_refresh":  status.NextRefresh,
		"node_count":    status.NodeCount,
		"is_refreshing": status.IsRefreshing,
		"refresh_error": status.LastError,
		"refresh_count": status.RefreshCount,
	}
}

// handleSubscriptions provides CRUD for individual URLs while preserving the
// existing []string YAML representation.
func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	urls, enabled, interval := s.subscriptionConfigSnapshot()
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.subscriptionManagementPayload())
		return
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		if !s.ensureSharedSourceOwner(w) {
			return
		}
		var req subscriptionMutationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		req.URL = strings.TrimSpace(req.URL)
		req.OriginalURL = strings.TrimSpace(req.OriginalURL)
		var refreshURLs []string

		switch r.Method {
		case http.MethodPost:
			if err := validateSubscriptionURL(req.URL); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": err.Error()})
				return
			}
			for _, existing := range urls {
				if existing == req.URL {
					w.WriteHeader(http.StatusConflict)
					writeJSON(w, map[string]any{"error": "订阅已存在"})
					return
				}
			}
			urls = append(urls, req.URL)
			refreshURLs = []string{req.URL}
		case http.MethodPut:
			if req.OriginalURL == "" {
				req.OriginalURL = req.URL
			}
			if err := validateSubscriptionURL(req.URL); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": err.Error()})
				return
			}
			found := false
			for i, existing := range urls {
				if existing == req.OriginalURL {
					urls[i] = req.URL
					found = true
				} else if existing == req.URL {
					w.WriteHeader(http.StatusConflict)
					writeJSON(w, map[string]any{"error": "订阅已存在"})
					return
				}
			}
			if !found {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(w, map[string]any{"error": "订阅不存在"})
				return
			}
			if req.OriginalURL != req.URL {
				sharedLock := s.sharedSourceLock()
				sharedLock.Lock()
				if shared := s.sharedSourceConfig(); shared != nil && !shared.SubscriptionEnabled(req.OriginalURL) {
					shared.SetSubscriptionEnabled(req.OriginalURL, true)
					shared.SetSubscriptionEnabled(req.URL, false)
				}
				sharedLock.Unlock()
			}
		case http.MethodDelete:
			target := req.URL
			filtered := make([]string, 0, len(urls))
			found := false
			for _, existing := range urls {
				if existing == target {
					found = true
					continue
				}
				filtered = append(filtered, existing)
			}
			if !found {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(w, map[string]any{"error": "订阅不存在"})
				return
			}
			urls = filtered
			sharedLock := s.sharedSourceLock()
			sharedLock.Lock()
			if shared := s.sharedSourceConfig(); shared != nil {
				shared.SetSubscriptionEnabled(target, true)
			}
			sharedLock.Unlock()
		case http.MethodPatch:
			if req.URL == "" || req.Enabled == nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": "缺少订阅地址或启用状态"})
				return
			}
			if s.subRefresher == nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(w, map[string]any{"error": "订阅管理器未启用"})
				return
			}
			if err := s.subRefresher.SetSubscriptionEnabled(req.URL, *req.Enabled); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(w, map[string]any{"error": err.Error()})
				return
			}
			sharedLock := s.sharedSourceLock()
			sharedLock.Lock()
			if shared := s.sharedSourceConfig(); shared != nil {
				shared.SetSubscriptionEnabled(req.URL, *req.Enabled)
				if err := shared.SaveSettings(); err != nil {
					sharedLock.Unlock()
					w.WriteHeader(http.StatusInternalServerError)
					writeJSON(w, map[string]any{"error": fmt.Sprintf("保存订阅状态失败: %v", err)})
					return
				}
			}
			sharedLock.Unlock()
			payload := s.subscriptionManagementPayload()
			if !s.catalogOnly {
				s.reloadSharedSources(r.Context(), payload)
			}
			writeJSON(w, payload)
			return
		}

		if err := s.applySubscriptionConfig(urls, enabled, interval, refreshURLs...); err != nil {
			writeJSON(w, map[string]any{"message": "订阅配置已保存，但刷新失败", "refresh_error": err.Error()})
			return
		}
		writeJSON(w, s.subscriptionManagementPayload())
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSubscriptionSettings(w http.ResponseWriter, r *http.Request) {
	if s.catalogOnly {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": "无项目时不设置自动订阅计划，请手动更新订阅"})
		return
	}
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Enabled  bool   `json:"enabled"`
		Interval string `json:"interval"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return
	}
	interval, err := time.ParseDuration(strings.TrimSpace(req.Interval))
	if err != nil || interval < 5*time.Minute {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "刷新间隔不能小于 5 分钟"})
		return
	}
	if err := s.applySubscriptionSchedule(req.Enabled, interval); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, s.subscriptionManagementPayload())
}

func (s *Server) handleManagedSubscriptionRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.subRefresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅管理器未启用"})
		return
	}
	if len(s.subRefresher.Subscriptions()) == 0 {
		writeJSON(w, s.subscriptionManagementPayload())
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	var refreshErr error
	if req.URL == "" {
		refreshErr = s.subRefresher.RefreshNow()
	} else {
		urls, enabled, interval := s.subscriptionConfigSnapshot()
		found := false
		for _, configuredURL := range urls {
			if configuredURL == req.URL {
				found = true
				break
			}
		}
		if !found {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]any{"error": "订阅不存在"})
			return
		}
		refreshErr = s.subRefresher.UpdateConfigAndRefreshSelected(urls, enabled, interval, []string{req.URL})
	}
	if refreshErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": refreshErr.Error()})
		return
	}
	writeJSON(w, s.subscriptionManagementPayload())
}

// nodePayload is the JSON request body for node CRUD operations.
type nodePayload struct {
	Name     string `json:"name"`
	URI      string `json:"uri"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (p nodePayload) toConfig() config.NodeConfig {
	return config.NodeConfig{
		Name:     p.Name,
		URI:      p.URI,
		Port:     p.Port,
		Username: p.Username,
		Password: p.Password,
	}
}

// configNodeView adds display-only ownership metadata without exposing the
// subscription URL or persisting it with the node definition.
type configNodeView struct {
	config.NodeConfig
	SubscriptionName string `json:"subscription_name,omitempty"`
}

func (s *Server) configNodeViews(nodes []config.NodeConfig) []configNodeView {
	subscriptionNames := make(map[string]string)
	if s.subRefresher != nil {
		for _, subscription := range s.subRefresher.Subscriptions() {
			rawURL := strings.TrimSpace(subscription.URL)
			if rawURL != "" {
				subscriptionNames[rawURL] = strings.TrimSpace(subscription.Name)
			}
		}
	}

	views := make([]configNodeView, 0, len(nodes))
	for _, node := range nodes {
		view := configNodeView{NodeConfig: node}
		if node.Source == config.NodeSourceSubscription {
			view.SubscriptionName = subscriptionNames[strings.TrimSpace(node.SubscriptionURL)]
			if view.SubscriptionName == "" {
				view.SubscriptionName = subscriptionNameFromURL(node.SubscriptionURL)
			}
		}
		views = append(views, view)
	}
	return views
}

func subscriptionNameFromURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err == nil && parsed.Hostname() != "" {
		return parsed.Hostname()
	}
	return "未知订阅"
}

// handleConfigNodes handles GET (list) and POST (create) for config nodes.
func (s *Server) handleConfigNodes(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNodeManager(w) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		nodes, err := s.nodeMgr.ListConfigNodes(r.Context())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"nodes": s.configNodeViews(nodes)})
	case http.MethodPost:
		if !s.ensureSharedSourceOwner(w) {
			return
		}
		var payload nodePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := s.nodeMgr.CreateNode(r.Context(), payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		response := map[string]any{"node": node}
		s.reloadAfterNodeChange(r.Context(), response, "节点已添加并重载生效")
		writeJSON(w, response)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleConfigNodeItem handles PUT (update) and DELETE for a specific config node.
func (s *Server) handleConfigNodeItem(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNodeManager(w) {
		return
	}

	namePart := strings.TrimPrefix(r.URL.Path, "/api/nodes/config/")
	nodeName, err := url.PathUnescape(namePart)
	if err != nil || nodeName == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "节点名称无效"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		if !s.ensureSharedSourceOwner(w) {
			return
		}
		var payload nodePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := s.nodeMgr.UpdateNode(r.Context(), nodeName, payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		response := map[string]any{"node": node}
		s.reloadAfterNodeChange(r.Context(), response, "节点已更新并重载生效")
		writeJSON(w, response)
	case http.MethodDelete:
		if !s.ensureSharedSourceOwner(w) {
			return
		}
		if err := s.nodeMgr.DeleteNode(r.Context(), nodeName); err != nil {
			s.respondNodeError(w, err)
			return
		}
		response := map[string]any{}
		s.reloadAfterNodeChange(r.Context(), response, "节点已删除并重载生效")
		writeJSON(w, response)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleNodeImport parses YAML/TXT/pasted content and atomically adds nodes.
func (s *Server) handleNodeImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.ensureNodeManager(w) {
		return
	}
	if !s.ensureSharedSourceOwner(w) {
		return
	}

	const (
		maxImportContentSize = 10 * 1024 * 1024
		maxImportBodySize    = 24 * 1024 * 1024
	)
	r.Body = http.MaxBytesReader(w, r.Body, maxImportBodySize)
	var req struct {
		Content  string `json:"content"`
		Filename string `json:"filename,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "导入内容格式错误或文件过大"})
		return
	}
	if len(req.Content) > maxImportContentSize {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		writeJSON(w, map[string]any{"error": "导入文件不能超过 10 MB"})
		return
	}

	nodes, err := config.ParseNodeImportContent(req.Content)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	added, skipped, err := s.nodeMgr.ImportConfigNodes(r.Context(), nodes)
	if err != nil {
		s.respondNodeError(w, err)
		return
	}

	response := map[string]any{
		"parsed":  len(nodes),
		"added":   len(added),
		"skipped": skipped,
		"nodes":   added,
		"message": fmt.Sprintf("已导入 %d 个节点", len(added)),
	}
	if len(added) > 0 {
		s.reloadAfterNodeChange(r.Context(), response, fmt.Sprintf("已导入 %d 个节点并重载生效", len(added)))
	}
	writeJSON(w, response)
}

// handleReload triggers a configuration reload.
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.ensureNodeManager(w) {
		return
	}

	if err := s.nodeMgr.TriggerReload(r.Context()); err != nil {
		s.respondNodeError(w, err)
		return
	}
	response := map[string]any{
		"message": "重载成功，现有连接已被中断",
	}
	writeJSON(w, response)
}

func (s *Server) ensureNodeManager(w http.ResponseWriter) bool {
	if s.nodeMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "节点管理未启用"})
		return false
	}
	return true
}

func (s *Server) reloadAfterNodeChange(ctx context.Context, response map[string]any, successMessage string) {
	if s.projects == nil {
		if err := s.nodeMgr.TriggerReload(ctx); err != nil {
			response["message"] = "节点配置已保存，但自动重载失败"
			response["reload_error"] = err.Error()
			return
		}
		response["reloaded"] = true
	} else {
		s.reloadSharedSources(ctx, response)
		if _, failed := response["shared_reload_error"]; failed {
			response["message"] = "节点配置已保存，但部分项目重载失败"
			return
		}
	}
	response["message"] = successMessage
}

func (s *Server) respondNodeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNodeNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrNodeConflict), errors.Is(err, ErrInvalidNode):
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	writeJSON(w, map[string]any{"error": err.Error()})
}

// handleTraffic streams real-time traffic from sing-box Clash API as SSE.
// Clash API /traffic returns newline-delimited JSON; we convert to SSE for browser EventSource.
func (s *Server) handleTraffic(w http.ResponseWriter, r *http.Request) {
	// Connect to sing-box Clash API
	trafficAPI := s.cfg.TrafficAPI
	if trafficAPI == "" {
		trafficAPI = "http://127.0.0.1:9092/traffic"
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, trafficAPI, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": "无法创建流量统计请求", "details": err.Error()})
		return
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		writeJSON(w, map[string]any{"error": "无法连接到流量统计接口", "details": err.Error()})
		return
	}
	defer resp.Body.Close()

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Read NDJSON lines from Clash API and forward as SSE
	buf := make([]byte, 4096)
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			// Each chunk may contain one or more JSON lines; forward as-is in SSE data frames
			lines := strings.Split(strings.TrimSpace(string(buf[:n])), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", line)
			}
			flusher.Flush()
		}
		if readErr != nil {
			return
		}
	}
}

// handleLogs returns recent console log content from the in-memory ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	buffer := s.logBuffer
	if buffer == nil {
		buffer = SharedLogBuffer
	}
	content := buffer.Content()
	writeJSON(w, map[string]any{"logs": content})
}

// Session management functions

// generateSessionToken creates a cryptographically secure random token.
func (s *Server) generateSessionToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate session token: %w", err)
	}
	return hex.EncodeToString(tokenBytes), nil
}

// createSession creates a new session with expiration.
func (s *Server) createSession() (*Session, error) {
	token, err := s.generateSessionToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		Token:     token,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionTTL),
	}

	s.sessionMu.Lock()
	s.sessions[token] = session
	s.sessionMu.Unlock()

	return session, nil
}

// validateSession checks if a session token is valid and not expired.
func (s *Server) validateSession(token string) bool {
	s.sessionMu.RLock()
	session, exists := s.sessions[token]
	s.sessionMu.RUnlock()

	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		s.sessionMu.Lock()
		delete(s.sessions, token)
		s.sessionMu.Unlock()
		return false
	}

	return true
}

// cleanupExpiredSessions periodically removes expired sessions.
func (s *Server) cleanupExpiredSessions() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		s.sessionMu.Lock()
		for token, session := range s.sessions {
			if now.After(session.ExpiresAt) {
				delete(s.sessions, token)
			}
		}
		s.sessionMu.Unlock()
	}
}

// secureCompareStrings performs constant-time string comparison to prevent timing attacks.
func secureCompareStrings(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
