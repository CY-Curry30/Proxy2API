package project

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
)

var ErrProjectNotFound = errors.New("project not found")

// Registry owns the global project catalog while each Runtime owns its mutable
// proxy resources.
type Registry struct {
	mu         sync.RWMutex
	catalogMu  sync.Mutex
	workspace  *config.Workspace
	parentCtx  context.Context
	sharedPath string
	sharedCfg  *config.Config
	sharedMu   *sync.RWMutex
	ports      *PortRegistry
	runtimes   map[string]*Runtime
	catalog    *CatalogRuntime
}

func NewRegistry(parent context.Context, workspace *config.Workspace) (*Registry, error) {
	if workspace == nil {
		return nil, errors.New("project registry requires a workspace")
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := workspace.Validate(); err != nil {
		return nil, err
	}
	sharedPath := strings.TrimSpace(workspace.SharedConfigPath())
	if sharedPath == "" {
		return nil, errors.New("project registry requires a shared source config")
	}
	if err := migrateSharedSources(workspace); err != nil {
		return nil, err
	}
	sharedCfg, err := config.LoadShared(sharedPath)
	if err != nil {
		return nil, fmt.Errorf("load shared source config: %w", err)
	}
	r := &Registry{
		workspace:  workspace,
		parentCtx:  parent,
		sharedPath: sharedPath,
		sharedCfg:  sharedCfg,
		sharedMu:   &sync.RWMutex{},
		ports:      NewPortRegistry(workspace.Management.Listen),
		runtimes:   make(map[string]*Runtime, len(workspace.Projects)),
	}
	r.catalog = NewCatalogRuntime(parent, sharedCfg, r.sharedMu, r.ports, r.reloadSharedConfigFromDisk)
	catalogChanged := false
	for _, id := range workspace.SortedProjectIDs() {
		spec := workspace.Projects[id]
		if spec.ClashAPIPort == 0 {
			port, err := r.ports.NextAvailable(9092)
			if err != nil {
				return nil, fmt.Errorf("allocate traffic API for project %q: %w", id, err)
			}
			spec.ClashAPIPort = port
			workspace.Projects[id] = spec
			catalogChanged = true
		}
		if err := r.ports.Claim(id, spec.ClashAPIPort, "internal traffic API"); err != nil {
			return nil, err
		}
		path, err := workspace.ProjectConfigPath(id)
		if err != nil {
			return nil, err
		}
		if cfg, err := config.LoadProjectWithShared(path, sharedCfg); err == nil {
			cfg.ClashAPIPort = spec.ClashAPIPort
			if err := r.ports.Reserve(id, cfg); err != nil {
				log.Printf("[project:%s] configured ports are not reservable: %v", id, err)
			}
		}
		r.runtimes[id] = NewRuntime(parent, id, path, sharedPath, sharedCfg, r.sharedMu, spec.ClashAPIPort, r.ports)
	}
	if catalogChanged && workspace.Persisted() {
		if err := workspace.Save(); err != nil {
			return nil, err
		}
	}
	if len(workspace.Projects) == 0 {
		if err := r.catalog.Start(); err != nil {
			log.Printf("[shared-catalog] startup failed: %v", err)
		}
	}
	return r, nil
}

func migrateSharedSources(workspace *config.Workspace) error {
	sharedPath := workspace.SharedConfigPath()
	legacyPath := workspace.LegacyConfigPath()
	_, sharedErr := os.Stat(sharedPath)
	sharedExists := sharedErr == nil
	if sharedErr != nil && !os.IsNotExist(sharedErr) {
		return fmt.Errorf("inspect shared source config: %w", sharedErr)
	}

	var shared *config.Config
	if sharedExists {
		var err error
		shared, err = config.LoadShared(sharedPath)
		if err != nil {
			return fmt.Errorf("load shared source config: %w", err)
		}
	} else {
		legacy, err := config.Load(legacyPath)
		if err != nil {
			return fmt.Errorf("load legacy source config: %w", err)
		}
		shared = config.NewSharedConfig(sharedPath, legacy)
	}

	changed := !sharedExists
	rewriteProjectConfigs := !workspace.SharedSourcesMigrated || !sharedExists
	knownURLs := make(map[string]struct{}, len(shared.Subscriptions))
	for _, rawURL := range shared.Subscriptions {
		knownURLs[rawURL] = struct{}{}
	}
	knownNodes := make(map[string]struct{}, len(shared.Nodes))
	for _, node := range shared.Nodes {
		if key := node.NodeKey(); key != "" {
			knownNodes[key] = struct{}{}
		}
	}

	for _, id := range workspace.SortedProjectIDs() {
		oldPath, err := workspace.ProjectConfigPath(id)
		if err != nil {
			return err
		}
		if filepath.Clean(oldPath) == filepath.Clean(sharedPath) {
			oldPath = legacyPath
		}
		projectCfg, err := config.LoadReadOnly(oldPath)
		if err != nil {
			return fmt.Errorf("load project %q during shared config migration: %w", id, err)
		}
		for _, rawURL := range projectCfg.Subscriptions {
			if _, exists := knownURLs[rawURL]; exists {
				continue
			}
			knownURLs[rawURL] = struct{}{}
			shared.Subscriptions = append(shared.Subscriptions, rawURL)
			changed = true
		}
		for _, rawURL := range projectCfg.DisabledSubscriptions {
			if !containsString(shared.DisabledSubscriptions, rawURL) {
				shared.DisabledSubscriptions = append(shared.DisabledSubscriptions, rawURL)
				changed = true
			}
		}
		for _, node := range projectCfg.Nodes {
			if node.Source == config.NodeSourceSubscription {
				continue
			}
			key := node.NodeKey()
			if key == "" {
				continue
			}
			if _, exists := knownNodes[key]; exists {
				continue
			}
			knownNodes[key] = struct{}{}
			node.Source = config.NodeSourceInline
			node.SubscriptionURL = ""
			node.StateKey = ""
			node.Port = 0
			node.Username = ""
			node.Password = ""
			shared.Nodes = append(shared.Nodes, node)
			changed = true
		}

		targetPath, err := workspace.NewProjectConfigPath(id)
		if err != nil {
			return err
		}
		if rewriteProjectConfigs || filepath.Clean(oldPath) != filepath.Clean(targetPath) || !fileExists(targetPath) {
			if err := config.WriteRuntimeProjectConfig(targetPath, projectCfg); err != nil {
				return fmt.Errorf("write migrated project %q config: %w", id, err)
			}
			changed = true
		}
		spec := workspace.Projects[id]
		newRef, relErr := filepath.Rel(workspace.RootDir(), targetPath)
		if relErr != nil {
			return fmt.Errorf("resolve migrated project %q path: %w", id, relErr)
		}
		if spec.Config != newRef {
			spec.Config = newRef
			workspace.Projects[id] = spec
			changed = true
		}
	}

	if changed {
		if err := config.WriteSharedConfig(sharedPath, shared); err != nil {
			return fmt.Errorf("write shared source config: %w", err)
		}
	}
	workspace.SharedSourcesMigrated = true
	if changed || workspace.Persisted() {
		if err := workspace.Save(); err != nil {
			return fmt.Errorf("save shared config migration marker: %w", err)
		}
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (r *Registry) DefaultProjectID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.workspace.DefaultProject
}

func (r *Registry) SystemSettings() monitor.SystemSettings {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return monitor.SystemSettings{
		Management: r.workspace.Management,
		Log:        r.workspace.Log,
	}
}

func (r *Registry) UpdateSystemSettings(ctx context.Context, settings monitor.SystemSettings) error {
	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if strings.TrimSpace(settings.Management.Listen) == "" {
		return errors.New("management listen address cannot be empty")
	}
	if settings.Log.Output != "stdout" && settings.Log.Output != "file" {
		return errors.New("log output must be stdout or file")
	}
	r.mu.Lock()
	previousManagement := r.workspace.Management
	previousLog := r.workspace.Log
	r.workspace.Management.Listen = strings.TrimSpace(settings.Management.Listen)
	r.workspace.Management.Password = settings.Management.Password
	if settings.Management.Enabled != nil {
		r.workspace.Management.Enabled = settings.Management.Enabled
	}
	r.workspace.Log.Output = settings.Log.Output
	if settings.Log.File != "" {
		r.workspace.Log.File = settings.Log.File
	}
	if settings.Log.MaxSize > 0 {
		r.workspace.Log.MaxSize = settings.Log.MaxSize
	}
	if settings.Log.MaxBackups >= 0 {
		r.workspace.Log.MaxBackups = settings.Log.MaxBackups
	}
	if settings.Log.MaxAge >= 0 {
		r.workspace.Log.MaxAge = settings.Log.MaxAge
	}
	r.workspace.Log.Compress = settings.Log.Compress
	if err := r.workspace.Save(); err != nil {
		r.workspace.Management = previousManagement
		r.workspace.Log = previousLog
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
	return nil
}

// StartAutostartProjects starts projects deterministically. A failed project is
// recorded but does not prevent the control plane or other projects starting.
func (r *Registry) StartAutostartProjects() {
	r.mu.RLock()
	ids := r.workspace.SortedProjectIDs()
	specs := make(map[string]config.ProjectSpec, len(ids))
	for _, id := range ids {
		specs[id] = r.workspace.Projects[id]
	}
	r.mu.RUnlock()
	for _, id := range ids {
		spec := specs[id]
		if !spec.Enabled || !spec.Autostart {
			continue
		}
		if err := r.StartProject(r.parentCtx, id); err != nil {
			log.Printf("[project:%s] startup failed: %v", id, err)
		}
	}
}

func (r *Registry) ListProjects() []monitor.ProjectSummary {
	r.mu.RLock()
	ids := make([]string, 0, len(r.workspace.Projects))
	for id := range r.workspace.Projects {
		ids = append(ids, id)
	}
	defaultID := r.workspace.DefaultProject
	specs := make(map[string]config.ProjectSpec, len(ids))
	runtimes := make(map[string]*Runtime, len(ids))
	for _, id := range ids {
		specs[id] = r.workspace.Projects[id]
		runtimes[id] = r.runtimes[id]
	}
	r.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool {
		if ids[i] == defaultID {
			return true
		}
		if ids[j] == defaultID {
			return false
		}
		return ids[i] < ids[j]
	})
	result := make([]monitor.ProjectSummary, 0, len(ids))
	for _, id := range ids {
		result = append(result, projectSummary(id, specs[id], runtimes[id]))
	}
	return result
}

func projectSummary(id string, spec config.ProjectSpec, runtime *Runtime) monitor.ProjectSummary {
	summary := monitor.ProjectSummary{
		ID:           id,
		Name:         spec.Name,
		Enabled:      spec.Enabled,
		Autostart:    spec.Autostart,
		Status:       string(StatusStopped),
		ConfigPath:   spec.Config,
		ClashAPIPort: spec.ClashAPIPort,
	}
	if runtime == nil {
		return summary
	}
	status, lastError, startedAt := runtime.Status()
	summary.Status = string(status)
	summary.LastError = lastError
	summary.StartedAt = startedAt
	cfg := runtime.Config()
	if cfg == nil {
		// Stopped projects still need a useful configuration preview in the
		// management page without updating their persisted port mapping.
		if runtime.sharedMu != nil {
			runtime.sharedMu.RLock()
		}
		if loaded, err := config.LoadProjectReadOnlyWithShared(runtime.configPath, runtime.sharedCfg); err == nil {
			cfg = loaded
		}
		if runtime.sharedMu != nil {
			runtime.sharedMu.RUnlock()
		}
	}
	if cfg != nil {
		summary.Mode = cfg.Mode
		summary.ListenerAddress = cfg.Listener.Address
		summary.ListenerPort = cfg.Listener.Port
		summary.MultiPortAddress = cfg.MultiPort.Address
		summary.MultiPortBase = cfg.MultiPort.BasePort
		summary.NodeCount = len(cfg.Nodes)
		summary.SubscriptionCount = len(cfg.Subscriptions)
		if summary.ClashAPIPort == 0 {
			summary.ClashAPIPort = cfg.ClashAPIPort
		}
		summary.Settings = runtimeSettings(cfg)
	} else if mgr := runtime.MonitorManager(); mgr != nil {
		summary.NodeCount = len(mgr.Snapshot())
	}
	return summary
}

func runtimeSettings(cfg *config.Config) monitor.ProjectRuntimeSettings {
	if cfg == nil {
		return monitor.ProjectRuntimeSettings{}
	}
	return monitor.ProjectRuntimeSettings{
		Mode:           cfg.Mode,
		ExternalIP:     cfg.ExternalIP,
		SkipCertVerify: cfg.SkipCertVerify,
		Listener: monitor.ProjectListenerSettings{
			Address: cfg.Listener.Address, Port: cfg.Listener.Port,
			Username: cfg.Listener.Username, Password: cfg.Listener.Password,
		},
		MultiPort: monitor.ProjectMultiPortSettings{
			Address: cfg.MultiPort.Address, BasePort: cfg.MultiPort.BasePort,
			Username: cfg.MultiPort.Username, Password: cfg.MultiPort.Password,
		},
		Pool: monitor.ProjectPoolSettings{
			Mode: cfg.Pool.Mode, FailureThreshold: cfg.Pool.FailureThreshold,
			BlacklistDuration: cfg.Pool.BlacklistDuration.String(),
			RetryEnabled:      cfg.Pool.RetryEnabledOrDefault(), RetryAttempts: cfg.Pool.RetryAttempts,
		},
		Sticky: monitor.ProjectStickySettings{Enabled: cfg.Sticky.Enabled, Port: cfg.Sticky.Port},
		Probe: monitor.ProjectProbeSettings{
			Target: cfg.ProbeTargetOrDefault(), Interval: cfg.ProbeIntervalOrDefault().String(),
			Timeout: cfg.ProbeTimeoutOrDefault().String(), Concurrency: cfg.ProbeConcurrencyOrDefault(),
		},
		SubscriptionRefresh: monitor.ProjectSubscriptionSettings{
			Enabled: cfg.SubscriptionRefresh.Enabled, Interval: cfg.SubscriptionRefresh.Interval.String(),
		},
		SelectedSubscriptions: cfg.SelectedSubscriptions,
	}
}

func (r *Registry) Project(id string) (monitor.ProjectBinding, error) {
	r.mu.RLock()
	spec, ok := r.workspace.Projects[id]
	runtime := r.runtimes[id]
	r.mu.RUnlock()
	if !ok || runtime == nil {
		return monitor.ProjectBinding{}, fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	status, lastError, _ := runtime.Status()
	if status != StatusRunning {
		if lastError != "" {
			return monitor.ProjectBinding{}, fmt.Errorf("project %q is %s: %s", id, status, lastError)
		}
		return monitor.ProjectBinding{}, fmt.Errorf("project %q is %s", id, status)
	}
	cfg := runtime.Config()
	mgr := runtime.MonitorManager()
	if cfg == nil || mgr == nil {
		return monitor.ProjectBinding{}, fmt.Errorf("project %q runtime is not ready", id)
	}
	return monitor.ProjectBinding{
		ID:                    id,
		Name:                  spec.Name,
		Config:                cfg,
		SharedConfig:          r.sharedCfg,
		SharedConfigMu:        r.sharedMu,
		Monitor:               mgr,
		NodeManager:           runtime.NodeManager(),
		SubscriptionRefresher: runtime.SubscriptionRefresher(),
		LogBuffer:             runtime.LogBuffer(),
	}, nil
}

func (r *Registry) SharedCatalog() (monitor.ProjectBinding, error) {
	r.mu.RLock()
	projectCount := len(r.workspace.Projects)
	catalog := r.catalog
	r.mu.RUnlock()
	if projectCount != 0 {
		return monitor.ProjectBinding{}, errors.New("共享目录模式仅在没有项目时可用")
	}
	if catalog == nil {
		return monitor.ProjectBinding{}, errors.New("共享目录未初始化")
	}
	return catalog.Binding()
}

func (r *Registry) CreateProject(ctx context.Context, request monitor.ProjectCreateRequest) (monitor.ProjectSummary, error) {
	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()
	id := strings.TrimSpace(request.ID)
	if err := config.ValidateProjectID(id); err != nil {
		return monitor.ProjectSummary{}, err
	}
	mode := strings.TrimSpace(request.Mode)
	if mode == "" {
		mode = "pool"
	}
	if mode == "multi_port" {
		mode = "multi-port"
	}
	if mode != "pool" && mode != "multi-port" && mode != "hybrid" {
		return monitor.ProjectSummary{}, fmt.Errorf("unsupported mode %q", mode)
	}

	r.mu.Lock()
	if _, exists := r.workspace.Projects[id]; exists {
		r.mu.Unlock()
		return monitor.ProjectSummary{}, fmt.Errorf("project %q already exists", id)
	}
	firstProject := len(r.workspace.Projects) == 0
	previousDefault := r.workspace.DefaultProject
	r.mu.Unlock()
	restoreCatalog := false
	defer func() {
		if restoreCatalog && r.catalog != nil {
			if err := r.catalog.Start(); err != nil {
				log.Printf("[shared-catalog] restore after project creation failure: %v", err)
			}
		}
	}()
	if firstProject && r.catalog != nil {
		if err := r.catalog.Stop(); err != nil {
			return monitor.ProjectSummary{}, fmt.Errorf("stop shared catalog: %w", err)
		}
		restoreCatalog = true
	}

	listenerPort := request.ListenerPort
	if listenerPort == 0 {
		var err error
		listenerPort, err = r.ports.NextAvailable(2323)
		if err != nil {
			return monitor.ProjectSummary{}, err
		}
	}
	multiPortBase := request.MultiPortBase
	if multiPortBase == 0 {
		var err error
		multiPortBase, err = r.ports.NextAvailable(24000)
		if err != nil {
			return monitor.ProjectSummary{}, err
		}
	}
	clashAPIPort, err := r.ports.NextAvailable(9092)
	if err != nil {
		return monitor.ProjectSummary{}, err
	}
	enabled := true
	if request.Enabled != nil {
		enabled = *request.Enabled
	}
	autostart := true
	if request.Autostart != nil {
		autostart = *request.Autostart
	}
	name := strings.TrimSpace(request.Name)
	if name == "" {
		name = id
	}

	projectCfg := defaultProjectConfig(mode, listenerPort, multiPortBase)
	projectCfg.ClashAPIPort = clashAPIPort
	if request.Settings != nil {
		settings := *request.Settings
		if settings.Listener.Port == 0 {
			settings.Listener.Port = listenerPort
		}
		if settings.MultiPort.BasePort == 0 {
			settings.MultiPort.BasePort = multiPortBase
		}
		if err := applyRuntimeSettings(projectCfg, settings); err != nil {
			return monitor.ProjectSummary{}, err
		}
	}
	if err := r.ports.Reserve(id, projectCfg); err != nil {
		return monitor.ProjectSummary{}, err
	}
	reserved := true
	defer func() {
		if reserved {
			r.ports.Release(id)
		}
	}()
	projectPath, err := r.workspace.NewProjectConfigPath(id)
	if err != nil {
		return monitor.ProjectSummary{}, err
	}
	if _, err := os.Stat(projectPath); err == nil {
		return monitor.ProjectSummary{}, fmt.Errorf("project directory already contains %s", projectPath)
	} else if !os.IsNotExist(err) {
		return monitor.ProjectSummary{}, err
	}
	if err := config.WriteProjectConfig(projectPath, projectCfg); err != nil {
		return monitor.ProjectSummary{}, err
	}

	configRef := filepath.Join(r.workspace.ProjectsDir, id, "project.yaml")
	if firstProject {
		if relativePath, relErr := filepath.Rel(r.workspace.RootDir(), projectPath); relErr == nil {
			configRef = relativePath
		} else {
			configRef = projectPath
		}
	}
	spec := config.ProjectSpec{
		Name:         name,
		Enabled:      enabled,
		Autostart:    autostart,
		Config:       configRef,
		ClashAPIPort: clashAPIPort,
	}
	runtime := NewRuntime(r.parentCtx, id, projectPath, r.sharedPath, r.sharedCfg, r.sharedMu, clashAPIPort, r.ports)
	r.mu.Lock()
	r.workspace.Projects[id] = spec
	if firstProject {
		r.workspace.DefaultProject = id
	}
	r.runtimes[id] = runtime
	if err := r.workspace.Save(); err != nil {
		delete(r.workspace.Projects, id)
		delete(r.runtimes, id)
		r.workspace.DefaultProject = previousDefault
		r.mu.Unlock()
		return monitor.ProjectSummary{}, err
	}
	r.mu.Unlock()
	reserved = false
	restoreCatalog = false

	if enabled && autostart {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return projectSummary(id, spec, runtime), ctx.Err()
			default:
			}
		}
		if err := runtime.Start(); err != nil {
			log.Printf("[project:%s] created but failed to start: %v", id, err)
		}
	}
	return projectSummary(id, spec, runtime), nil
}

func defaultProjectConfig(mode string, listenerPort, multiPortBase uint16) *config.Config {
	enabled := false
	return &config.Config{
		Mode:     mode,
		LogLevel: "info",
		Listener: config.ListenerConfig{
			Address: "0.0.0.0",
			Port:    listenerPort,
		},
		MultiPort: config.MultiPortConfig{
			Address:  "0.0.0.0",
			BasePort: multiPortBase,
		},
		Pool: config.PoolConfig{
			Mode:              "sequential",
			FailureThreshold:  3,
			BlacklistDuration: 24 * time.Hour,
			RetryAttempts:     3,
		},
		Management: config.ManagementConfig{Enabled: &enabled},
		Probe: config.ProbeConfig{
			Target:      "http://cp.cloudflare.com/generate_204",
			Interval:    5 * time.Minute,
			Timeout:     config.DefaultProbeTimeout,
			Concurrency: 32,
		},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Interval:           time.Hour,
			Timeout:            30 * time.Second,
			HealthCheckTimeout: 2 * time.Minute,
			DrainTimeout:       30 * time.Second,
			MinAvailableNodes:  1,
			FetchConcurrency:   16,
		},
		SkipCertVerify: true,
		Log:            config.LogConfig{Output: "stdout"},
		NodesFile:      "nodes.txt",
		Nodes:          []config.NodeConfig{},
	}
}

func (r *Registry) UpdateProject(ctx context.Context, id string, request monitor.ProjectUpdateRequest) (monitor.ProjectSummary, error) {
	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()
	r.mu.RLock()
	spec, ok := r.workspace.Projects[id]
	runtime := r.runtimes[id]
	r.mu.RUnlock()
	if !ok || runtime == nil {
		return monitor.ProjectSummary{}, fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	previous := spec
	if request.Name != nil {
		name := strings.TrimSpace(*request.Name)
		if name == "" {
			return monitor.ProjectSummary{}, errors.New("project name cannot be empty")
		}
		spec.Name = name
	}
	if request.Enabled != nil {
		spec.Enabled = *request.Enabled
	}
	if request.Autostart != nil {
		spec.Autostart = *request.Autostart
	}

	var nextCfg, previousCfg *config.Config
	if request.Settings != nil {
		if runtime.sharedMu != nil {
			runtime.sharedMu.RLock()
		}
		cfg, err := config.LoadProjectWithShared(runtime.configPath, runtime.sharedCfg)
		if runtime.sharedMu != nil {
			runtime.sharedMu.RUnlock()
		}
		if err != nil {
			return monitor.ProjectSummary{}, fmt.Errorf("load project settings: %w", err)
		}
		previousCopy := *cfg
		previousCfg = &previousCopy
		previousCfg.ClashAPIPort = runtime.clashAPIPort
		if err := applyRuntimeSettings(cfg, *request.Settings); err != nil {
			return monitor.ProjectSummary{}, err
		}
		cfg.ClashAPIPort = runtime.clashAPIPort
		if err := r.ports.Reserve(id, cfg); err != nil {
			return monitor.ProjectSummary{}, err
		}
		nextCfg = cfg
		if err := nextCfg.SaveSettings(); err != nil {
			rollbackErr := r.ports.Reserve(id, previousCfg)
			return monitor.ProjectSummary{}, errors.Join(fmt.Errorf("save project settings: %w", err), rollbackErr)
		}
	}

	r.mu.Lock()
	r.workspace.Projects[id] = spec
	if err := r.workspace.Save(); err != nil {
		r.workspace.Projects[id] = previous
		r.mu.Unlock()
		if previousCfg != nil {
			restoreSettingsErr := previousCfg.SaveSettings()
			restorePortsErr := r.ports.Reserve(id, previousCfg)
			return monitor.ProjectSummary{}, errors.Join(err, restoreSettingsErr, restorePortsErr)
		}
		return monitor.ProjectSummary{}, err
	}
	r.mu.Unlock()

	if request.Enabled != nil && previous.Enabled != spec.Enabled {
		if !*request.Enabled {
			if err := runtime.Stop(); err != nil {
				return projectSummary(id, spec, runtime), err
			}
		} else if spec.Autostart {
			if err := runtime.Start(); err != nil {
				return projectSummary(id, spec, runtime), err
			}
		}
	} else if request.Settings != nil {
		status, _, _ := runtime.Status()
		if status == StatusRunning {
			if err := runtime.Restart(); err != nil {
				return projectSummary(id, spec, runtime), err
			}
		}
	}
	return projectSummary(id, spec, runtime), nil
}

func applyRuntimeSettings(cfg *config.Config, settings monitor.ProjectRuntimeSettings) error {
	if cfg == nil {
		return errors.New("project config is nil")
	}
	mode := strings.TrimSpace(settings.Mode)
	if mode == "multi_port" {
		mode = "multi-port"
	}
	if mode != "pool" && mode != "multi-port" && mode != "hybrid" {
		return fmt.Errorf("unsupported mode %q", settings.Mode)
	}
	if (mode == "pool" || mode == "hybrid") && settings.Listener.Port == 0 {
		return errors.New("listener port is required in pool and hybrid modes")
	}
	if (mode == "multi-port" || mode == "hybrid") && settings.MultiPort.BasePort == 0 {
		return errors.New("multi-port base port is required in multi-port and hybrid modes")
	}
	blacklist, err := time.ParseDuration(strings.TrimSpace(settings.Pool.BlacklistDuration))
	if err != nil || blacklist <= 0 {
		return errors.New("blacklist duration must be a positive duration")
	}
	probeInterval, err := time.ParseDuration(strings.TrimSpace(settings.Probe.Interval))
	if err != nil || probeInterval <= 0 {
		return errors.New("probe interval must be a positive duration")
	}
	probeTimeout, err := time.ParseDuration(strings.TrimSpace(settings.Probe.Timeout))
	if err != nil {
		return errors.New("probe timeout format is invalid")
	}
	if err := config.ValidateProbeTimeout(probeTimeout); err != nil {
		return err
	}
	subInterval, err := time.ParseDuration(strings.TrimSpace(settings.SubscriptionRefresh.Interval))
	if err != nil || subInterval < 5*time.Minute {
		return errors.New("subscription refresh interval cannot be less than 5 minutes")
	}
	if settings.Probe.Concurrency <= 0 || settings.Probe.Concurrency > 1024 {
		return errors.New("probe concurrency must be between 1 and 1024")
	}
	if settings.Pool.FailureThreshold <= 0 || settings.Pool.RetryAttempts <= 0 {
		return errors.New("pool thresholds must be positive")
	}
	cfg.Mode = mode
	cfg.ExternalIP = strings.TrimSpace(settings.ExternalIP)
	cfg.SkipCertVerify = settings.SkipCertVerify
	cfg.Listener.Address = strings.TrimSpace(settings.Listener.Address)
	cfg.Listener.Port = settings.Listener.Port
	cfg.Listener.Username = settings.Listener.Username
	cfg.Listener.Password = settings.Listener.Password
	cfg.MultiPort.Address = strings.TrimSpace(settings.MultiPort.Address)
	cfg.MultiPort.BasePort = settings.MultiPort.BasePort
	cfg.MultiPort.Username = settings.MultiPort.Username
	cfg.MultiPort.Password = settings.MultiPort.Password
	cfg.Pool.Mode = strings.TrimSpace(settings.Pool.Mode)
	cfg.Pool.FailureThreshold = settings.Pool.FailureThreshold
	cfg.Pool.BlacklistDuration = blacklist
	cfg.Pool.RetryEnabled = &settings.Pool.RetryEnabled
	cfg.Pool.RetryAttempts = settings.Pool.RetryAttempts
	cfg.Sticky.Enabled = settings.Sticky.Enabled
	cfg.Sticky.Port = settings.Sticky.Port
	cfg.Probe.Target = strings.TrimSpace(settings.Probe.Target)
	cfg.Probe.Interval = probeInterval
	cfg.Probe.Timeout = probeTimeout
	cfg.Probe.Concurrency = settings.Probe.Concurrency
	cfg.SubscriptionRefresh.Enabled = settings.SubscriptionRefresh.Enabled
	cfg.SubscriptionRefresh.Interval = subInterval
	if settings.SelectedSubscriptions != nil {
		cfg.SelectedSubscriptions = append([]string(nil), settings.SelectedSubscriptions...)
	}
	return nil
}

// DeleteProject removes a project from the catalog while retaining its files.
// It preserves the original API for callers that do not opt into data deletion.
func (r *Registry) DeleteProject(ctx context.Context, id string) error {
	_, err := r.DeleteProjectWithData(ctx, id, false)
	return err
}

func (r *Registry) DeleteProjectWithData(ctx context.Context, id string, deleteData bool) (monitor.ProjectDeleteResult, error) {
	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()
	result := monitor.ProjectDeleteResult{ProjectID: id, DataRetained: !deleteData}
	r.mu.RLock()
	defaultID := r.workspace.DefaultProject
	runtime, ok := r.runtimes[id]
	r.mu.RUnlock()
	if !ok {
		return result, fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	var dataDir string
	if deleteData {
		expectedConfigPath, err := r.workspace.NewProjectConfigPath(id)
		if err != nil {
			return result, err
		}
		actualConfigPath, err := filepath.Abs(runtime.configPath)
		if err != nil {
			return result, fmt.Errorf("resolve project config path: %w", err)
		}
		expectedConfigPath, err = filepath.Abs(expectedConfigPath)
		if err != nil {
			return result, fmt.Errorf("resolve managed project path: %w", err)
		}
		if filepath.Clean(actualConfigPath) != filepath.Clean(expectedConfigPath) {
			return result, fmt.Errorf("refusing to delete project data outside managed path %q", expectedConfigPath)
		}
		dataDir = filepath.Dir(expectedConfigPath)
		rootDir, err := filepath.Abs(r.workspace.RootDir())
		if err != nil {
			return result, fmt.Errorf("resolve workspace root: %w", err)
		}
		rel, err := filepath.Rel(rootDir, dataDir)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return result, fmt.Errorf("refusing to delete unsafe project directory %q", dataDir)
		}
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
	}
	status, _, _ := runtime.Status()
	wasRunning := status == StatusRunning
	if err := runtime.Stop(); err != nil {
		return result, err
	}

	r.mu.Lock()
	previous := r.workspace.Projects[id]
	previousDefault := r.workspace.DefaultProject
	delete(r.workspace.Projects, id)
	delete(r.runtimes, id)
	if len(r.workspace.Projects) == 0 {
		r.workspace.DefaultProject = ""
	} else if id == defaultID {
		ids := r.workspace.SortedProjectIDs()
		r.workspace.DefaultProject = ids[0]
	}
	if err := r.workspace.Save(); err != nil {
		r.workspace.Projects[id] = previous
		r.runtimes[id] = runtime
		r.workspace.DefaultProject = previousDefault
		r.mu.Unlock()
		var restartErr error
		if wasRunning {
			restartErr = runtime.Start()
		}
		return result, errors.Join(err, restartErr)
	}
	r.mu.Unlock()
	r.ports.Release(id)
	if deleteData {
		if err := os.RemoveAll(dataDir); err != nil {
			result.DataRetained = true
			result.Warning = fmt.Sprintf("project removed from catalog, but local data could not be deleted: %v", err)
			return result, nil
		}
		result.DataDeleted = true
		result.DataRetained = false
	}
	if r.isEmpty() && r.catalog != nil {
		if err := r.catalog.Start(); err != nil {
			result.Warning = strings.TrimSpace(strings.Join([]string{result.Warning, fmt.Sprintf("共享目录运行时启动失败: %v", err)}, " "))
		}
	}
	return result, nil
}

func (r *Registry) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workspace.Projects) == 0
}

func (r *Registry) StartProject(ctx context.Context, id string) error {
	r.mu.RLock()
	spec, ok := r.workspace.Projects[id]
	runtime := r.runtimes[id]
	r.mu.RUnlock()
	if !ok || runtime == nil {
		return fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	if !spec.Enabled {
		return fmt.Errorf("project %q is disabled", id)
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return runtime.Start()
}

func (r *Registry) StopProject(ctx context.Context, id string) error {
	r.mu.RLock()
	runtime, ok := r.runtimes[id]
	r.mu.RUnlock()
	if !ok || runtime == nil {
		return fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return runtime.Stop()
}

func (r *Registry) ReloadProject(ctx context.Context, id string) error {
	r.mu.RLock()
	runtime, ok := r.runtimes[id]
	r.mu.RUnlock()
	if !ok || runtime == nil {
		return fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	return runtime.Reload(ctx)
}

// ReloadSharedSources restarts every running project so it picks up the latest
// standalone shared node and subscription definitions. Project state remains
// isolated in each project's own state database.
func (r *Registry) ReloadSharedSources(ctx context.Context) error {
	if err := r.reloadSharedConfigFromDisk(); err != nil {
		return err
	}
	r.mu.RLock()
	runtimes := make(map[string]*Runtime, len(r.runtimes))
	catalog := r.catalog
	noProjects := len(r.workspace.Projects) == 0
	for id, runtime := range r.runtimes {
		runtimes[id] = runtime
	}
	r.mu.RUnlock()
	if noProjects && catalog != nil {
		if err := catalog.ReloadFromShared(); err != nil {
			return fmt.Errorf("reload shared catalog: %w", err)
		}
	}

	ids := make([]string, 0, len(runtimes))
	for id := range runtimes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var reloadErrors []error
	for _, id := range ids {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return errors.Join(append(reloadErrors, ctx.Err())...)
			default:
			}
		}
		status, _, _ := runtimes[id].Status()
		if status != StatusRunning {
			continue
		}
		if err := runtimes[id].Restart(); err != nil {
			reloadErrors = append(reloadErrors, fmt.Errorf("reload project %q shared sources: %w", id, err))
		}
	}
	return errors.Join(reloadErrors...)
}

// reloadSharedConfigFromDisk refreshes the shared catalog in place so all
// project runtimes and the projectless catalog keep the same pointer while
// subscription refreshes update nodes.txt.
func (r *Registry) reloadSharedConfigFromDisk() error {
	if r.sharedMu != nil {
		r.sharedMu.Lock()
		defer r.sharedMu.Unlock()
	}
	fresh, err := config.LoadShared(r.sharedPath)
	if err != nil {
		return err
	}
	if r.sharedCfg == nil {
		r.sharedCfg = fresh
		return nil
	}
	*r.sharedCfg = *fresh
	return nil
}

func (r *Registry) Close() error {
	r.mu.RLock()
	ids := make([]string, 0, len(r.runtimes))
	runtimes := make(map[string]*Runtime, len(r.runtimes))
	for id, runtime := range r.runtimes {
		ids = append(ids, id)
		runtimes[id] = runtime
	}
	r.mu.RUnlock()
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	var firstErr error
	for _, id := range ids {
		if err := runtimes[id].Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.catalog != nil {
		if err := r.catalog.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
