package project

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/state"

	"gopkg.in/yaml.v3"
)

// ExportBackup builds the portable backup document for one project or for the
// whole workspace. A project scoped export still carries the shared sources the
// project depends on, because nodes and subscriptions live in the shared
// catalog rather than inside the project.
func (r *Registry) ExportBackup(ctx context.Context, projectID string, all bool) (monitor.BackupBundle, error) {
	bundle := monitor.BackupBundle{
		Format:     monitor.BackupFormat,
		Version:    monitor.BackupVersion,
		ExportedAt: time.Now().UTC(),
	}
	r.sharedMu.RLock()
	sharedCfg := r.sharedCfg
	r.sharedMu.RUnlock()
	if sharedCfg == nil {
		return bundle, errors.New("共享源配置未初始化")
	}

	r.mu.RLock()
	workspace := r.workspace
	runtimes := make(map[string]*Runtime, len(r.runtimes))
	for id, runtime := range r.runtimes {
		runtimes[id] = runtime
	}
	r.mu.RUnlock()

	var ids []string
	if all {
		bundle.Scope = monitor.BackupScopeWorkspace
		bundle.Workspace = &monitor.BackupWorkspace{
			DefaultProject: workspace.DefaultProject,
			Settings: monitor.SystemSettings{
				Management:  workspace.Management,
				Proxy:       workspace.Proxy,
				LatencyTest: workspace.LatencyTest,
				Log:         workspace.Log,
			},
		}
		shared, err := exportSharedCatalog(sharedCfg)
		if err != nil {
			return bundle, err
		}
		bundle.Shared = shared
		ids = workspace.SortedProjectIDs()
	} else {
		if projectID == "" {
			projectID = workspace.DefaultProject
		}
		if _, ok := workspace.Projects[projectID]; !ok {
			return bundle, fmt.Errorf("%w: %s", ErrProjectNotFound, projectID)
		}
		bundle.Scope = monitor.BackupScopeProject
		ids = []string{projectID}
	}

	for _, id := range ids {
		if err := ctxErr(ctx); err != nil {
			return bundle, err
		}
		project, effective, err := r.exportProject(id, workspace, runtimes[id], sharedCfg, !all)
		if err != nil {
			return bundle, fmt.Errorf("导出项目 %q 失败: %w", id, err)
		}
		bundle.Projects = append(bundle.Projects, project)
		if !all {
			bundle.Shared = exportProjectSources(effective)
		}
	}
	return bundle, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func exportSharedCatalog(sharedCfg *config.Config) (*monitor.BackupShared, error) {
	shared := &monitor.BackupShared{
		Subscriptions:             append([]string(nil), sharedCfg.Subscriptions...),
		DisabledSubscriptions:     append([]string(nil), sharedCfg.DisabledSubscriptions...),
		AutoDisabledSubscriptions: append([]string(nil), sharedCfg.AutoDisabledSubscriptions...),
	}
	if path := sharedCfg.FilePath(); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			shared.ConfigYAML = string(data)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("读取共享配置失败: %w", err)
		}
	}
	for _, node := range sharedCfg.Nodes {
		if strings.TrimSpace(node.URI) == "" {
			continue
		}
		shared.Nodes = append(shared.Nodes, node.URI)
	}
	if path := sharedCfg.FilePath(); path != "" {
		if snapshot, err := state.LoadBackupSnapshot(path); err == nil {
			shared.Subscription = snapshot.Subscription
		} else {
			log.Printf("[备份] 读取共享目录状态失败: %v", err)
		}
		shared.SubscriptionCache = readOptionalText(filepath.Join(filepath.Dir(path), config.SubscriptionCacheFileName))
	}
	return shared, nil
}

// exportProjectSources narrows the shared catalog down to the sources one
// project actually uses, so a project scoped backup stays restorable on its own.
func exportProjectSources(effective *config.Config) *monitor.BackupShared {
	shared := &monitor.BackupShared{}
	if effective == nil {
		return shared
	}
	shared.Subscriptions = append([]string(nil), effective.Subscriptions...)
	for _, node := range effective.Nodes {
		if node.Source == config.NodeSourceSubscription {
			continue
		}
		if strings.TrimSpace(node.URI) == "" {
			continue
		}
		shared.Nodes = append(shared.Nodes, node.URI)
	}
	shared.Nodes = dedupeStrings(shared.Nodes)
	return shared
}

func (r *Registry) exportProject(id string, workspace *config.Workspace, runtime *Runtime, sharedCfg *config.Config, includeInheritedNodes bool) (monitor.BackupProject, *config.Config, error) {
	spec, ok := workspace.Projects[id]
	if !ok {
		return monitor.BackupProject{}, nil, fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	project := monitor.BackupProject{
		ID:        id,
		Name:      spec.Name,
		Enabled:   spec.Enabled,
		Autostart: spec.Autostart,
		Running:   spec.DesiredRunning(),
		Cooldown:  spec.ManualCooling(),
	}
	configPath, err := workspace.ProjectConfigPath(id)
	if err != nil {
		return project, nil, err
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return project, nil, err
		}
	} else {
		project.ConfigYAML = string(data)
	}

	declaresOwnSources := projectDeclaresOwnSources(data)
	// A running project owns the state database, so loading its configuration
	// from disk (which replays the recovery snapshot) would time out. The live
	// runtime config is the same effective view — shared nodes and subscriptions
	// already merged — without touching the locked database.
	effective := liveProjectConfig(runtime)
	if effective == nil {
		loaded, loadErr := config.LoadProjectReadOnlyWithShared(configPath, sharedCfg)
		if loadErr != nil {
			log.Printf("[备份] 加载项目 %q 的有效配置失败: %v", id, loadErr)
			effective = liveProjectConfig(runtime)
		} else {
			effective = loaded
		}
	}
	if declaresOwnSources {
		project.Nodes = readProjectNodeFile(configPath, effective)
	} else if includeInheritedNodes && effective != nil {
		for _, node := range effective.Nodes {
			if node.Source == config.NodeSourceSubscription || strings.TrimSpace(node.URI) == "" {
				continue
			}
			project.Nodes = append(project.Nodes, node.URI)
		}
		project.Nodes = dedupeStrings(project.Nodes)
	}
	if ports, err := readNodePortMap(filepath.Join(filepath.Dir(configPath), "node_ports.json")); err == nil {
		project.NodePorts = ports
	}
	project.SubscriptionCache = readOptionalText(filepath.Join(filepath.Dir(configPath), config.SubscriptionCacheFileName))
	snapshot, err := projectStateSnapshot(runtime, configPath)
	if err != nil {
		log.Printf("[备份] 读取项目 %q 的状态失败: %v", id, err)
	} else {
		project.NodeStates = snapshot.Nodes
		project.Traffic = snapshot.Traffic
		project.Subscription = snapshot.Subscription
	}
	return project, effective, nil
}

// liveProjectConfig returns a running project's effective configuration, or nil
// when the project is not running.
func liveProjectConfig(runtime *Runtime) *config.Config {
	if runtime == nil {
		return nil
	}
	status, _, _ := runtime.Status()
	if status != StatusRunning {
		return nil
	}
	return runtime.Config()
}

// projectDeclaresOwnSources reports whether the project's own YAML file defines
// nodes or subscriptions. A project that defines neither is a pure consumer of
// the shared catalog, so its nodes must travel in the shared block instead.
func projectDeclaresOwnSources(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	var probe struct {
		Nodes         []config.NodeConfig `yaml:"nodes"`
		Subscriptions []string            `yaml:"subscriptions"`
	}
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return false
	}
	return len(probe.Nodes) > 0 || len(probe.Subscriptions) > 0
}

func readProjectNodeFile(configPath string, effective *config.Config) []string {
	dir := filepath.Dir(configPath)
	candidates := make([]string, 0, 2)
	if effective != nil {
		if name := strings.TrimSpace(effective.NodesFile); name != "" {
			if !filepath.IsAbs(name) {
				name = filepath.Join(dir, name)
			}
			candidates = append(candidates, name)
		}
	}
	candidates = append(candidates, filepath.Join(dir, "nodes.txt"))
	seen := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		if seen[candidate] {
			continue
		}
		seen[candidate] = true
		lines, err := readNodeLines(candidate)
		if err == nil && len(lines) > 0 {
			return lines
		}
	}
	return nil
}

func projectStateSnapshot(runtime *Runtime, configPath string) (state.BackupSnapshot, error) {
	if runtime != nil {
		runtime.mu.RLock()
		store := runtime.stateStore
		status := runtime.status
		runtime.mu.RUnlock()
		if store != nil && status == StatusRunning {
			return store.ExportSnapshot()
		}
	}
	return state.LoadBackupSnapshot(configPath)
}

// ImportBackup restores a bundle. Shared sources are merged additively so an
// import can never delete subscriptions or nodes the operator added later;
// project configuration, node list, port map and state are replaced.
func (r *Registry) ImportBackup(ctx context.Context, bundle monitor.BackupBundle, mode string) (monitor.BackupImportResult, error) {
	result := monitor.BackupImportResult{
		Created:   []string{},
		Updated:   []string{},
		Skipped:   []string{},
		Failed:    []string{},
		Started:   []string{},
		Restarted: []string{},
		Warnings:  []string{},
	}
	if err := validateBackupBundle(bundle); err != nil {
		return result, err
	}
	mode, err := normalizeImportMode(mode)
	if err != nil {
		return result, err
	}
	if len(bundle.Projects) == 0 && bundle.Shared == nil && bundle.Workspace == nil {
		return result, errors.New("备份文件中没有任何可导入的数据")
	}

	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()

	if bundle.Shared != nil {
		changed, warnings, err := r.importSharedBackup(*bundle.Shared, bundle.Scope == monitor.BackupScopeWorkspace)
		result.Warnings = append(result.Warnings, warnings...)
		if err != nil {
			return result, err
		}
		result.SharedUpdated = changed
	}
	if bundle.Workspace != nil {
		if err := r.importWorkspaceSettings(ctx, *bundle.Workspace); err != nil {
			result.Warnings = append(result.Warnings, "工作区设置未导入: "+err.Error())
		} else {
			result.WorkspaceUpdated = true
		}
	}
	for _, project := range bundle.Projects {
		if err := ctxErr(ctx); err != nil {
			return result, err
		}
		r.importOneProject(ctx, project, mode, &result)
	}
	if bundle.Workspace != nil && bundle.Workspace.DefaultProject != "" {
		r.applyImportedDefaultProject(bundle.Workspace.DefaultProject)
	}
	return result, nil
}

func validateBackupBundle(bundle monitor.BackupBundle) error {
	if bundle.Format != "" && bundle.Format != monitor.BackupFormat {
		return fmt.Errorf("不是 Proxy2API 备份文件（format=%q）", bundle.Format)
	}
	if bundle.Version > monitor.BackupVersion {
		return fmt.Errorf("备份版本 %d 高于当前支持的版本 %d，请升级后再导入", bundle.Version, monitor.BackupVersion)
	}
	return nil
}

func normalizeImportMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", monitor.BackupImportOverwrite:
		return monitor.BackupImportOverwrite, nil
	case monitor.BackupImportSkip:
		return monitor.BackupImportSkip, nil
	default:
		return "", fmt.Errorf("导入模式 %q 无效，请使用 overwrite 或 skip", mode)
	}
}

func (r *Registry) importOneProject(ctx context.Context, project monitor.BackupProject, mode string, result *monitor.BackupImportResult) {
	id := strings.TrimSpace(project.ID)
	if err := config.ValidateProjectID(id); err != nil {
		result.Failed = append(result.Failed, fmt.Sprintf("%s: %v", id, err))
		return
	}
	r.mu.RLock()
	_, exists := r.workspace.Projects[id]
	runtime := r.runtimes[id]
	r.mu.RUnlock()
	if exists {
		if mode == monitor.BackupImportSkip {
			result.Skipped = append(result.Skipped, id)
			return
		}
		wasRunning := false
		if runtime != nil {
			status, _, _ := runtime.Status()
			wasRunning = status == StatusRunning
		}
		if err := r.overwriteProjectFromBackup(ctx, id, runtime, project, result); err != nil {
			result.Failed = append(result.Failed, fmt.Sprintf("%s: %v", id, err))
			return
		}
		result.Updated = append(result.Updated, id)
		if wasRunning && containsString(result.Started, id) {
			result.Restarted = append(result.Restarted, id)
		}
		return
	}
	if err := r.createProjectFromBackup(ctx, project, result); err != nil {
		result.Failed = append(result.Failed, fmt.Sprintf("%s: %v", id, err))
		return
	}
	result.Created = append(result.Created, id)
}

func (r *Registry) createProjectFromBackup(ctx context.Context, project monitor.BackupProject, result *monitor.BackupImportResult) error {
	id := strings.TrimSpace(project.ID)
	cfg, err := decodeBackupProjectConfig(project)
	if err != nil {
		return err
	}
	configPath, err := r.workspace.NewProjectConfigPath(id)
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(configPath); statErr == nil {
		return fmt.Errorf("项目目录中已存在 %s，请先删除该项目或改用覆盖模式", configPath)
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	clashAPIPort, err := r.ports.NextAvailable(9092)
	if err != nil {
		return err
	}
	cfg.ClashAPIPort = clashAPIPort
	shifted, err := r.reserveProjectPorts(id, cfg)
	if err != nil {
		r.ports.Release(id)
		return err
	}
	if shifted {
		result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 的监听端口与原环境冲突，已重新分配", id))
	}
	if err := config.WriteProjectConfig(configPath, cfg); err != nil {
		r.ports.Release(id)
		return err
	}
	if err := writeBackupProjectArtifacts(configPath, cfg, project); err != nil {
		r.ports.Release(id)
		return err
	}
	if err := importBackupProjectState(configPath, project, result); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 的状态数据未导入: %v", id, err))
	}

	name := strings.TrimSpace(project.Name)
	if name == "" {
		name = id
	}
	running := project.Running
	cooldown := project.Cooldown
	spec := config.ProjectSpec{
		Name:           name,
		Enabled:        project.Enabled,
		Autostart:      project.Autostart,
		Running:        &running,
		ManualCooldown: &cooldown,
		Config:         filepath.Join(r.workspace.ProjectsDir, id, "project.yaml"),
		ClashAPIPort:   clashAPIPort,
	}
	runtime := NewRuntime(r.parentCtx, id, configPath, r.sharedPath, r.sharedCfg, r.sharedMu, clashAPIPort, r.ports,
		WithFetchCoordinator(r.fetchCoordinator),
		WithProbeCoordinator(r.probeCoordinator),
		WithSystemProxyURL(r.systemProxyURL),
		WithGlobalProbeTarget(r.probeTargetOverride),
	)
	runtime.SetManualCooldown(spec.ManualCooling())

	r.mu.Lock()
	previousDefault := r.workspace.DefaultProject
	firstProject := len(r.workspace.Projects) == 0
	r.workspace.Projects[id] = spec
	if firstProject {
		r.workspace.DefaultProject = id
	}
	r.runtimes[id] = runtime
	saveErr := r.workspace.Save()
	if saveErr != nil {
		delete(r.workspace.Projects, id)
		delete(r.runtimes, id)
		r.workspace.DefaultProject = previousDefault
	}
	r.mu.Unlock()
	if saveErr != nil {
		r.ports.Release(id)
		return saveErr
	}
	if project.Running && project.Enabled {
		if err := runtime.Start(); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 已导入但启动失败: %v", id, err))
		} else {
			result.Started = append(result.Started, id)
		}
	}
	return nil
}

func (r *Registry) overwriteProjectFromBackup(ctx context.Context, id string, runtime *Runtime, project monitor.BackupProject, result *monitor.BackupImportResult) error {
	cfg, err := decodeBackupProjectConfig(project)
	if err != nil {
		return err
	}
	configPath, err := r.workspace.ProjectConfigPath(id)
	if err != nil {
		return err
	}
	r.mu.RLock()
	spec := r.workspace.Projects[id]
	r.mu.RUnlock()
	// The state database and the port reservations belong to the running
	// runtime, so the project has to be stopped before its files are replaced.
	if runtime != nil {
		if err := runtime.Stop(); err != nil {
			return fmt.Errorf("停止项目失败: %w", err)
		}
	}
	cfg.ClashAPIPort = spec.ClashAPIPort
	shifted, err := r.reserveProjectPorts(id, cfg)
	if err != nil {
		return err
	}
	if shifted {
		result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 的监听端口与原环境冲突，已重新分配", id))
	}
	if err := config.WriteProjectConfig(configPath, cfg); err != nil {
		return err
	}
	if err := writeBackupProjectArtifacts(configPath, cfg, project); err != nil {
		return err
	}
	if err := importBackupProjectState(configPath, project, result); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 的状态数据未导入: %v", id, err))
	}

	name := strings.TrimSpace(project.Name)
	if name == "" {
		name = id
	}
	running := project.Running
	cooldown := project.Cooldown
	r.mu.Lock()
	spec = r.workspace.Projects[id]
	spec.Name = name
	spec.Enabled = project.Enabled
	spec.Autostart = project.Autostart
	spec.Running = &running
	spec.ManualCooldown = &cooldown
	spec.ClashAPIPort = cfg.ClashAPIPort
	r.workspace.Projects[id] = spec
	saveErr := r.workspace.Save()
	r.mu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	if runtime != nil {
		runtime.SetManualCooldown(project.Cooldown)
	}
	if project.Running && project.Enabled && runtime != nil {
		if err := runtime.Start(); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("项目 %s 已导入但启动失败: %v", id, err))
		} else {
			result.Started = append(result.Started, id)
		}
	}
	return nil
}

func decodeBackupProjectConfig(project monitor.BackupProject) (*config.Config, error) {
	raw := strings.TrimSpace(project.ConfigYAML)
	if raw == "" {
		return nil, errors.New("备份中缺少项目配置")
	}
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, fmt.Errorf("解析项目配置失败: %w", err)
	}
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = "hybrid"
	}
	if cfg.Mode == "multi_port" {
		cfg.Mode = "multi-port"
	}
	if cfg.Mode != "pool" && cfg.Mode != "multi-port" && cfg.Mode != "hybrid" {
		return nil, fmt.Errorf("不支持的运行模式 %q", cfg.Mode)
	}
	// An absolute log path recorded on the source machine would point outside
	// this workspace, so drop it and let the project default apply.
	if filepath.IsAbs(cfg.Log.File) {
		cfg.Log.File = ""
	}
	return &cfg, nil
}

// reserveProjectPorts claims the imported listeners, moving them to free ports
// when the original ones are already owned by another project on this machine.
func (r *Registry) reserveProjectPorts(id string, cfg *config.Config) (bool, error) {
	if err := r.ports.Reserve(id, cfg); err == nil {
		return false, nil
	}
	_, recommendations := r.ports.CreationHints()
	if cfg.Mode == "pool" || cfg.Mode == "hybrid" {
		if recommendations.ListenerPort > 0 {
			cfg.Listener.Port = recommendations.ListenerPort
		}
	}
	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		if recommendations.MultiPortBase > 0 {
			cfg.MultiPort.BasePort = recommendations.MultiPortBase
		}
	}
	if cfg.Sticky.Enabled && recommendations.StickyPort > 0 {
		cfg.Sticky.Port = recommendations.StickyPort
	}
	if err := r.ports.Reserve(id, cfg); err != nil {
		return true, fmt.Errorf("导入的端口无法占用: %w", err)
	}
	return true, nil
}

func writeBackupProjectArtifacts(configPath string, cfg *config.Config, project monitor.BackupProject) error {
	dir := filepath.Dir(configPath)
	if len(project.Nodes) > 0 {
		nodesPath := strings.TrimSpace(cfg.NodesFile)
		if nodesPath == "" {
			nodesPath = "nodes.txt"
		}
		if !filepath.IsAbs(nodesPath) {
			nodesPath = filepath.Join(dir, nodesPath)
		}
		if err := os.MkdirAll(filepath.Dir(nodesPath), 0o755); err != nil {
			return fmt.Errorf("创建节点目录失败: %w", err)
		}
		if err := writeNodeLines(nodesPath, project.Nodes); err != nil {
			return err
		}
	}
	if len(project.NodePorts) > 0 {
		data, err := json.MarshalIndent(project.NodePorts, "", "  ")
		if err != nil {
			return fmt.Errorf("编码节点端口映射失败: %w", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "node_ports.json"), data, 0o644); err != nil {
			return fmt.Errorf("写入节点端口映射失败: %w", err)
		}
	}
	if strings.TrimSpace(project.SubscriptionCache) != "" {
		path := filepath.Join(dir, config.SubscriptionCacheFileName)
		if err := os.WriteFile(path, []byte(project.SubscriptionCache), 0o644); err != nil {
			return fmt.Errorf("写入订阅节点缓存失败: %w", err)
		}
	}
	return nil
}

func importBackupProjectState(configPath string, project monitor.BackupProject, result *monitor.BackupImportResult) error {
	snapshot := state.BackupSnapshot{
		Nodes:        project.NodeStates,
		Traffic:      project.Traffic,
		Subscription: project.Subscription,
	}
	if snapshot.Empty() {
		return nil
	}
	if err := state.ImportBackup(configPath, snapshot); err != nil {
		return err
	}
	result.NodesImported += len(snapshot.Nodes)
	result.TrafficDays += len(snapshot.Traffic)
	return nil
}

// importSharedBackup merges the shared node and subscription catalog. Existing
// entries are never removed: a backup is a source of missing data, not an
// authority over what the operator added afterwards.
func (r *Registry) importSharedBackup(shared monitor.BackupShared, full bool) (bool, []string, error) {
	warnings := make([]string, 0, 1)
	changed := false

	r.sharedMu.Lock()
	cfg := r.sharedCfg
	if cfg == nil {
		r.sharedMu.Unlock()
		return false, warnings, errors.New("共享源配置未初始化")
	}
	for _, rawURL := range shared.Subscriptions {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" || containsString(cfg.Subscriptions, rawURL) {
			continue
		}
		cfg.Subscriptions = append(cfg.Subscriptions, rawURL)
		changed = true
	}
	if full {
		for _, rawURL := range shared.DisabledSubscriptions {
			rawURL = strings.TrimSpace(rawURL)
			if rawURL == "" || !containsString(cfg.Subscriptions, rawURL) || containsString(cfg.DisabledSubscriptions, rawURL) {
				continue
			}
			cfg.DisabledSubscriptions = append(cfg.DisabledSubscriptions, rawURL)
			changed = true
		}
		for _, rawURL := range shared.AutoDisabledSubscriptions {
			rawURL = strings.TrimSpace(rawURL)
			if rawURL == "" || !containsString(cfg.Subscriptions, rawURL) || containsString(cfg.AutoDisabledSubscriptions, rawURL) {
				continue
			}
			cfg.AutoDisabledSubscriptions = append(cfg.AutoDisabledSubscriptions, rawURL)
			changed = true
		}
	}
	if changed {
		if err := cfg.SaveSettings(); err != nil {
			r.sharedMu.Unlock()
			return false, warnings, fmt.Errorf("保存共享订阅失败: %w", err)
		}
	}
	sharedPath := sharedNodeFilePath(cfg)
	sharedConfigPath := cfg.FilePath()
	r.sharedMu.Unlock()

	nodesAdded, err := mergeSharedNodeFile(sharedPath, shared.Nodes)
	if err != nil {
		return changed, warnings, err
	}
	changed = changed || nodesAdded > 0
	// The shared subscription cache is only restored by a full workspace
	// backup: a single-project import must not rewrite the global cache.
	if full && strings.TrimSpace(shared.SubscriptionCache) != "" && sharedConfigPath != "" {
		path := filepath.Join(filepath.Dir(sharedConfigPath), config.SubscriptionCacheFileName)
		if err := os.WriteFile(path, []byte(shared.SubscriptionCache), 0o644); err != nil {
			warnings = append(warnings, "共享订阅缓存写入失败: "+err.Error())
		}
	}
	if changed {
		if err := r.ReloadSharedSources(context.Background()); err != nil {
			warnings = append(warnings, "共享源已写入但重载失败: "+err.Error())
		}
	}
	return changed, warnings, nil
}

func sharedNodeFilePath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	name := strings.TrimSpace(cfg.NodesFile)
	if name == "" {
		name = "nodes.txt"
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(filepath.Dir(cfg.FilePath()), name)
	}
	return name
}

func mergeSharedNodeFile(path string, nodes []string) (int, error) {
	if path == "" || len(nodes) == 0 {
		return 0, nil
	}
	existing, err := readNodeLines(path)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("读取共享节点文件失败: %w", err)
	}
	seen := make(map[string]bool, len(existing))
	for _, uri := range existing {
		seen[uri] = true
	}
	added := 0
	merged := append([]string(nil), existing...)
	for _, uri := range nodes {
		uri = strings.TrimSpace(uri)
		if uri == "" || seen[uri] {
			continue
		}
		seen[uri] = true
		merged = append(merged, uri)
		added++
	}
	if added == 0 {
		return 0, nil
	}
	if err := writeNodeLines(path, merged); err != nil {
		return 0, err
	}
	return added, nil
}

func (r *Registry) importWorkspaceSettings(ctx context.Context, workspace monitor.BackupWorkspace) error {
	settings := workspace.Settings
	if strings.TrimSpace(settings.Management.Listen) == "" {
		settings.Management.Listen = r.workspace.Management.Listen
	}
	if settings.Log.Output == "" {
		settings.Log.Output = "stdout"
	}
	return r.applySystemSettingsLocked(ctx, settings)
}

func (r *Registry) applyImportedDefaultProject(defaultProject string) {
	defaultProject = strings.TrimSpace(defaultProject)
	if defaultProject == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.workspace.Projects[defaultProject]; !ok {
		return
	}
	if r.workspace.DefaultProject == defaultProject {
		return
	}
	r.workspace.DefaultProject = defaultProject
	if err := r.workspace.Save(); err != nil {
		log.Printf("[备份] 保存默认项目失败: %v", err)
	}
}

func readNodeLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := make([]string, 0, 32)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func writeNodeLines(path string, lines []string) error {
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("创建节点目录失败: %w", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("写入节点文件 %q 失败: %w", path, err)
	}
	return nil
}

// readOptionalText returns a file's content, or an empty string when it does
// not exist or cannot be read. Backup extras are best-effort by design.
func readOptionalText(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func readNodePortMap(path string) (map[string]uint16, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ports map[string]uint16
	if err := json.Unmarshal(data, &ports); err != nil {
		return nil, err
	}
	return ports, nil
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
