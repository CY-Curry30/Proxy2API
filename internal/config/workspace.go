package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	WorkspaceFileName    = "projects.yaml"
	SharedConfigFileName = "shared.yaml"
)

var projectIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// ControlManagementConfig contains settings owned by the process-wide control
// plane. Probe settings deliberately remain in each project's Config.
type ControlManagementConfig struct {
	Enabled  *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Listen   string `yaml:"listen" json:"listen"`
	Password string `yaml:"password,omitempty" json:"password,omitempty"`
}

// ProjectSpec is the process-wide catalog entry for one project runtime.
type ProjectSpec struct {
	Name         string `yaml:"name" json:"name"`
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	Autostart    bool   `yaml:"autostart" json:"autostart"`
	Config       string `yaml:"config" json:"-"`
	ClashAPIPort uint16 `yaml:"clash_api_port,omitempty" json:"-"`
}

// Workspace describes the global control plane and project catalog. Shared
// node/subscription definitions live in SharedConfigFileName; the legacy
// -config file is retained as the migration source for existing deployments.
type Workspace struct {
	Management            ControlManagementConfig `yaml:"management" json:"management"`
	Log                   LogConfig               `yaml:"log" json:"log"`
	ProjectsDir           string                  `yaml:"projects_dir" json:"projects_dir"`
	DefaultProject        string                  `yaml:"default_project" json:"default_project"`
	SharedSourcesMigrated bool                    `yaml:"shared_sources_migrated,omitempty" json:"shared_sources_migrated"`
	Projects              map[string]ProjectSpec  `yaml:"projects" json:"projects"`

	filePath         string
	rootDir          string
	legacyConfigPath string
	sharedConfigPath string
	persisted        bool
}

// ValidateProjectID rejects identifiers that could escape or ambiguously map
// to a project directory.
func ValidateProjectID(id string) error {
	if !projectIDPattern.MatchString(id) {
		return errors.New("项目 ID 必须由 1-64 个小写字母、数字、'-' 或 '_' 组成")
	}
	return nil
}

// LoadWorkspace loads projects.yaml next to the legacy config. When it does
// not exist, a compatible in-memory catalog containing only "default" is
// returned and the current config file is left untouched.
func LoadWorkspace(configPath string, legacy *Config) (*Workspace, error) {
	if legacy == nil {
		return nil, errors.New("工作区缺少共享源配置")
	}
	absConfig, err := filepath.Abs(configPath)
	if err != nil {
		return nil, fmt.Errorf("解析共享配置路径失败: %w", err)
	}
	rootDir := filepath.Dir(absConfig)
	manifestPath := filepath.Join(rootDir, WorkspaceFileName)

	w := &Workspace{
		Management: ControlManagementConfig{
			Enabled:  cloneBoolPointer(legacy.Management.Enabled),
			Listen:   legacy.Management.Listen,
			Password: legacy.Management.Password,
		},
		Log:            legacy.Log,
		ProjectsDir:    "projects",
		DefaultProject: "default",
		Projects: map[string]ProjectSpec{
			"default": {
				Name:      "Default",
				Enabled:   true,
				Autostart: true,
				Config:    relativeOrAbsolute(rootDir, absConfig),
			},
		},
		filePath:         manifestPath,
		rootDir:          rootDir,
		legacyConfigPath: absConfig,
		sharedConfigPath: filepath.Join(rootDir, SharedConfigFileName),
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			w.normalize()
			return w, nil
		}
		return nil, fmt.Errorf("读取工作区配置失败: %w", err)
	}
	loaded := &Workspace{
		Management: ControlManagementConfig{
			Enabled:  cloneBoolPointer(legacy.Management.Enabled),
			Listen:   legacy.Management.Listen,
			Password: legacy.Management.Password,
		},
		Log:         legacy.Log,
		ProjectsDir: "projects",
	}
	if err := yaml.Unmarshal(data, loaded); err != nil {
		return nil, fmt.Errorf("解析工作区配置失败: %w", err)
	}
	loaded.filePath = manifestPath
	loaded.rootDir = rootDir
	loaded.legacyConfigPath = absConfig
	loaded.sharedConfigPath = filepath.Join(rootDir, SharedConfigFileName)
	loaded.persisted = true
	// A manifest without a projects field predates the project catalog. An
	// explicit empty map, however, represents the supported no-project state.
	if loaded.Projects == nil {
		loaded.DefaultProject = "default"
		loaded.Projects = map[string]ProjectSpec{
			"default": {
				Name:      "Default",
				Enabled:   true,
				Autostart: true,
				Config:    relativeOrAbsolute(rootDir, absConfig),
			},
		}
	}
	loaded.normalize()
	if err := loaded.Validate(); err != nil {
		return nil, err
	}
	return loaded, nil
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func relativeOrAbsolute(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return rel
	}
	return path
}

func (w *Workspace) normalize() {
	if w.ProjectsDir == "" {
		w.ProjectsDir = "projects"
	}
	w.DefaultProject = strings.TrimSpace(w.DefaultProject)
	if w.Management.Enabled == nil {
		enabled := true
		w.Management.Enabled = &enabled
	}
	if w.Management.Listen == "" {
		w.Management.Listen = "0.0.0.0:9091"
	}
	if w.Log.Output == "" {
		w.Log.Output = "stdout"
	}
	if w.Log.File == "" {
		w.Log.File = filepath.Join(w.rootDir, "logs", "Proxy2API.log")
	} else if !filepath.IsAbs(w.Log.File) {
		w.Log.File = filepath.Join(w.rootDir, w.Log.File)
	}
	if w.Log.MaxSize <= 0 {
		w.Log.MaxSize = 50
	}
	if w.Log.MaxBackups <= 0 {
		w.Log.MaxBackups = 3
	}
	if w.Log.MaxAge <= 0 {
		w.Log.MaxAge = 7
	}
	if w.Projects == nil {
		w.Projects = make(map[string]ProjectSpec)
	}
	if len(w.Projects) == 0 {
		w.DefaultProject = ""
		return
	}
	if _, ok := w.Projects[w.DefaultProject]; !ok {
		ids := w.SortedProjectIDs()
		for _, id := range ids {
			path := strings.TrimSpace(w.Projects[id].Config)
			if path == "" {
				continue
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(w.rootDir, path)
			}
			if filepath.Clean(path) == filepath.Clean(w.legacyConfigPath) {
				w.DefaultProject = id
				return
			}
		}
		if w.legacyConfigPath != "" {
			w.DefaultProject = "default"
			w.Projects[w.DefaultProject] = ProjectSpec{
				Name:      "Default",
				Enabled:   true,
				Autostart: true,
				Config:    relativeOrAbsolute(w.rootDir, w.legacyConfigPath),
			}
			return
		}
		w.DefaultProject = ids[0]
	}
}

func (w *Workspace) Validate() error {
	if w == nil {
		return errors.New("工作区不能为空")
	}
	if _, err := w.NewProjectConfigPath("validation"); err != nil {
		return err
	}
	if len(w.Projects) == 0 {
		if w.DefaultProject != "" {
			return errors.New("项目目录为空时默认项目必须为空")
		}
		return nil
	}
	if err := ValidateProjectID(w.DefaultProject); err != nil {
		return fmt.Errorf("默认项目无效: %w", err)
	}
	if _, ok := w.Projects[w.DefaultProject]; !ok {
		return fmt.Errorf("默认项目 %q 不在项目目录中", w.DefaultProject)
	}
	for id := range w.Projects {
		if err := ValidateProjectID(id); err != nil {
			return fmt.Errorf("项目 %q 无效: %w", id, err)
		}
		if _, err := w.ProjectConfigPath(id); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workspace) FilePath() string { return w.filePath }
func (w *Workspace) RootDir() string  { return w.rootDir }
func (w *Workspace) Persisted() bool  { return w.persisted }

// SharedConfigPath returns the independent shared node/subscription config.
func (w *Workspace) SharedConfigPath() string { return w.sharedConfigPath }

// LegacyConfigPath returns the original -config path used for migration.
func (w *Workspace) LegacyConfigPath() string { return w.legacyConfigPath }

func (w *Workspace) ManagementEnabled() bool {
	return w.Management.Enabled == nil || *w.Management.Enabled
}

// ProjectConfigPath resolves and confines a project's config to the workspace
// root. Config values are stored relative to the manifest whenever possible.
func (w *Workspace) ProjectConfigPath(id string) (string, error) {
	if err := ValidateProjectID(id); err != nil {
		return "", err
	}
	spec, ok := w.Projects[id]
	if !ok {
		return "", fmt.Errorf("项目 %q 不存在", id)
	}
	path := strings.TrimSpace(spec.Config)
	if path == "" {
		return w.NewProjectConfigPath(id)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(w.rootDir, path)
	}
	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("解析项目 %q 的配置路径失败: %w", id, err)
	}
	rel, err := filepath.Rel(w.rootDir, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("项目 %q 的配置必须位于工作区 %q 内", id, w.rootDir)
	}
	return absPath, nil
}

// NewProjectConfigPath resolves the conventional path for a catalog entry
// that does not exist yet, while keeping projects_dir inside the workspace.
func (w *Workspace) NewProjectConfigPath(id string) (string, error) {
	if err := ValidateProjectID(id); err != nil {
		return "", err
	}
	projectsDir := strings.TrimSpace(w.ProjectsDir)
	if projectsDir == "" {
		projectsDir = "projects"
	}
	if filepath.IsAbs(projectsDir) {
		return "", errors.New("projects_dir 必须是相对于工作区的路径")
	}
	absPath, err := filepath.Abs(filepath.Join(w.rootDir, projectsDir, id, "project.yaml"))
	if err != nil {
		return "", fmt.Errorf("解析 projects_dir 失败: %w", err)
	}
	rel, err := filepath.Rel(w.rootDir, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("projects_dir 必须位于工作区 %q 内", w.rootDir)
	}
	return absPath, nil
}

// SortedProjectIDs provides deterministic startup and persistence behavior.
func (w *Workspace) SortedProjectIDs() []string {
	ids := make([]string, 0, len(w.Projects))
	for id := range w.Projects {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Save atomically updates only the workspace manifest. Project configuration
// files are persisted separately by their own runtime.
func (w *Workspace) Save() error {
	if err := w.Validate(); err != nil {
		return err
	}
	saved := *w
	saved.Management.Enabled = cloneBoolPointer(w.Management.Enabled)
	saved.Log.File = relativeOrAbsolute(w.rootDir, w.Log.File)
	saved.Projects = make(map[string]ProjectSpec, len(w.Projects))
	for id, spec := range w.Projects {
		path, err := w.ProjectConfigPath(id)
		if err != nil {
			return err
		}
		spec.Config = relativeOrAbsolute(w.rootDir, path)
		saved.Projects[id] = spec
	}
	data, err := yaml.Marshal(&saved)
	if err != nil {
		return fmt.Errorf("编码工作区配置失败: %w", err)
	}
	if err := writeFileWithLock(w.filePath, data, 0o644); err != nil {
		return fmt.Errorf("写入工作区配置失败: %w", err)
	}
	w.persisted = true
	return nil
}

// WriteProjectConfig creates a new project's initial YAML file.
func WriteProjectConfig(path string, cfg *Config) error {
	if cfg == nil {
		return errors.New("项目配置不能为空")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("创建项目目录失败: %w", err)
	}
	cfg.SetFilePath(path)
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("编码项目配置失败: %w", err)
	}
	if err := writeFileWithLock(path, data, 0o644); err != nil {
		return fmt.Errorf("写入项目配置失败: %w", err)
	}
	if cfg.NodesFile != "" {
		nodesPath := cfg.NodesFile
		if !filepath.IsAbs(nodesPath) {
			nodesPath = filepath.Join(filepath.Dir(path), nodesPath)
		}
		if _, err := os.Stat(nodesPath); os.IsNotExist(err) {
			if err := writeFileWithLock(nodesPath, nil, 0o644); err != nil {
				return fmt.Errorf("创建项目节点文件失败: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("检查项目节点文件失败: %w", err)
		}
	}
	return nil
}

// NewSharedConfig creates an in-memory standalone source catalog from an
// existing config. Runtime and process settings are deliberately omitted.
func NewSharedConfig(path string, source *Config) *Config {
	shared := &Config{filePath: path, sourcesOnly: true}
	if source == nil {
		return shared
	}
	shared.Nodes = append([]NodeConfig(nil), source.Nodes...)
	for idx := range shared.Nodes {
		shared.Nodes[idx].Port = 0
		shared.Nodes[idx].Username = ""
		shared.Nodes[idx].Password = ""
	}
	shared.NodesFile = source.NodesFile
	shared.Subscriptions = append([]string(nil), source.Subscriptions...)
	shared.DisabledSubscriptions = append([]string(nil), source.DisabledSubscriptions...)
	return shared
}

// WriteSharedConfig persists a standalone source catalog.
func WriteSharedConfig(path string, source *Config) error {
	shared := NewSharedConfig(path, source)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("创建共享配置目录失败: %w", err)
	}
	if err := shared.SaveSettings(); err != nil {
		return err
	}
	return shared.SaveNodes()
}

// WriteRuntimeProjectConfig writes only project-owned settings from a legacy
// config, leaving nodes and subscriptions in the standalone shared catalog.
func WriteRuntimeProjectConfig(path string, source *Config) error {
	if source == nil {
		return errors.New("运行时项目配置不能为空")
	}
	runtime := *source
	runtime.Nodes = nil
	runtime.NodesFile = ""
	runtime.Subscriptions = nil
	runtime.DisabledSubscriptions = nil
	runtime.filePath = ""
	runtime.sourcesShared = false
	runtime.sourcesOnly = false
	runtime.skipRuntimeRecovery = false
	return WriteProjectConfig(path, &runtime)
}
