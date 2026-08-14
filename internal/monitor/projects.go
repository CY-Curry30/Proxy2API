package monitor

import (
	"context"
	"sync"
	"time"

	"Proxy2API/internal/config"
)

// ProjectSummary is the control-plane view of one isolated runtime.
type ProjectSummary struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	Enabled           bool                   `json:"enabled"`
	Autostart         bool                   `json:"autostart"`
	Status            string                 `json:"status"`
	LastError         string                 `json:"last_error,omitempty"`
	StartedAt         time.Time              `json:"started_at,omitempty"`
	ConfigPath        string                 `json:"config_path"`
	Mode              string                 `json:"mode,omitempty"`
	ListenerAddress   string                 `json:"listener_address,omitempty"`
	ListenerPort      uint16                 `json:"listener_port,omitempty"`
	MultiPortAddress  string                 `json:"multi_port_address,omitempty"`
	MultiPortBase     uint16                 `json:"multi_port_base,omitempty"`
	ClashAPIPort      uint16                 `json:"clash_api_port,omitempty"`
	NodeCount         int                    `json:"node_count"`
	SubscriptionCount int                    `json:"subscription_count"`
	Settings          ProjectRuntimeSettings `json:"settings"`
}

type ProjectRuntimeSettings struct {
	Mode                  string                      `json:"mode"`
	ExternalIP            string                      `json:"external_ip"`
	SkipCertVerify        bool                        `json:"skip_cert_verify"`
	Listener              ProjectListenerSettings     `json:"listener"`
	MultiPort             ProjectMultiPortSettings    `json:"multi_port"`
	Pool                  ProjectPoolSettings         `json:"pool"`
	Sticky                ProjectStickySettings       `json:"sticky"`
	Probe                 ProjectProbeSettings        `json:"probe"`
	SubscriptionRefresh   ProjectSubscriptionSettings `json:"subscription_refresh"`
	SelectedSubscriptions []string                    `json:"selected_subscriptions,omitempty"`
}

type ProjectListenerSettings struct {
	Address  string `json:"address"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ProjectMultiPortSettings struct {
	Address  string `json:"address"`
	BasePort uint16 `json:"base_port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ProjectPoolSettings struct {
	Mode              string `json:"mode"`
	FailureThreshold  int    `json:"failure_threshold"`
	BlacklistDuration string `json:"blacklist_duration"`
	RetryEnabled      bool   `json:"retry_enabled"`
	RetryAttempts     int    `json:"retry_attempts"`
}

type ProjectStickySettings struct {
	Enabled bool   `json:"enabled"`
	Port    uint16 `json:"port"`
}

type ProjectProbeSettings struct {
	Target      string `json:"target"`
	Interval    string `json:"interval"`
	Timeout     string `json:"timeout"`
	Concurrency int    `json:"concurrency"`
}

type ProjectSubscriptionSettings struct {
	Enabled  bool   `json:"enabled"`
	Interval string `json:"interval"`
}

type ProjectCreateRequest struct {
	ID            string                  `json:"id"`
	Name          string                  `json:"name"`
	Mode          string                  `json:"mode"`
	ListenerPort  uint16                  `json:"listener_port"`
	MultiPortBase uint16                  `json:"multi_port_base"`
	Enabled       *bool                   `json:"enabled,omitempty"`
	Autostart     *bool                   `json:"autostart,omitempty"`
	Settings      *ProjectRuntimeSettings `json:"settings,omitempty"`
}

type ProjectUpdateRequest struct {
	Name      *string                 `json:"name,omitempty"`
	Enabled   *bool                   `json:"enabled,omitempty"`
	Autostart *bool                   `json:"autostart,omitempty"`
	Settings  *ProjectRuntimeSettings `json:"settings,omitempty"`
}

type ProjectDeleteResult struct {
	ProjectID    string `json:"project_id"`
	DataDeleted  bool   `json:"data_deleted"`
	DataRetained bool   `json:"data_retained"`
	Warning      string `json:"warning,omitempty"`
}

type SystemSettings struct {
	Management config.ControlManagementConfig `json:"management"`
	Log        config.LogConfig               `json:"log"`
}

// ProjectBinding contains the per-project dependencies used by the existing
// monitoring handlers. A binding is resolved for every request so reloads can
// replace a project's live config without leaving stale pointers in the server.
type ProjectBinding struct {
	ID                    string
	Name                  string
	CatalogOnly           bool
	Config                *config.Config
	SharedConfig          *config.Config
	SharedConfigMu        *sync.RWMutex
	Monitor               *Manager
	NodeManager           NodeManager
	SubscriptionRefresher SubscriptionRefresher
	LogBuffer             *LogBuffer
}

// ProjectController is implemented by the project registry and consumed by
// the process-wide management server.
type ProjectController interface {
	DefaultProjectID() string
	ListProjects() []ProjectSummary
	Project(id string) (ProjectBinding, error)
	SharedCatalog() (ProjectBinding, error)
	CreateProject(ctx context.Context, request ProjectCreateRequest) (ProjectSummary, error)
	UpdateProject(ctx context.Context, id string, request ProjectUpdateRequest) (ProjectSummary, error)
	DeleteProjectWithData(ctx context.Context, id string, deleteData bool) (ProjectDeleteResult, error)
	StartProject(ctx context.Context, id string) error
	StopProject(ctx context.Context, id string) error
	ReloadProject(ctx context.Context, id string) error
	ReloadSharedSources(ctx context.Context) error
	SystemSettings() SystemSettings
	UpdateSystemSettings(ctx context.Context, settings SystemSettings) error
}
