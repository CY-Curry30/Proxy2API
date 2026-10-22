package monitor

import (
	"time"

	"Proxy2API/internal/state"
)

// Backup format identifiers. The version is bumped whenever the on-wire shape
// changes in a way older builds cannot read.
const (
	BackupFormat  = "proxy2api-backup"
	BackupVersion = 1

	// BackupScopeProject exports the project the request resolved to.
	BackupScopeProject = "project"
	// BackupScopeWorkspace exports the workspace settings, the shared node and
	// subscription catalog, and every project.
	BackupScopeWorkspace = "workspace"

	// BackupImportOverwrite replaces the configuration, node list, port map and
	// state of a project that already exists.
	BackupImportOverwrite = "overwrite"
	// BackupImportSkip leaves existing projects untouched.
	BackupImportSkip = "skip"
)

// BackupBundle is the portable document produced by the export endpoints and
// consumed by the import endpoint. It deliberately carries the project
// configuration as YAML text so a backup round-trips every field, including
// ones a newer build may have added.
type BackupBundle struct {
	Format     string           `json:"format"`
	Version    int              `json:"version"`
	ExportedAt time.Time        `json:"exported_at"`
	Scope      string           `json:"scope"`
	Workspace  *BackupWorkspace `json:"workspace,omitempty"`
	Shared     *BackupShared    `json:"shared,omitempty"`
	Projects   []BackupProject  `json:"projects"`
}

// BackupWorkspace carries the workspace-level settings stored in projects.yaml.
type BackupWorkspace struct {
	DefaultProject string         `json:"default_project"`
	Settings       SystemSettings `json:"settings"`
}

// BackupShared carries the shared node/subscription catalog. For a project
// scoped export it holds only the sources that project depends on.
type BackupShared struct {
	ConfigYAML                string   `json:"config_yaml,omitempty"`
	Nodes                     []string `json:"nodes,omitempty"`
	Subscriptions             []string `json:"subscriptions,omitempty"`
	DisabledSubscriptions     []string `json:"disabled_subscriptions,omitempty"`
	AutoDisabledSubscriptions []string `json:"auto_disabled_subscriptions,omitempty"`
	// SubscriptionCache is the raw .subscription-cache.json content of the
	// shared catalog, so a restored install can resolve subscription nodes
	// before its first successful refresh.
	SubscriptionCache string                   `json:"subscription_cache,omitempty"`
	Subscription      *state.SubscriptionState `json:"subscription,omitempty"`
}

// BackupProject is one project's complete data: catalog entry, configuration,
// node list, node→port sidecar, node states, daily traffic and subscription
// usage.
type BackupProject struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Enabled    bool              `json:"enabled"`
	Autostart  bool              `json:"autostart"`
	Running    bool              `json:"running"`
	Cooldown   bool              `json:"cooldown"`
	ConfigYAML string            `json:"config_yaml"`
	Nodes      []string          `json:"nodes,omitempty"`
	NodePorts  map[string]uint16 `json:"node_ports,omitempty"`
	// SubscriptionCache mirrors the project's .subscription-cache.json so the
	// project keeps its nodes across a migration even if the provider is
	// temporarily unreachable.
	SubscriptionCache string                   `json:"subscription_cache,omitempty"`
	NodeStates        []state.NodeRecord       `json:"node_states,omitempty"`
	Traffic           []state.TrafficDay       `json:"traffic,omitempty"`
	Subscription      *state.SubscriptionState `json:"subscription,omitempty"`
}

// BackupImportRequest is the body of POST /api/backup/import.
type BackupImportRequest struct {
	Mode   string       `json:"mode"`
	Bundle BackupBundle `json:"bundle"`
}

// BackupImportResult reports what the import did, per project, so the panel can
// show a meaningful summary instead of a bare success.
type BackupImportResult struct {
	Created          []string `json:"created"`
	Updated          []string `json:"updated"`
	Skipped          []string `json:"skipped"`
	Failed           []string `json:"failed"`
	Started          []string `json:"started"`
	Restarted        []string `json:"restarted"`
	Warnings         []string `json:"warnings"`
	SharedUpdated    bool     `json:"shared_updated"`
	WorkspaceUpdated bool     `json:"workspace_updated"`
	NodesImported    int      `json:"nodes_imported"`
	TrafficDays      int      `json:"traffic_days"`
}
