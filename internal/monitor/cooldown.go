package monitor

import (
	"net"
	"strings"
	"sync"
	"time"
)

// CooldownSettings carries one project's idle cool-down policy into the
// manager. A project enters cool-down when no application has used its nodes
// for Duration, or when the operator parks it by hand (Manual). While cooling
// down the periodic health-check sweep is skipped; the state ends on the next
// node application or on a manual release.
type CooldownSettings struct {
	// Enabled reports whether the idle cool-down is active at all.
	Enabled bool
	// Duration is the idle threshold. Zero disables the automatic part.
	Duration time.Duration
	// Manual is true when the operator parked the project by hand. A manual
	// cool-down has no deadline: it ends on release or on the next application.
	Manual bool
	// LastApplied seeds the idle timer. Passing the value observed by the
	// previous manager instance keeps the count running across an internal
	// restart (a subscription refresh reload) instead of starting over.
	LastApplied time.Time
}

// CooldownStatus is the read-only view of a project's cool-down state.
type CooldownStatus struct {
	// Active is true while automatic probing is paused.
	Active bool `json:"active"`
	// Manual distinguishes an operator-triggered cool-down from an idle one.
	Manual bool `json:"manual"`
	// Since is when the current cool-down started (zero while active is false).
	Since time.Time `json:"since,omitempty"`
	// LastApplied is the last time an application used one of the nodes.
	LastApplied time.Time `json:"last_applied_at,omitempty"`
	// IdleFor is how long the project has been without a node application.
	IdleFor time.Duration `json:"idle_for,omitempty"`
	// Duration is the configured idle threshold (zero while disabled).
	Duration time.Duration `json:"duration,omitempty"`
}

type cooldownState struct {
	mu sync.RWMutex
	// duration is the idle threshold; zero switches the automatic part off.
	duration time.Duration
	// manual is set while the operator parked the project by hand.
	manual bool
	// since records when the current cool-down began.
	since time.Time
	// lastApplied is the last node application observed for this project.
	lastApplied time.Time
	// announced avoids logging the same cool-down on every probe interval.
	announced bool
	// internalDestinations holds the hosts Proxy2API itself fetches through this
	// project's proxy entry (subscription providers). Their requests reach the
	// pool exactly like a client's but are not node applications.
	internalDestinations map[string]struct{}
}

// applyCooldownSettings installs the policy carried by monitor.Config. The idle
// timer is seeded from the previous manager when the caller provides one, and
// starts now otherwise, so a freshly started project is not treated as idle
// since the beginning of time.
func (m *Manager) applyCooldownSettings(settings CooldownSettings) {
	if m == nil {
		return
	}
	now := time.Now()
	m.cooldown.mu.Lock()
	defer m.cooldown.mu.Unlock()
	m.cooldown.duration = settings.Duration
	if !settings.Enabled || settings.Duration <= 0 {
		// The idle policy is off: only an explicit manual park can still pause
		// probing, so the automatic part is dropped here.
		m.cooldown.duration = 0
	}
	// The manual park is the operator's own decision and is independent of the
	// idle policy, so it is restored as-is.
	m.cooldown.manual = settings.Manual
	if settings.LastApplied.IsZero() {
		m.cooldown.lastApplied = now
	} else {
		m.cooldown.lastApplied = settings.LastApplied
	}
	m.syncCooldownSinceLocked(now)
}

// syncCooldownSinceLocked keeps the displayed cool-down start in step with the
// effective state: the manual moment for a parked project, the moment the idle
// threshold was crossed for an idle one, and zero while not cooling.
func (m *Manager) syncCooldownSinceLocked(now time.Time) {
	switch {
	case m.cooldown.manual:
		if m.cooldown.since.IsZero() {
			m.cooldown.since = now
		}
	case m.cooldown.duration > 0 && m.idleExceededLocked(now):
		m.cooldown.since = m.cooldown.lastApplied.Add(m.cooldown.duration)
	default:
		m.cooldown.since = time.Time{}
		m.cooldown.announced = false
	}
}

// SetManualCooldown parks or releases the project by hand. Parking takes effect
// immediately; releasing always takes effect too, because an explicit release
// restarts the idle countdown — otherwise an idle cool-down would stay in force
// (its condition is still true) and the button would look broken. A release
// also refreshes health data, which is the point of pre-warming a parked
// project.
func (m *Manager) SetManualCooldown(active bool) {
	if m == nil {
		return
	}
	now := time.Now()
	m.cooldown.mu.Lock()
	wasActive := m.cooldownActiveLocked(now)
	if active {
		m.cooldown.manual = true
	} else {
		m.cooldown.manual = false
		// Releasing restarts the idle countdown: an explicit release means "probe
		// now", otherwise an idle cool-down would stay in force (its condition is
		// still true) and the button would appear to do nothing.
		m.cooldown.lastApplied = now
	}
	m.syncCooldownSinceLocked(now)
	isActive := m.cooldownActiveLocked(now)
	m.cooldown.mu.Unlock()
	if active && !wasActive {
		m.logCooldown("项目已进入冷却，暂停自动探测（有新的节点应用或手动释放后恢复）")
	}
	if !active && wasActive {
		m.logCooldown("项目已退出冷却，恢复自动探测")
		if !isActive {
			go m.ProbeAllNow(m.currentProbeTimeout())
		}
	}
}

// RegisterInternalDestination records a host that Proxy2API fetches itself
// through this project's proxy entry — today the subscription providers whose
// direct fetch failed and which were retried over the project's own entry.
//
// Such a request arrives at the node pool exactly like a client's, but it is not
// a node application: counting it would keep resetting the idle timer, and with
// a subscription refresh interval shorter than the cool-down duration the
// project would never enter cool-down at all.
//
// The set only ever grows. A stale entry is harmless — it can only suppress an
// application to a host that is one of Proxy2API's own subscription providers.
func (m *Manager) RegisterInternalDestination(host string) {
	if m == nil {
		return
	}
	host = normalizeDestinationHost(host)
	if host == "" {
		return
	}
	m.cooldown.mu.Lock()
	defer m.cooldown.mu.Unlock()
	if m.cooldown.internalDestinations == nil {
		m.cooldown.internalDestinations = make(map[string]struct{})
	}
	m.cooldown.internalDestinations[host] = struct{}{}
}

// IsInternalDestination reports whether host belongs to Proxy2API's own
// subscription traffic and must therefore not count as a node application.
func (m *Manager) IsInternalDestination(host string) bool {
	if m == nil {
		return false
	}
	host = normalizeDestinationHost(host)
	if host == "" {
		return false
	}
	m.cooldown.mu.RLock()
	defer m.cooldown.mu.RUnlock()
	_, ok := m.cooldown.internalDestinations[host]
	return ok
}

// normalizeDestinationHost reduces an address to a comparable host: lower case,
// no port, no IPv6 brackets. Both the registered subscription host and the
// pool's dial destination go through it so the two always match.
func normalizeDestinationHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return ""
	}
	if split, _, err := net.SplitHostPort(host); err == nil {
		host = split
	}
	return strings.Trim(host, "[]")
}

// MarkApplied records that an application just used this project's nodes. It is
// called from the proxy hot path, so it must stay cheap. A cool-down that is in
// progress — automatic or manual — ends here and automatic probing resumes.
func (m *Manager) MarkApplied() {
	if m == nil {
		return
	}
	now := time.Now()
	m.cooldown.mu.Lock()
	// Evaluate the cool-down *before* refreshing the idle timestamp: an idle
	// cool-down is derived from that timestamp, so checking afterwards would
	// always report "not cooling" and silently skip the resume sweep below.
	wasCooling := m.cooldownActiveLocked(now)
	m.cooldown.lastApplied = now
	if wasCooling {
		m.cooldown.manual = false
		m.cooldown.since = time.Time{}
		m.cooldown.announced = false
	}
	m.cooldown.mu.Unlock()
	if !wasCooling {
		return
	}
	m.logCooldown("检测到节点应用，已退出冷却并恢复自动探测")
	// Refresh health data right away instead of waiting for the next periodic
	// tick: the sweep was skipped while cooling, so the readings are stale and
	// the caller's request is about to rely on them. The sweep is asynchronous,
	// so it never delays the connection that triggered it.
	timeout := m.currentProbeTimeout()
	go m.ProbeAllNow(timeout)
}

// CooldownActive reports whether automatic probing is currently paused.
func (m *Manager) CooldownActive() bool {
	if m == nil {
		return false
	}
	m.cooldown.mu.RLock()
	defer m.cooldown.mu.RUnlock()
	return m.cooldownActiveLocked(time.Now())
}

// CooldownStatus returns the current cool-down view for the WebUI.
func (m *Manager) CooldownStatus() CooldownStatus {
	if m == nil {
		return CooldownStatus{}
	}
	now := time.Now()
	m.cooldown.mu.RLock()
	defer m.cooldown.mu.RUnlock()
	status := CooldownStatus{
		Manual:      m.cooldown.manual,
		Since:       m.cooldown.since,
		LastApplied: m.cooldown.lastApplied,
		Duration:    m.cooldown.duration,
	}
	if !m.cooldown.lastApplied.IsZero() {
		status.IdleFor = now.Sub(m.cooldown.lastApplied)
	}
	status.Active = m.cooldownActiveLocked(now)
	return status
}

// LastAppliedAt returns the last node application recorded for this project.
func (m *Manager) LastAppliedAt() time.Time {
	if m == nil {
		return time.Time{}
	}
	m.cooldown.mu.RLock()
	defer m.cooldown.mu.RUnlock()
	return m.cooldown.lastApplied
}

// announceCooldownOnce logs the transition into an automatic cool-down exactly
// once, so a short probe interval does not flood the project log.
func (m *Manager) announceCooldownOnce() {
	if m == nil {
		return
	}
	m.cooldown.mu.Lock()
	active := m.cooldownActiveLocked(time.Now())
	announce := active && !m.cooldown.announced
	if announce {
		m.cooldown.announced = true
	}
	if !active {
		m.cooldown.announced = false
	}
	m.cooldown.mu.Unlock()
	if announce {
		m.logCooldown("项目已进入冷却，暂停自动探测（有新的节点应用或手动释放后恢复）")
	}
}

// cooldownActiveLocked must be called with cooldown.mu held. A manual park
// pauses probing even when the idle policy is switched off, because it is the
// operator's own explicit decision.
func (m *Manager) cooldownActiveLocked(now time.Time) bool {
	if m.cooldown.manual {
		return true
	}
	return m.idleExceededLocked(now)
}

// idleExceededLocked reports whether the idle timer passed the threshold.
func (m *Manager) idleExceededLocked(now time.Time) bool {
	if m.cooldown.duration <= 0 || m.cooldown.lastApplied.IsZero() {
		return false
	}
	return now.Sub(m.cooldown.lastApplied) >= m.cooldown.duration
}

// currentProbeTimeout reads the configured per-node probe timeout.
func (m *Manager) currentProbeTimeout() time.Duration {
	m.mu.RLock()
	timeout := m.probeTimeout
	m.mu.RUnlock()
	if timeout <= 0 {
		timeout = 110 * time.Second
	}
	return timeout
}

func (m *Manager) logCooldown(message string) {
	if m.logger != nil {
		m.logger.Info(message)
	}
}
