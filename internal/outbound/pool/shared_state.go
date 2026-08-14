package pool

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Proxy2API/internal/monitor"
)

// sharedMemberState holds failure/blacklist state shared across all pool instances.
// This enables hybrid mode where pool and multi-port modes share the same node state.
type sharedMemberState struct {
	mu                sync.Mutex
	failures          int
	blacklisted       bool
	blacklistedUntil  time.Time
	releaseTimer      *time.Timer
	releaseProbe      func()
	restored          bool
	activationPending bool
	entry             atomic.Pointer[monitor.EntryHandle]
	active            atomic.Int32
}

// transientCooldown is how long a node is skipped after a transient failure
// (rate-limit / timeout / connection reset). Far shorter than the 24h blacklist
// used for permanent faults, because these errors usually clear on their own —
// e.g. a shared free node briefly rate-limited (HTTP 429) by its CDN.
const transientCooldown = 60 * time.Second

// isTransientError reports whether err looks like a temporary condition that
// should NOT count toward the permanent-blacklist threshold. Rate limiting
// (429), timeouts and connection resets fall here: the node is likely alive and
// will recover, so a full 24h ban would needlessly drain the healthy pool.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "deadline exceeded"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "reset by peer"),
		strings.Contains(msg, "temporarily"),
		strings.Contains(msg, "try again"),
		strings.Contains(msg, "service unavailable"),
		strings.Contains(msg, "503"):
		return true
	}
	return false
}

// SharedStateStore owns the mutable pool state for one project runtime. Pool
// and multi-port outbounds inside the same project share this store, while
// separate projects receive separate instances.
type SharedStateStore struct {
	states sync.Map // map[tag]*sharedMemberState
}

// NewSharedStateStore creates an isolated pool state container.
func NewSharedStateStore() *SharedStateStore {
	return &SharedStateStore{}
}

type sharedStateStoreContextKey struct{}

var defaultSharedStateStore = NewSharedStateStore()

// ContextWithSharedStateStore makes a project state container available to
// every custom pool outbound constructed by sing-box.
func ContextWithSharedStateStore(ctx context.Context, store *SharedStateStore) context.Context {
	if store == nil {
		return ctx
	}
	return context.WithValue(ctx, sharedStateStoreContextKey{}, store)
}

func sharedStateStoreFromContext(ctx context.Context) *SharedStateStore {
	if store, ok := ctx.Value(sharedStateStoreContextKey{}).(*SharedStateStore); ok && store != nil {
		return store
	}
	return defaultSharedStateStore
}

// acquireSharedState returns the shared state for a tag, creating if needed.
func (s *SharedStateStore) acquire(tag string) *sharedMemberState {
	if s == nil {
		s = defaultSharedStateStore
	}
	if v, ok := s.states.Load(tag); ok {
		return v.(*sharedMemberState)
	}
	state := &sharedMemberState{}
	actual, _ := s.states.LoadOrStore(tag, state)
	return actual.(*sharedMemberState)
}

// lookupSharedState returns the shared state if it exists.
func (s *SharedStateStore) lookup(tag string) (*sharedMemberState, bool) {
	if s == nil {
		s = defaultSharedStateStore
	}
	v, ok := s.states.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(*sharedMemberState), true
}

// Reset clears this project's shared state during reload or shutdown.
func (s *SharedStateStore) Reset() {
	if s == nil {
		return
	}
	s.states.Range(func(key, value any) bool {
		state := value.(*sharedMemberState)
		state.mu.Lock()
		if state.releaseTimer != nil {
			state.releaseTimer.Stop()
			state.releaseTimer = nil
		}
		state.blacklisted = false
		state.releaseProbe = nil
		state.mu.Unlock()
		s.states.Delete(key)
		return true
	})
}

func (s *sharedMemberState) attachEntry(entry *monitor.EntryHandle) {
	if entry == nil {
		return
	}
	s.entry.Store(entry)
}

func (s *sharedMemberState) entryHandle() *monitor.EntryHandle {
	return s.entry.Load()
}

func (s *sharedMemberState) setReleaseProbe(fn func()) {
	s.mu.Lock()
	s.releaseProbe = fn
	s.mu.Unlock()
}

func (s *sharedMemberState) restore(entry *monitor.EntryHandle) {
	if entry == nil {
		return
	}
	failures, blacklisted, until := entry.RestoredPoolState()
	s.mu.Lock()
	if s.restored {
		s.mu.Unlock()
		return
	}
	s.restored = true
	s.failures = failures
	s.blacklisted = blacklisted
	s.blacklistedUntil = until
	if blacklisted {
		s.activationPending = true
	}
	s.mu.Unlock()
}

func (s *sharedMemberState) activateRestoredBlacklist() {
	s.mu.Lock()
	if !s.activationPending || !s.blacklisted {
		s.mu.Unlock()
		return
	}
	s.activationPending = false
	s.scheduleReleaseLocked(s.blacklistedUntil)
	s.mu.Unlock()
}

// ActivateRestoredBlacklists starts expiry timers only after sing-box has
// completed startup, so an already-expired entry cannot probe a half-started
// outbound during construction.
func (s *SharedStateStore) ActivateRestoredBlacklists() {
	if s == nil {
		return
	}
	s.states.Range(func(_, value any) bool {
		value.(*sharedMemberState).activateRestoredBlacklist()
		return true
	})
}

func (s *sharedMemberState) scheduleReleaseLocked(until time.Time) {
	if s.releaseTimer != nil {
		s.releaseTimer.Stop()
	}
	delay := time.Until(until)
	if delay < 0 {
		delay = 0
	}
	s.releaseTimer = time.AfterFunc(delay, func() {
		s.isBlacklisted(time.Now())
	})
}

// recordFailure records a failure and decides whether to blacklist the node.
//
// Transient errors (rate-limit 429, timeouts, connection resets) do NOT count
// toward the permanent threshold; instead they impose a short cooldown so the
// node is briefly skipped and then retried automatically. Permanent errors
// (handshake/cert/protocol failures, 404, etc.) accumulate toward the threshold
// and trigger the full blacklist duration once it is reached.
//
// Returns: (current permanent-failure count, blacklisted, blacklist-until, transient).
func (s *sharedMemberState) recordFailure(cause error, threshold int, duration time.Duration) (int, bool, time.Time, bool) {
	transient := isTransientError(cause)

	s.mu.Lock()
	var count int
	var persistedFailures int
	triggered := false
	var until time.Time
	if transient {
		// Short cooldown only; do not accumulate toward the 24h blacklist.
		count = s.failures
		until = time.Now().Add(transientCooldown)
		s.blacklisted = true
		s.blacklistedUntil = until
		s.scheduleReleaseLocked(until)
	} else {
		s.failures++
		count = s.failures
		if s.failures >= threshold {
			triggered = true
			until = time.Now().Add(duration)
			s.failures = 0
			s.blacklisted = true
			s.blacklistedUntil = until
			s.scheduleReleaseLocked(until)
		}
	}
	persistedFailures = s.failures
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.RecordFailure(cause, persistedFailures)
		if triggered || transient {
			entry.Blacklist(until)
		}
	}
	return count, triggered, until, transient
}

func (s *sharedMemberState) recordSuccess() {
	s.mu.Lock()
	s.failures = 0
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.RecordSuccess()
	}
}

// isBlacklisted checks if the node is currently blacklisted, auto-clearing if expired.
func (s *sharedMemberState) isBlacklisted(now time.Time) bool {
	s.mu.Lock()
	expired := s.blacklisted && !now.Before(s.blacklistedUntil)
	var releaseProbe func()
	if expired {
		if s.releaseTimer != nil {
			s.releaseTimer.Stop()
			s.releaseTimer = nil
		}
		s.blacklisted = false
		s.activationPending = false
		s.blacklistedUntil = time.Time{}
		s.failures = 0
		releaseProbe = s.releaseProbe
	}
	blacklisted := s.blacklisted
	s.mu.Unlock()

	if expired {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearBlacklist()
		}
		if releaseProbe != nil {
			releaseProbe()
		}
	}
	return blacklisted
}

// blacklistRemaining returns the remaining blacklist duration.
// Returns 0 if not blacklisted.
func (s *sharedMemberState) blacklistRemaining(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.blacklisted {
		return 0
	}

	remaining := s.blacklistedUntil.Sub(now)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (s *sharedMemberState) forceRelease() {
	s.mu.Lock()
	wasBlacklisted := s.blacklisted
	if s.releaseTimer != nil {
		s.releaseTimer.Stop()
		s.releaseTimer = nil
	}
	s.failures = 0
	s.blacklisted = false
	s.activationPending = false
	s.blacklistedUntil = time.Time{}
	releaseProbe := s.releaseProbe
	s.mu.Unlock()

	if wasBlacklisted {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearBlacklist()
		}
		if releaseProbe != nil {
			releaseProbe()
		}
	}
}

func (s *sharedMemberState) incActive() {
	s.active.Add(1)
	if entry := s.entry.Load(); entry != nil {
		entry.IncActive()
	}
}

func (s *sharedMemberState) decActive() {
	s.active.Add(-1)
	if entry := s.entry.Load(); entry != nil {
		entry.DecActive()
	}
}

func (s *sharedMemberState) activeCount() int32 {
	return s.active.Load()
}

// release clears blacklist state for a tag (called from release functions).
func (s *SharedStateStore) release(tag string) {
	if state, ok := s.lookup(tag); ok {
		state.forceRelease()
	}
}

// blacklist manually blacklists a node in this project's pool state.
func (s *SharedStateStore) blacklist(tag string, duration time.Duration) {
	if state, ok := s.lookup(tag); ok {
		until := time.Now().Add(duration)
		state.mu.Lock()
		state.blacklisted = true
		state.activationPending = false
		state.blacklistedUntil = until
		state.failures = 0
		state.scheduleReleaseLocked(until)
		state.mu.Unlock()
	}
}

// ResetSharedStateStore preserves the legacy package API for callers that do
// not inject a project store. New runtimes should call Reset on their own store.
func ResetSharedStateStore() {
	defaultSharedStateStore.Reset()
}

// ActivateRestoredBlacklists preserves the legacy package API.
func ActivateRestoredBlacklists() {
	defaultSharedStateStore.ActivateRestoredBlacklists()
}
