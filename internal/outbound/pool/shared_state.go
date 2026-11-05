package pool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	pending           int
	capacity          int
	epoch             uint64
	transientFailures int
	recovering        bool
	recoverySuccesses int
	trialInFlight     bool
	exploring         bool
	lastSelected      time.Time
	lastSequence      uint64
	nextExplore       time.Time
}

const (
	transientCooldown     = 15 * time.Second
	maxTransientCooldown  = 5 * time.Minute
	recoverySuccessTarget = 3
	defaultNodeCapacity   = 128
)

// Prefer typed transport errors; string matching is only for opaque upstream
// proxy handshakes. Application HTTP responses inside a tunnel are not observed.
func isTransientError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var cert *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &cert) || errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return true
	} // destination failures must not permanently eject a proxy
	var transport net.Error
	if errors.As(err, &transport) && (transport.Timeout() || transport.Temporary()) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, fragment := range []string{"too many requests", "timeout", "deadline exceeded", "connection reset", "reset by peer", "temporarily", "try again", "service unavailable"} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}
	return transientHTTPStatus.MatchString(msg)
}

var transientHTTPStatus = regexp.MustCompile(`(?i)(?:HTTP/[0-9](?:[.][0-9])?\s+|status(?:\s+code)?[ :=]+|response(?:\s+code)?[ :=]+)(429|503)\b`)

func cooldownFor(failures int, jitter float64) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures > 6 {
		failures = 6
	}
	base := transientCooldown * time.Duration(1<<uint(failures-1))
	if base > maxTransientCooldown {
		base = maxTransientCooldown
	}
	// 80%-100% jitter keeps the documented upper bound intact.
	return time.Duration(float64(base) * (0.8 + 0.2*jitter))
}

// SharedStateStore owns the mutable pool state for one project runtime. Pool
// and multi-port outbounds inside the same project share this store, while
// separate projects receive separate instances.
type SharedStateStore struct {
	selectionMu sync.Mutex // selection and reservation across pool/sticky/per-node listeners
	states      sync.Map   // map[tag]*sharedMemberState
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
	s.transientFailures, s.recovering = entry.CircuitState()
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

// recordFailure closes a half-open circuit immediately. Failures from dials
// started before the current circuit epoch are still recorded as observations,
// but cannot repeatedly grow the backoff or change recovery state.
func (s *sharedMemberState) recordFailure(cause error, threshold int, duration time.Duration, lease *memberLease) (int, bool, time.Time, bool) {
	transient := isTransientError(cause)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishPendingLocked(lease)
	current := lease == nil || lease.epoch == s.epoch
	count, triggered := s.failures, false
	until := s.blacklistedUntil
	if current {
		if transient {
			s.transientFailures++
			until = time.Now().Add(cooldownFor(s.transientFailures, rand.Float64()))
			if s.blacklistedUntil.After(until) {
				until = s.blacklistedUntil
			}
			s.openCircuitLocked(until)
		} else {
			s.failures++
			count = s.failures
			if s.failures >= threshold || (lease != nil && lease.trial) {
				triggered = true
				until = time.Now().Add(duration)
				if s.blacklistedUntil.After(until) {
					until = s.blacklistedUntil
				}
				s.failures = 0
				s.openCircuitLocked(until)
			}
		}
	}
	if entry := s.entry.Load(); entry != nil {
		entry.RecordFailure(cause, s.failures)
		entry.SetCircuitState(s.transientFailures, s.recovering, s.failures)
		if s.blacklisted {
			entry.Blacklist(s.blacklistedUntil)
		}
	}
	return count, triggered, until, transient
}

func (s *sharedMemberState) openCircuitLocked(until time.Time) {
	s.epoch++
	s.blacklisted, s.recovering = true, true
	s.recoverySuccesses = 0
	s.blacklistedUntil = until
	s.scheduleReleaseLocked(until)
}

func (s *sharedMemberState) recordSuccess(latency time.Duration, lease *memberLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishPendingLocked(lease)
	if lease == nil || lease.epoch == s.epoch {
		s.failures = 0
		if lease != nil && lease.trial && !s.blacklisted {
			s.recoverySuccesses++
			if s.transientFailures > 0 {
				s.transientFailures--
			}
			if s.recoverySuccesses >= recoverySuccessTarget {
				s.recovering = false
				s.transientFailures = 0
			}
		} else if !s.recovering && !s.blacklisted {
			s.transientFailures = 0
		}
	}
	if entry := s.entry.Load(); entry != nil {
		entry.RecordSuccess(latency)
		entry.SetCircuitState(s.transientFailures, s.recovering, s.failures)
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
		s.recovering = true
		s.recoverySuccesses = 0
		s.activationPending = false
		s.blacklistedUntil = time.Time{}
		s.failures = 0
		releaseProbe = s.releaseProbe
	}
	blacklisted := s.blacklisted
	if expired {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearBlacklist()
			entry.SetCircuitState(s.transientFailures, true, s.failures)
		}
	}
	s.mu.Unlock()
	if expired && releaseProbe != nil {
		releaseProbe()
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
	s.epoch++
	s.failures, s.transientFailures, s.recoverySuccesses = 0, 0, 0
	s.blacklisted, s.activationPending = false, false
	s.recovering = wasBlacklisted || s.recovering
	s.blacklistedUntil = time.Time{}
	releaseProbe := s.releaseProbe
	if entry := s.entry.Load(); entry != nil {
		if wasBlacklisted {
			entry.ClearBlacklist()
		}
		entry.SetCircuitState(0, s.recovering, 0)
	}
	s.mu.Unlock()
	if wasBlacklisted && releaseProbe != nil {
		releaseProbe()
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
		state.activationPending = false
		state.failures = 0
		state.openCircuitLocked(until)
		if entry := state.entry.Load(); entry != nil {
			entry.SetCircuitState(state.transientFailures, true, state.failures)
		}
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
