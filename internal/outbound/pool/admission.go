package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

var selectionSequence atomic.Uint64

// A lease reserves capacity before dialing. It tracks pending dials separately
// from established connections and releases both exactly once on cancellation.
type memberLease struct {
	member      *memberState
	epoch       uint64
	trial       bool
	exploratory bool
	pending     bool // guarded by shared.mu
	once        sync.Once
}

func (s *sharedMemberState) configureCapacity(value int) {
	if value == 0 {
		value = defaultNodeCapacity
	}
	s.mu.Lock()
	s.capacity = value
	s.mu.Unlock()
}

func (s *sharedMemberState) canReserveLocked(cold, fallback bool, now time.Time) bool {
	if s.blacklisted {
		return false
	}
	active := s.active.Load()
	if s.capacity > 0 && int(active) >= s.capacity {
		return false
	}
	if (s.recovering || fallback) && (active >= 1 || s.trialInFlight) {
		return false
	}
	if cold && s.pending >= 1 {
		return false
	}
	if fallback && now.Before(s.nextExplore) {
		return false
	}
	return true
}

func (s *sharedMemberState) canReserve(cold, fallback bool, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canReserveLocked(cold, fallback, now)
}

func (s *sharedMemberState) reserve(member *memberState, cold, exploratory, fallback bool, now time.Time) *memberLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	if member.entry != nil && (member.entry.Quarantined() || member.entry.Suppressed() || (!fallback && !member.entry.Healthy())) {
		return nil
	}
	if !s.canReserveLocked(cold, fallback, now) || (exploratory && (s.exploring || s.pending > 0)) {
		return nil
	}
	lease := &memberLease{member: member, epoch: s.epoch, trial: s.recovering || fallback, exploratory: exploratory, pending: true}
	s.pending++
	s.incActive()
	s.lastSelected = now
	s.lastSequence = selectionSequence.Add(1)
	if lease.trial {
		s.trialInFlight = true
	}
	if exploratory {
		s.exploring = true
		s.nextExplore = now.Add(5 * time.Second)
	}
	if fallback {
		s.nextExplore = now.Add(15 * time.Second)
	}
	return lease
}

func (s *sharedMemberState) finishPendingLocked(lease *memberLease) {
	if lease == nil || !lease.pending {
		return
	}
	lease.pending = false
	s.pending--
	if lease.trial {
		s.trialInFlight = false
	}
	if lease.exploratory {
		s.exploring = false
	}
}

func (lease *memberLease) release() {
	lease.once.Do(func() {
		s := lease.member.shared
		s.mu.Lock()
		defer s.mu.Unlock()
		s.finishPendingLocked(lease)
		s.decActive()
	})
}

func (s *sharedMemberState) schedulingState() (active, capacity, pending int, recovering bool, lastSelected, nextExplore time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.active.Load()), s.capacity, s.pending, s.recovering, s.lastSelected, s.nextExplore
}

func coldMember(member *memberState) bool {
	if member.entry == nil {
		return false
	}
	_, attempts, _ := member.entry.RecentWindow(0)
	return attempts < 8
}

func normalizedCapacity(value int) int {
	if value == 0 {
		return defaultNodeCapacity
	}
	return value
}

func (s *sharedMemberState) selectionOrder() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSequence
}
