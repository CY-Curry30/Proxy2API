package monitor

import (
	"strings"
	"time"

	"Proxy2API/internal/successrate"
)

// NodeSuccessMetrics separates observed dial results from the scheduler's score.
// Scores are a snapshot of the available pool, before per-request protocol,
// retry, exploration and active-connection selection rules are applied.
type NodeSuccessMetrics struct {
	Basis             string   `json:"basis"`
	WindowSize        int      `json:"window_size"`
	Successes         int64    `json:"successes"`
	Attempts          int64    `json:"attempts"`
	EffectiveAttempts float64  `json:"effective_attempts"`
	Stale             bool     `json:"stale"`
	Eligible          bool     `json:"eligible"`
	SchedulingEnabled bool     `json:"scheduling_enabled"`
	SchedulerScore    *float64 `json:"scheduler_score,omitempty"`
}

// annotateSuccessMetrics uses the same window, prior and score functions as the
// pool, so a custom window or legacy configuration is reflected by both tables.
func (s *Server) annotateSuccessMetrics(nodes []Snapshot, now time.Time) {
	window, strength := 0, 0
	var halfLife time.Duration
	scheduling := false
	s.cfgMu.RLock()
	if cfg := s.cfgSrc; cfg != nil {
		window, strength = cfg.Pool.SuccessWindow, cfg.Pool.SuccessPriorStrength
		halfLife = cfg.Pool.SuccessHalfLife
		mode := strings.ToLower(cfg.Pool.Mode)
		scheduling = !s.catalogOnly && (cfg.Mode == "pool" || cfg.Mode == "hybrid") &&
			(mode == "success" || mode == "highest-success-rate")
	}
	s.cfgMu.RUnlock()
	window = successrate.WindowSize(window)
	strength = successrate.PriorStrength(strength)

	type effectiveWindow struct{ successes, attempts float64 }
	effective := make([]effectiveWindow, len(nodes))
	totalSuccess, totalAttempts := 0.0, 0.0
	for i := range nodes {
		node := &nodes[i]
		metrics := &NodeSuccessMetrics{
			Basis: "recent", WindowSize: window,
			Eligible:          node.InitialCheckDone && node.Available && !node.Blacklisted && !node.Quarantined && !node.Suppressed,
			SchedulingEnabled: scheduling,
		}
		node.SuccessMetrics = metrics
		if window < 0 {
			metrics.Basis, metrics.WindowSize = "cumulative", 0
			metrics.Successes = node.SuccessCount
			metrics.Attempts = node.SuccessCount + int64(node.FailureCount)
			continue
		}
		successes, attempts := successrate.Window(node.recentMask, node.RecentAttempts, window)
		metrics.Successes, metrics.Attempts = int64(successes), int64(attempts)
		metrics.Stale = attempts > 0 && !node.RecentLastAt.IsZero() && now.Sub(node.RecentLastAt) >= successrate.StaleAfter
		weightedSuccess, weightedAttempts := successrate.WeightedWindow(node.recentMask, node.RecentAttempts, window, node.recentTimes[:], node.RecentLastAt, now, halfLife)
		metrics.EffectiveAttempts = weightedAttempts
		effective[i] = effectiveWindow{weightedSuccess, weightedAttempts}
		if metrics.Eligible {
			totalSuccess += weightedSuccess
			totalAttempts += weightedAttempts
		}
	}

	if !scheduling {
		return
	}
	prior := successrate.DefaultPrior
	if totalAttempts > 0 {
		prior = float64(totalSuccess) / float64(totalAttempts)
	}
	for i := range nodes {
		metrics := nodes[i].SuccessMetrics
		if !metrics.Eligible {
			continue
		}
		var score float64
		if window < 0 {
			if metrics.Attempts == 0 {
				continue
			}
			score = float64(metrics.Successes) / float64(metrics.Attempts)
		} else {
			score = successrate.Score(effective[i].successes, effective[i].attempts, prior, float64(strength))
		}
		points := score * 100
		metrics.SchedulerScore = &points
	}
}
