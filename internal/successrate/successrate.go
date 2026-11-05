// Package successrate shares success scheduling calculations with monitoring.
package successrate

import (
	"math"
	"math/bits"
	"time"
)

const (
	WindowLimit          = 64
	DefaultPriorStrength = 8
	DefaultPrior         = 0.9
	StaleAfter           = 30 * time.Minute
	DefaultHalfLife      = 30 * time.Minute
)

// WindowSize preserves negative values for legacy cumulative scoring.
func WindowSize(size int) int {
	if size == 0 || size > WindowLimit {
		return WindowLimit
	}
	return size
}

func PriorStrength(strength int) int {
	if strength == 0 {
		return DefaultPriorStrength
	}
	if strength < 0 {
		return 0
	}
	return strength
}

// Window reads the newest outcomes, with bit zero representing the newest dial.
func Window(mask uint64, count, limit int) (successes, attempts int) {
	if limit > 0 && count > limit {
		count = limit
		mask &= (uint64(1) << uint(limit)) - 1
	}
	return bits.OnesCount64(mask), count
}

// HalfLife normalizes the time it takes for an observation to lose half its weight.
func HalfLife(value time.Duration) time.Duration {
	if value <= 0 {
		return DefaultHalfLife
	}
	return value
}

// WeightedWindow decays each observation independently. A new dial cannot restore
// the weight of older outcomes. Old databases without timestamps use last as the
// migration anchor, never the time of the next new dial.
func WeightedWindow(mask uint64, count, limit int, times []time.Time, last, now time.Time, halfLife time.Duration) (successes, attempts float64) {
	_, count = Window(mask, count, limit)
	halfLife = HalfLife(halfLife)
	for i := 0; i < count; i++ {
		at := last
		if i < len(times) {
			at = times[i]
		}
		if at.IsZero() {
			continue
		}
		age := now.Sub(at)
		if age < 0 {
			age = 0
		}
		weight := math.Exp2(-float64(age) / float64(halfLife))
		attempts += weight
		if mask&(uint64(1)<<uint(i)) != 0 {
			successes += weight
		}
	}
	return
}

// Score turns a window into a comparable score using a Beta posterior:
//
//	α = S + m·p0        β = (n - S) + m·(1 - p0)
//	score = α/(α+β) - z·sqrt(αβ / ((α+β)²·(α+β+1)))
//
// The first term smooths a small sample toward the pool mean; the second is a
// one-sigma lower confidence bound, which is what stops a node with a single
// lucky dial from outranking a well-sampled one — the fresh node's interval is
// wide, so its lower bound sits below the proven node's. Using the Beta variance
// rather than the normal approximation keeps the bound meaningful at 0% and
// 100%, where p(1-p) would collapse to zero and make a perfect record unbeatable
// by any amount of evidence.
func Score(successes, attempts, prior, priorStrength float64) float64 {
	// A prior of exactly 0 or 1 would make one of the pseudo-counts vanish and
	// take the confidence interval with it.
	if prior < 0.05 {
		prior = 0.05
	} else if prior > 0.95 {
		prior = 0.95
	}
	alpha := successes + priorStrength*prior
	beta := (attempts - successes) + priorStrength*(1-prior)
	total := alpha + beta
	if total <= 0 {
		return prior
	}
	rate := alpha / total
	variance := alpha * beta / (total * total * (total + 1))
	return rate - math.Sqrt(variance)
}
