package project

import (
	"fmt"
	"sort"

	"Proxy2API/internal/state"
)

func (r *Runtime) LoadTrafficUsage() (state.UsageTotals, error) {
	r.mu.RLock()
	store := r.stateStore
	configPath := r.configPath
	r.mu.RUnlock()
	if store != nil {
		return store.UsageTracker().Totals(), nil
	}
	totals, err := state.LoadTrafficUsageSnapshot(configPath)
	if err != nil {
		// A stopped project may have started and opened its database while the
		// snapshot was being read. Prefer its live counters when now available.
		r.mu.RLock()
		store = r.stateStore
		r.mu.RUnlock()
		if store != nil {
			return store.UsageTracker().Totals(), nil
		}
	}
	return totals, err
}

// LoadTrafficUsage includes every registered project, including stopped ones.
// The shared catalog's diagnostic runtime is not a source of business traffic.
func (r *Registry) LoadTrafficUsage() (state.UsageTotals, error) {
	r.mu.RLock()
	projects := make(map[string]*Runtime, len(r.runtimes))
	ids := make([]string, 0, len(r.runtimes))
	for id, runtime := range r.runtimes {
		projects[id] = runtime
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Strings(ids)
	var total state.UsageTotals
	for _, id := range ids {
		usage, err := projects[id].LoadTrafficUsage()
		if err != nil {
			return state.UsageTotals{}, fmt.Errorf("读取项目 %q 的节点用量失败: %w", id, err)
		}
		total.Merge(usage)
	}
	return total, nil
}
