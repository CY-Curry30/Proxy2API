package project

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"Proxy2API/internal/state"
)

func (r *Runtime) ClearTrafficHistory() error {
	// Starting or stopping can transfer ownership of the database between a
	// live store and the snapshot path, so keep that ownership stable here.
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.RLock()
	store, configPath := r.stateStore, r.configPath
	r.mu.RUnlock()
	if store != nil {
		return store.ClearTrafficHistory()
	}
	return state.ClearTrafficHistorySnapshot(configPath)
}

// ClearTrafficHistory includes stopped projects and leaves their runtime
// status unchanged. Catalog changes and backup imports wait until it finishes.
func (r *Registry) ClearTrafficHistory(ctx context.Context) error {
	r.catalogMu.Lock()
	defer r.catalogMu.Unlock()
	r.mu.RLock()
	projects := make(map[string]*Runtime, len(r.runtimes))
	ids := make([]string, 0, len(r.runtimes))
	for id, runtime := range r.runtimes {
		projects[id] = runtime
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Strings(ids)
	var failures []error
	for _, id := range ids {
		if ctx != nil && ctx.Err() != nil {
			failures = append(failures, ctx.Err())
			break
		}
		if err := projects[id].ClearTrafficHistory(); err != nil {
			failures = append(failures, fmt.Errorf("项目 %q: %w", id, err))
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("部分项目的流量历史未能清空: %w", err)
	}
	return nil
}
