package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProbeCoordinator shares health-check results between project runtimes. The
// cache key is supplied by Manager and includes the node identity plus the
// effective probe profile, so projects with different probe settings do not
// accidentally reuse one another's result.
type ProbeCoordinator struct {
	mu           sync.Mutex
	entries      map[string]coordinatedProbe
	inflight     map[string]*coordinatedProbeCall
	subscribers  map[string]map[uint64]probeSubscriber
	nextObserver uint64
}

type coordinatedProbe struct {
	at     time.Time
	result ProbeResult
	err    error
}

type coordinatedProbeCall struct {
	done   chan struct{}
	result ProbeResult
	err    error
	owners map[string]struct{}
}

type probeSubscriber struct {
	owner    string
	callback func(ProbeResult, error)
}

// NewProbeCoordinator creates a process-local probe result coordinator.
func NewProbeCoordinator() *ProbeCoordinator {
	return &ProbeCoordinator{
		entries:     make(map[string]coordinatedProbe),
		inflight:    make(map[string]*coordinatedProbeCall),
		subscribers: make(map[string]map[uint64]probeSubscriber),
	}
}

// Subscribe registers a project-local receiver for fresh probe results. The
// receiver is not called for a request made by the same owner; that caller
// applies its result directly. Notifications run asynchronously.
func (c *ProbeCoordinator) Subscribe(key, owner string, callback func(ProbeResult, error)) func() {
	if c == nil || key == "" || callback == nil {
		return func() {}
	}
	c.mu.Lock()
	c.nextObserver++
	token := c.nextObserver
	if c.subscribers[key] == nil {
		c.subscribers[key] = make(map[uint64]probeSubscriber)
	}
	c.subscribers[key][token] = probeSubscriber{owner: owner, callback: callback}
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		if observers := c.subscribers[key]; observers != nil {
			delete(observers, token)
			if len(observers) == 0 {
				delete(c.subscribers, key)
			}
		}
		c.mu.Unlock()
	}
}

// Execute runs one probe for a key and shares its result with other managers.
// An explicit request may bypass the completed-result cache, while still
// joining a request already in flight for the same key.
func (c *ProbeCoordinator) Execute(
	ctx context.Context,
	key string,
	maxAge time.Duration,
	force bool,
	probe func(context.Context, func(ProbeResult)) (ProbeResult, error),
	onProgress func(ProbeResult),
) (ProbeResult, error) {
	return c.ExecuteOwned(ctx, "", key, maxAge, force, probe, onProgress)
}

// ExecuteOwned is Execute with an owner identity used to suppress duplicate
// notifications to projects that already joined the same in-flight request.
func (c *ProbeCoordinator) ExecuteOwned(
	ctx context.Context,
	owner string,
	key string,
	maxAge time.Duration,
	force bool,
	probe func(context.Context, func(ProbeResult)) (ProbeResult, error),
	onProgress func(ProbeResult),
) (ProbeResult, error) {
	if c == nil {
		return executeProbe(ctx, probe, onProgress)
	}
	if probe == nil {
		return ProbeResult{}, errors.New("未配置节点探测函数")
	}

	c.mu.Lock()
	now := time.Now()
	for cacheKey, cached := range c.entries {
		if now.Sub(cached.at) > 24*time.Hour {
			delete(c.entries, cacheKey)
		}
	}
	if call, ok := c.inflight[key]; ok {
		if owner != "" {
			call.owners[owner] = struct{}{}
		}
		c.mu.Unlock()
		select {
		case <-call.done:
			if onProgress != nil {
				onProgress(call.result)
			}
			return call.result, call.err
		case <-ctx.Done():
			c.mu.Lock()
			if c.inflight[key] == call {
				delete(call.owners, owner)
			}
			c.mu.Unlock()
			return ProbeResult{}, fmt.Errorf("等待共享探测结果已取消: %w", ctx.Err())
		}
	}
	if !force && maxAge > 0 {
		if cached, ok := c.entries[key]; ok && now.Sub(cached.at) < maxAge {
			c.mu.Unlock()
			if onProgress != nil {
				onProgress(cached.result)
			}
			return cached.result, cached.err
		}
	}
	call := &coordinatedProbeCall{done: make(chan struct{}), owners: make(map[string]struct{}, 1)}
	if owner != "" {
		call.owners[owner] = struct{}{}
	}
	c.inflight[key] = call
	c.mu.Unlock()

	result, err := executeProbe(ctx, probe, onProgress)
	c.mu.Lock()
	call.result = result
	call.err = err
	c.entries[key] = coordinatedProbe{at: time.Now(), result: result, err: err}
	callbacks := make([]func(ProbeResult, error), 0)
	for _, subscriber := range c.subscribers[key] {
		if _, participated := call.owners[subscriber.owner]; participated {
			continue
		}
		callbacks = append(callbacks, subscriber.callback)
	}
	delete(c.inflight, key)
	close(call.done)
	c.mu.Unlock()
	for _, callback := range callbacks {
		go callback(result, err)
	}
	return result, err
}
