package subscription

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"Proxy2API/internal/config"
)

// FetchCoordinator shares remote subscription fetches between project
// runtimes.  Each project still owns its cache, status and reload lifecycle,
// but concurrent or recently completed requests for the same URL reuse the
// one remote response.
type FetchCoordinator struct {
	mu             sync.Mutex
	entries        map[string]coordinatedFetch
	inflight       map[string]*coordinatedFetchCall
	subscribers    map[string]fetchSubscriber
	nextSubscriber uint64
}

type coordinatedFetch struct {
	at     time.Time
	result subscriptionFetchResult
}

type coordinatedFetchCall struct {
	done   chan struct{}
	result subscriptionFetchResult
	owners map[string]struct{}
}

type fetchSubscriber struct {
	token    uint64
	callback func(string)
}

// Get returns a completed shared response without starting a remote request.
func (c *FetchCoordinator) Get(rawURL string) (subscriptionFetchResult, bool) {
	if c == nil {
		return subscriptionFetchResult{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[rawURL]
	if !ok {
		return subscriptionFetchResult{}, false
	}
	return cloneSubscriptionFetchResult(entry.result), true
}

// NewFetchCoordinator creates a coordinator shared by all project runtimes in
// one process.
func NewFetchCoordinator() *FetchCoordinator {
	return &FetchCoordinator{
		entries:     make(map[string]coordinatedFetch),
		inflight:    make(map[string]*coordinatedFetchCall),
		subscribers: make(map[string]fetchSubscriber),
	}
}

// Subscribe registers a project runtime to receive a URL notification after a
// different project obtains a fresh remote response. Callbacks run
// asynchronously and never while the coordinator lock is held.
func (c *FetchCoordinator) Subscribe(owner string, callback func(rawURL string)) func() {
	if c == nil || owner == "" || callback == nil {
		return func() {}
	}
	c.mu.Lock()
	c.nextSubscriber++
	token := c.nextSubscriber
	c.subscribers[owner] = fetchSubscriber{token: token, callback: callback}
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		if current, ok := c.subscribers[owner]; ok && current.token == token {
			delete(c.subscribers, owner)
		}
		c.mu.Unlock()
	}
}

// Fetch executes fetch when the URL is not fresh in the shared cache. A forced
// request still joins an already-running request, preventing duplicate remote
// calls when two projects refresh at the same time.
func (c *FetchCoordinator) Fetch(ctx context.Context, owner, rawURL string, maxAge time.Duration, force bool, fetch func(context.Context) subscriptionFetchResult) subscriptionFetchResult {
	if c == nil {
		if fetch == nil {
			return subscriptionFetchResult{url: rawURL, err: errors.New("未配置订阅获取函数")}
		}
		return fetch(ctx)
	}
	if fetch == nil {
		return subscriptionFetchResult{url: rawURL, err: errors.New("未配置订阅获取函数")}
	}

	c.mu.Lock()
	now := time.Now()
	for key, cached := range c.entries {
		if now.Sub(cached.at) > 24*time.Hour {
			delete(c.entries, key)
		}
	}
	if call, ok := c.inflight[rawURL]; ok {
		if owner != "" {
			call.owners[owner] = struct{}{}
		}
		c.mu.Unlock()
		select {
		case <-call.done:
			return cloneSubscriptionFetchResult(call.result)
		case <-ctx.Done():
			c.mu.Lock()
			if c.inflight[rawURL] == call {
				delete(call.owners, owner)
			}
			c.mu.Unlock()
			return subscriptionFetchResult{url: rawURL, err: fmt.Errorf("等待共享订阅获取结果已取消: %w", ctx.Err())}
		}
	}
	if !force && maxAge > 0 {
		if cached, ok := c.entries[rawURL]; ok && now.Sub(cached.at) < maxAge && cached.result.err == nil {
			result := cloneSubscriptionFetchResult(cached.result)
			c.mu.Unlock()
			return result
		}
	}
	call := &coordinatedFetchCall{
		done:   make(chan struct{}),
		owners: make(map[string]struct{}, 1),
	}
	if owner != "" {
		call.owners[owner] = struct{}{}
	}
	c.inflight[rawURL] = call
	c.mu.Unlock()

	result := fetch(ctx)
	c.mu.Lock()
	call.result = cloneSubscriptionFetchResult(result)
	callbacks := make([]func(string), 0, len(c.subscribers))
	if result.err == nil {
		c.entries[rawURL] = coordinatedFetch{at: time.Now(), result: cloneSubscriptionFetchResult(result)}
		for subscriberOwner, subscriber := range c.subscribers {
			if _, participated := call.owners[subscriberOwner]; !participated {
				callbacks = append(callbacks, subscriber.callback)
			}
		}
	}
	delete(c.inflight, rawURL)
	close(call.done)
	c.mu.Unlock()
	for _, callback := range callbacks {
		go callback(rawURL)
	}
	return result
}

func cloneSubscriptionFetchResult(result subscriptionFetchResult) subscriptionFetchResult {
	result.nodes = append([]config.NodeConfig(nil), result.nodes...)
	return result
}
