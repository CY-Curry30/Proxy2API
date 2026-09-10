package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TaskStatus is the lifecycle state exposed by the asynchronous operation API.
// A task is deliberately process-local: it represents work in the current
// runtime and is not persisted across a process restart.
type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

// TaskSnapshot is a race-free, JSON-friendly view of an asynchronous task.
// StartedAt and FinishedAt are pointers so a queued task can omit timestamps
// that do not exist yet.
type TaskSnapshot struct {
	ID         string     `json:"id"`
	Kind       string     `json:"kind"`
	Scope      string     `json:"scope,omitempty"`
	ProjectID  string     `json:"project_id,omitempty"`
	Status     TaskStatus `json:"status"`
	Progress   float64    `json:"progress"`
	Message    string     `json:"message,omitempty"`
	Result     any        `json:"result,omitempty"`
	Error      string     `json:"error,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// TaskFunc is the unit of work executed by TaskManager. The context belongs to
// the task manager (and therefore survives the HTTP request that submitted the
// task); it is canceled when the task is canceled or the server shuts down.
type TaskFunc func(context.Context, *TaskReporter) (any, error)

// TaskSpec describes one operation submitted to TaskManager.
type TaskSpec struct {
	Kind      string
	Scope     string
	ProjectID string

	// Resource serializes operations that touch one project. Tasks using
	// different resources can run in parallel. An empty resource means no
	// per-resource serialization.
	Resource string

	// Exclusive makes the task take the workspace write lock. Shared tasks take
	// the workspace read lock, allowing independent projects to proceed in
	// parallel while preventing a global catalog mutation from racing them.
	Exclusive bool

	// SkipWorkspaceLock runs the task without acquiring the workspace lock at
	// all. It is for fast, in-memory operations (e.g. manually blacklisting or
	// probing a single node) that must never be delayed behind a long-running
	// exclusive task such as a subscription refresh or config reload — otherwise
	// a blacklist issued mid-probe could sit queued until the probe finishes.
	SkipWorkspaceLock bool

	Message string
	Run     TaskFunc
}

var (
	ErrTaskQueueFull = errors.New("后台任务队列已满，请稍后重试")
	ErrTaskNotFound  = errors.New("后台任务不存在或已过期")
	ErrTaskClosed    = errors.New("后台任务管理器已关闭")
)

type taskRecord struct {
	snapshot TaskSnapshot
	run      TaskFunc
	ctx      context.Context
	cancel   context.CancelFunc

	subsMu      sync.Mutex
	subscribers map[uint64]chan TaskSnapshot
}

// TaskManager runs bounded, cancelable background work and provides a small
// in-memory event stream for HTTP polling/SSE clients. It intentionally avoids
// spawning one unbounded goroutine per request; the queue and worker limit are
// the main protection against a large burst of API calls exhausting memory or
// file descriptors.
type TaskManager struct {
	workers   int
	queue     chan *taskRecord
	retention time.Duration

	mu       sync.RWMutex
	tasks    map[string]*taskRecord
	closed   bool
	started  bool
	startMu  sync.Mutex
	cancel   context.CancelFunc
	ctx      context.Context
	workerWG sync.WaitGroup

	workspaceMu sync.RWMutex
	resourceMu  sync.Mutex
	resources   map[string]chan struct{}

	subscriberSeq atomic.Uint64
	cleanupDone   chan struct{}
}

const (
	defaultTaskQueueSize = 256
	defaultTaskRetention = 30 * time.Minute
)

// NewTaskManager constructs an unstarted task manager. Start is normally called
// by Server.Start; Submit also starts it lazily for tests and embedded users.
func NewTaskManager(workers int) *TaskManager {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
		if workers < 2 {
			workers = 2
		}
		if workers > 8 {
			workers = 8
		}
	}
	return &TaskManager{
		workers:   workers,
		queue:     make(chan *taskRecord, defaultTaskQueueSize),
		retention: defaultTaskRetention,
		tasks:     make(map[string]*taskRecord),
		resources: make(map[string]chan struct{}),
	}
}

// Start binds the manager to the application lifetime and launches its bounded
// worker pool. Calling Start more than once is harmless.
func (m *TaskManager) Start(parent context.Context) {
	if m == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	m.startMu.Lock()
	if m.started {
		m.startMu.Unlock()
		return
	}
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		m.startMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cancel()
		m.startMu.Unlock()
		return
	}
	m.ctx, m.cancel = ctx, cancel
	m.cleanupDone = make(chan struct{})
	m.started = true
	m.mu.Unlock()
	m.startMu.Unlock()

	for i := 0; i < m.workers; i++ {
		m.workerWG.Add(1)
		go m.worker()
	}
	go m.cleanupLoop()
}

// Submit queues a task without tying it to the lifetime of the submitting HTTP
// request. The returned snapshot can be used immediately to build a 202 API
// response. Queue saturation is reported synchronously so callers can retry
// instead of silently losing work.
func (m *TaskManager) Submit(spec TaskSpec) (TaskSnapshot, error) {
	if m == nil {
		return TaskSnapshot{}, ErrTaskClosed
	}
	if spec.Run == nil {
		return TaskSnapshot{}, errors.New("后台任务缺少执行函数")
	}
	m.Start(context.Background())

	id, err := newTaskID()
	if err != nil {
		return TaskSnapshot{}, err
	}
	now := time.Now().UTC()
	message := spec.Message
	if message == "" {
		message = "等待后台处理"
	}
	m.mu.RLock()
	closed := m.closed
	parent := m.ctx
	m.mu.RUnlock()
	if closed || parent == nil || parent.Err() != nil {
		return TaskSnapshot{}, ErrTaskClosed
	}
	ctx, cancel := context.WithCancel(withTaskIsolation(parent, taskIsolation{
		resource:          spec.Resource,
		exclusive:         spec.Exclusive,
		skipWorkspaceLock: spec.SkipWorkspaceLock,
	}))
	record := &taskRecord{
		snapshot: TaskSnapshot{
			ID:        id,
			Kind:      spec.Kind,
			Scope:     spec.Scope,
			ProjectID: spec.ProjectID,
			Status:    TaskQueued,
			Progress:  0,
			Message:   message,
			CreatedAt: now,
		},
		run:         spec.Run,
		ctx:         ctx,
		cancel:      cancel,
		subscribers: make(map[uint64]chan TaskSnapshot),
	}

	// Keep the closed check, task registration, and non-blocking enqueue under
	// one lock so shutdown cannot race a task into the queue after it has
	// already canceled the worker pool.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cancel()
		return TaskSnapshot{}, ErrTaskClosed
	}
	select {
	case m.queue <- record:
		m.tasks[id] = record
		snapshot := record.snapshotCopyLocked()
		m.mu.Unlock()
		return snapshot, nil
	default:
		m.mu.Unlock()
		cancel()
		return TaskSnapshot{}, ErrTaskQueueFull
	}
}

// Get returns the current state of one task.
func (m *TaskManager) Get(id string) (TaskSnapshot, error) {
	if m == nil {
		return TaskSnapshot{}, ErrTaskNotFound
	}
	m.mu.RLock()
	record := m.tasks[id]
	if record == nil {
		m.mu.RUnlock()
		return TaskSnapshot{}, ErrTaskNotFound
	}
	snapshot := record.snapshotCopyLocked()
	m.mu.RUnlock()
	return snapshot, nil
}

// List returns a stable copy of known tasks, newest first. It is intentionally
// capped so a client cannot turn a stale task history into an unbounded response.
func (m *TaskManager) List(limit int) []TaskSnapshot {
	if m == nil {
		return nil
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	m.mu.RLock()
	items := make([]TaskSnapshot, 0, len(m.tasks))
	for _, record := range m.tasks {
		items = append(items, record.snapshotCopyLocked())
	}
	m.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}

// Cancel requests cancellation. A running operation must still honor its
// context; cancellation is therefore best-effort and is reflected by the
// terminal canceled state once the worker observes it.
func (m *TaskManager) Cancel(id string) (TaskSnapshot, error) {
	if m == nil {
		return TaskSnapshot{}, ErrTaskNotFound
	}
	m.mu.RLock()
	record := m.tasks[id]
	m.mu.RUnlock()
	if record == nil {
		return TaskSnapshot{}, ErrTaskNotFound
	}
	record.cancel()
	m.mu.Lock()
	if record.snapshot.Status == TaskQueued {
		now := time.Now().UTC()
		record.snapshot.Status = TaskCanceled
		record.snapshot.Message = "任务已取消"
		record.snapshot.FinishedAt = &now
		m.publishLocked(record)
	}
	snapshot := record.snapshotCopyLocked()
	m.mu.Unlock()
	return snapshot, nil
}

// Subscribe returns a buffered stream containing the current snapshot followed
// by changes. The channel is closed after a terminal state or when unsubscribe
// is called. Updates are coalesced when a slow client falls behind; the newest
// state is always retained.
func (m *TaskManager) Subscribe(id string) (<-chan TaskSnapshot, func(), error) {
	if m == nil {
		return nil, func() {}, ErrTaskNotFound
	}
	m.mu.Lock()
	record := m.tasks[id]
	if record == nil {
		m.mu.Unlock()
		return nil, func() {}, ErrTaskNotFound
	}
	channel := make(chan TaskSnapshot, 8)
	snapshot := record.snapshotCopyLocked()
	if isTerminal(snapshot.Status) {
		channel <- snapshot
		close(channel)
		m.mu.Unlock()
		return channel, func() {}, nil
	}
	token := m.subscriberSeq.Add(1)
	record.subsMu.Lock()
	record.subscribers[token] = channel
	record.subsMu.Unlock()
	channel <- snapshot
	m.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			m.mu.Lock()
			record.subsMu.Lock()
			if _, ok := record.subscribers[token]; ok {
				delete(record.subscribers, token)
				close(channel)
			}
			record.subsMu.Unlock()
			m.mu.Unlock()
		})
	}
	return channel, unsubscribe, nil
}

// Close cancels queued/running work and waits for the bounded worker pool. It
// is idempotent and safe to call during both normal and error shutdown paths.
func (m *TaskManager) Close() {
	if m == nil {
		return
	}
	m.startMu.Lock()
	if !m.started {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		m.startMu.Unlock()
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.startMu.Unlock()
		return
	}
	m.closed = true
	if m.cancel != nil {
		m.cancel()
	}
	var queuedSubscribers []chan TaskSnapshot
	for _, record := range m.tasks {
		record.cancel()
		if record.snapshot.Status == TaskQueued {
			now := time.Now().UTC()
			record.snapshot.Status = TaskCanceled
			record.snapshot.Message = "服务正在关闭，任务已取消"
			record.snapshot.FinishedAt = &now
			m.publishLocked(record)
			record.subsMu.Lock()
			for token, channel := range record.subscribers {
				queuedSubscribers = append(queuedSubscribers, channel)
				delete(record.subscribers, token)
			}
			record.subsMu.Unlock()
		}
	}
	m.mu.Unlock()
	m.startMu.Unlock()
	for _, channel := range queuedSubscribers {
		close(channel)
	}
	if m.cleanupDone != nil {
		<-m.cleanupDone
	}
	m.workerWG.Wait()
}

// Update lets a task function expose lightweight progress without taking a
// lock in user code. A terminal task ignores late updates.
func (r *TaskReporter) Update(progress float64, message string, result any) {
	if r == nil || r.manager == nil {
		return
	}
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	r.manager.mu.Lock()
	record := r.manager.tasks[r.id]
	if record == nil || isTerminal(record.snapshot.Status) {
		r.manager.mu.Unlock()
		return
	}
	record.snapshot.Progress = progress
	if message != "" {
		record.snapshot.Message = message
	}
	if result != nil {
		record.snapshot.Result = result
	}
	r.manager.publishLocked(record)
	r.manager.mu.Unlock()
}

type TaskReporter struct {
	manager *TaskManager
	id      string
}

func (m *TaskManager) worker() {
	defer m.workerWG.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case record := <-m.queue:
			if record == nil {
				continue
			}
			m.run(record)
		}
	}
}

func (m *TaskManager) run(record *taskRecord) {
	if record.ctx.Err() != nil {
		m.finish(record, TaskCanceled, nil, record.ctx.Err())
		return
	}

	releaseWorkspace := func() {}
	// Isolation is attached to the record through the private context value.
	// The worker reads it before invoking the task so TaskFunc stays a small,
	// reusable API.
	if isolation, ok := taskIsolationFromContext(record.ctx); ok {
		if !isolation.skipWorkspaceLock {
			releaseWorkspace = m.acquireWorkspace(isolation.exclusive)
		}
		if isolation.resource != "" {
			releaseResource := m.acquireResource(record.ctx, isolation.resource)
			if releaseResource == nil {
				releaseWorkspace()
				m.finish(record, TaskCanceled, nil, record.ctx.Err())
				return
			}
			defer releaseResource()
		}
	}
	defer releaseWorkspace()

	m.mu.Lock()
	if record.snapshot.Status == TaskCanceled || record.ctx.Err() != nil {
		m.mu.Unlock()
		m.finish(record, TaskCanceled, nil, record.ctx.Err())
		return
	}
	now := time.Now().UTC()
	record.snapshot.Status = TaskRunning
	record.snapshot.StartedAt = &now
	record.snapshot.Message = "正在处理"
	m.publishLocked(record)
	m.mu.Unlock()

	reporter := &TaskReporter{manager: m, id: record.snapshot.ID}
	var result any
	var runErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				runErr = fmt.Errorf("后台任务异常: %v", recovered)
			}
		}()
		result, runErr = record.run(record.ctx, reporter)
	}()
	if record.ctx.Err() != nil && (runErr == nil || errors.Is(runErr, context.Canceled)) {
		m.finish(record, TaskCanceled, result, record.ctx.Err())
		return
	}
	if runErr != nil {
		m.finish(record, TaskFailed, result, runErr)
		return
	}
	m.finish(record, TaskSucceeded, result, nil)
}

func (m *TaskManager) finish(record *taskRecord, status TaskStatus, result any, runErr error) {
	m.mu.Lock()
	if current := m.tasks[record.snapshot.ID]; current == nil {
		m.mu.Unlock()
		return
	}
	record.snapshot.Status = status
	if status == TaskSucceeded {
		record.snapshot.Progress = 100
		record.snapshot.Message = "处理完成"
	} else if status == TaskFailed {
		record.snapshot.Message = "处理失败"
	} else if status == TaskCanceled {
		record.snapshot.Message = "任务已取消"
	}
	if result != nil {
		record.snapshot.Result = result
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		record.snapshot.Error = runErr.Error()
	}
	now := time.Now().UTC()
	record.snapshot.FinishedAt = &now
	m.publishLocked(record)
	// Detach subscribers under the manager lock, then close their channels after
	// releasing it. This keeps unsubscribe and completion race-free.
	record.subsMu.Lock()
	subs := make([]chan TaskSnapshot, 0, len(record.subscribers))
	for token, channel := range record.subscribers {
		subs = append(subs, channel)
		delete(record.subscribers, token)
	}
	record.subsMu.Unlock()
	m.mu.Unlock()
	for _, channel := range subs {
		close(channel)
	}
}

func (m *TaskManager) publishLocked(record *taskRecord) {
	snapshot := record.snapshotCopyLocked()
	record.subsMu.Lock()
	defer record.subsMu.Unlock()
	for _, channel := range record.subscribers {
		select {
		case channel <- snapshot:
		default:
			// Drop the oldest buffered update, then retain the newest one.
			select {
			case <-channel:
			default:
			}
			select {
			case channel <- snapshot:
			default:
			}
		}
	}
}

func (record *taskRecord) snapshotCopy() TaskSnapshot {
	return record.snapshot
}

func (record *taskRecord) snapshotCopyLocked() TaskSnapshot {
	return record.snapshot
}

func (m *TaskManager) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	defer close(m.cleanupDone)
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-ticker.C:
			cutoff := now.Add(-m.retention)
			m.mu.Lock()
			for id, record := range m.tasks {
				if record.snapshot.FinishedAt != nil && record.snapshot.FinishedAt.Before(cutoff) {
					delete(m.tasks, id)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *TaskManager) acquireWorkspace(exclusive bool) func() {
	if exclusive {
		m.workspaceMu.Lock()
		return m.workspaceMu.Unlock
	}
	m.workspaceMu.RLock()
	return m.workspaceMu.RUnlock
}

func (m *TaskManager) acquireResource(ctx context.Context, resource string) func() {
	m.resourceMu.Lock()
	lock := m.resources[resource]
	if lock == nil {
		lock = make(chan struct{}, 1)
		lock <- struct{}{}
		m.resources[resource] = lock
	}
	m.resourceMu.Unlock()
	select {
	case <-lock:
		return func() { lock <- struct{}{} }
	case <-ctx.Done():
		return nil
	}
}

type taskIsolation struct {
	resource          string
	exclusive         bool
	skipWorkspaceLock bool
}

type taskIsolationKey struct{}

// withTaskIsolation stores lock metadata in a derived context without exposing
// it to callers. It keeps TaskSpec and TaskFunc independent of the scheduler's
// internal locking implementation.
func withTaskIsolation(ctx context.Context, isolation taskIsolation) context.Context {
	return context.WithValue(ctx, taskIsolationKey{}, isolation)
}

func taskIsolationFromContext(ctx context.Context) (taskIsolation, bool) {
	value, ok := ctx.Value(taskIsolationKey{}).(taskIsolation)
	return value, ok
}

func newTaskID() (string, error) {
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("生成后台任务 ID 失败: %w", err)
	}
	return "task_" + hex.EncodeToString(bytes[:]), nil
}

func isTerminal(status TaskStatus) bool {
	return status == TaskSucceeded || status == TaskFailed || status == TaskCanceled
}
