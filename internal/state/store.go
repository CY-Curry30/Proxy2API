package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "github.com/sagernet/bbolt"
)

const (
	FileName         = ".proxy2api-state.db"
	currentVersion   = 1
	defaultFlush     = time.Second
	defaultRetention = 30 * 24 * time.Hour
)

var (
	bucketMeta          = []byte("meta")
	bucketNodes         = []byte("nodes")
	bucketSubscriptions = []byte("subscriptions")
	keyVersion          = []byte("version")
	keyCatalogCommitted = []byte("catalog_committed_at")
	keyCatalogSubs      = []byte("catalog_subscriptions")
	keyGlobal           = []byte("global")
)

type TimelineEvent struct {
	Time      time.Time `json:"time"`
	Success   bool      `json:"success"`
	LatencyMS int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}

type NodeRecord struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	URI              string          `json:"uri"`
	Source           string          `json:"source,omitempty"`
	SubscriptionURL  string          `json:"subscription_url,omitempty"`
	Port             uint16          `json:"port,omitempty"`
	Username         string          `json:"username,omitempty"`
	Password         string          `json:"password,omitempty"`
	Disabled         bool            `json:"disabled,omitempty"`
	Order            int             `json:"order,omitempty"`
	Active           bool            `json:"active"`
	IP               string          `json:"ip,omitempty"`
	Region           string          `json:"region,omitempty"`
	Country          string          `json:"country,omitempty"`
	FailureCount     int             `json:"failure_count"`
	SuccessCount     int64           `json:"success_count"`
	ConsecutiveFails int             `json:"consecutive_failures"`
	Blacklisted      bool            `json:"blacklisted"`
	BlacklistedUntil time.Time       `json:"blacklisted_until,omitempty"`
	LastError        string          `json:"last_error,omitempty"`
	LastFailure      time.Time       `json:"last_failure,omitempty"`
	LastSuccess      time.Time       `json:"last_success,omitempty"`
	LastProbeLatency time.Duration   `json:"last_probe_latency,omitempty"`
	InitialCheckDone bool            `json:"initial_check_done"`
	Available        bool            `json:"available"`
	Timeline         []TimelineEvent `json:"timeline,omitempty"`
	LastSeen         time.Time       `json:"last_seen"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type SubscriptionInfo struct {
	ID             string    `json:"id"`
	URL            string    `json:"url"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	NodeCount      int       `json:"node_count"`
	Included       bool      `json:"included"`
	Enabled        bool      `json:"enabled"`
	UploadBytes    int64     `json:"upload_bytes"`
	DownloadBytes  int64     `json:"download_bytes"`
	UsedBytes      int64     `json:"used_bytes"`
	TotalBytes     int64     `json:"total_bytes"`
	RemainingBytes int64     `json:"remaining_bytes"`
	ExpiresAt      int64     `json:"expires_at"`
	LastRefresh    time.Time `json:"last_refresh"`
	LastError      string    `json:"last_error,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type SubscriptionState struct {
	LastRefresh  time.Time                   `json:"last_refresh"`
	NextRefresh  time.Time                   `json:"next_refresh"`
	NodeCount    int                         `json:"node_count"`
	LastError    string                      `json:"last_error,omitempty"`
	RefreshCount int                         `json:"refresh_count"`
	Items        []SubscriptionInfo          `json:"items,omitempty"`
	NodeCache    map[string][]NodeDefinition `json:"node_cache,omitempty"`
	UpdatedAt    time.Time                   `json:"updated_at"`
}

type NodeDefinition struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name"`
	URI             string `json:"uri"`
	Port            uint16 `json:"port,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
	Source          string `json:"source,omitempty"`
	SubscriptionURL string `json:"subscription_url,omitempty"`
	Disabled        bool   `json:"disabled,omitempty"`
}

type Store struct {
	db         *bolt.DB
	flushEvery time.Duration
	retention  time.Duration

	writeMu      sync.Mutex
	mu           sync.Mutex
	pendingNodes map[string]NodeRecord
	pendingSub   *SubscriptionState
	wake         chan struct{}
	stop         chan struct{}
	done         chan struct{}
	closeOnce    sync.Once
}

func NodeID(nodeKey string) string {
	sum := sha256.Sum256([]byte(nodeKey))
	return hex.EncodeToString(sum[:])
}

func Open(configPath string) (*Store, error) {
	if configPath == "" {
		return nil, errors.New("state store requires a config path")
	}
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	db, err := bolt.Open(filepath.Join(dir, FileName), 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	s := &Store{
		db:           db,
		flushEvery:   defaultFlush,
		retention:    defaultRetention,
		pendingNodes: make(map[string]NodeRecord),
		wake:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if err := s.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.Prune(time.Now()); err != nil {
		_ = db.Close()
		return nil, err
	}
	go s.flushLoop()
	return s, nil
}

// LoadRecoverySnapshot reads committed state before the long-lived store is
// opened. A missing database is expected on first startup.
func LoadRecoverySnapshot(configPath string) (map[string]NodeRecord, SubscriptionState, bool, bool, []string, error) {
	nodes := make(map[string]NodeRecord)
	var subscriptions SubscriptionState
	var catalogSubscriptions []string
	path := filepath.Join(filepath.Dir(configPath), FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nodes, subscriptions, false, false, nil, nil
		}
		return nil, subscriptions, false, false, nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return nil, subscriptions, false, false, nil, fmt.Errorf("open recovery state: %w", err)
	}
	defer db.Close()
	foundSubscription := false
	foundCatalog := false
	err = db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket(bucketMeta); bucket != nil {
			foundCatalog = bucket.Get(keyCatalogCommitted) != nil
			if value := bucket.Get(keyCatalogSubs); value != nil {
				if err := json.Unmarshal(value, &catalogSubscriptions); err != nil {
					return fmt.Errorf("decode recovery catalog subscriptions: %w", err)
				}
			}
		}
		if bucket := tx.Bucket(bucketNodes); bucket != nil {
			if err := bucket.ForEach(func(key, value []byte) error {
				var record NodeRecord
				if err := json.Unmarshal(value, &record); err != nil {
					return fmt.Errorf("decode recovery node %q: %w", key, err)
				}
				nodes[string(key)] = record
				return nil
			}); err != nil {
				return err
			}
		}
		if bucket := tx.Bucket(bucketSubscriptions); bucket != nil {
			if value := bucket.Get(keyGlobal); value != nil {
				foundSubscription = true
				if err := json.Unmarshal(value, &subscriptions); err != nil {
					return fmt.Errorf("decode recovery subscription state: %w", err)
				}
			}
		}
		return nil
	})
	return nodes, subscriptions, foundSubscription, foundCatalog, catalogSubscriptions, err
}

func (s *Store) initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketNodes); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketSubscriptions); err != nil {
			return err
		}
		if existing := meta.Get(keyVersion); existing != nil {
			var version int
			if err := json.Unmarshal(existing, &version); err != nil {
				return fmt.Errorf("decode state database version: %w", err)
			}
			if version > currentVersion {
				return fmt.Errorf("state database version %d is newer than supported version %d", version, currentVersion)
			}
		}
		version, err := json.Marshal(currentVersion)
		if err != nil {
			return err
		}
		return meta.Put(keyVersion, version)
	})
}

func (s *Store) LoadNodes() (map[string]NodeRecord, error) {
	result := make(map[string]NodeRecord)
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketNodes)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			var record NodeRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decode node state %q: %w", key, err)
			}
			result[string(key)] = record
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	for id, record := range s.pendingNodes {
		result[id] = record
	}
	s.mu.Unlock()
	return result, nil
}

func (s *Store) LoadNode(id string) (NodeRecord, bool, error) {
	s.mu.Lock()
	if record, ok := s.pendingNodes[id]; ok {
		s.mu.Unlock()
		return record, true, nil
	}
	s.mu.Unlock()
	var result NodeRecord
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(bucketNodes).Get([]byte(id))
		if value == nil {
			return nil
		}
		found = true
		return json.Unmarshal(value, &result)
	})
	return result, found, err
}

// ReconcileActiveNodes atomically records the node set belonging to the last
// successfully started proxy configuration. Historical records are retained as
// inactive entries until the retention window expires.
func (s *Store) ReconcileActiveNodes(records []NodeRecord, subscriptionURLs []string) error {
	now := time.Now().UTC()
	active := make(map[string]NodeRecord, len(records))
	for _, record := range records {
		if record.ID == "" {
			continue
		}
		record.Active = true
		record.LastSeen = now
		record.UpdatedAt = now
		active[record.ID] = record
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	pendingSnapshot := make(map[string]NodeRecord, len(s.pendingNodes))
	for id, record := range s.pendingNodes {
		pendingSnapshot[id] = record
	}
	s.mu.Unlock()
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketNodes)
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			id := string(key)
			if _, ok := active[id]; ok {
				continue
			}
			var existing NodeRecord
			if json.Unmarshal(value, &existing) != nil {
				continue
			}
			if existing.Active {
				existing.Active = false
				existing.LastSeen = now
			}
			existing.UpdatedAt = now
			data, err := json.Marshal(existing)
			if err != nil {
				return err
			}
			if err := bucket.Put(key, data); err != nil {
				return err
			}
		}
		for id, record := range active {
			if queued, ok := pendingSnapshot[id]; ok {
				mergeNodeRuntime(&record, queued)
			} else if existingData := bucket.Get([]byte(id)); existingData != nil {
				var existing NodeRecord
				if json.Unmarshal(existingData, &existing) == nil {
					mergeNodeRuntime(&record, existing)
				}
			}
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(id), data); err != nil {
				return err
			}
		}
		committedAt, err := json.Marshal(now)
		if err != nil {
			return err
		}
		meta := tx.Bucket(bucketMeta)
		if err := meta.Put(keyCatalogCommitted, committedAt); err != nil {
			return err
		}
		catalogSubscriptions, err := json.Marshal(subscriptionURLs)
		if err != nil {
			return err
		}
		return meta.Put(keyCatalogSubs, catalogSubscriptions)
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	for id, pending := range s.pendingNodes {
		if !pending.UpdatedAt.After(now) {
			delete(s.pendingNodes, id)
		}
	}
	s.mu.Unlock()
	return nil
}

func mergeNodeRuntime(target *NodeRecord, existing NodeRecord) {
	target.IP = existing.IP
	target.Region = existing.Region
	target.Country = existing.Country
	target.FailureCount = existing.FailureCount
	target.SuccessCount = existing.SuccessCount
	target.ConsecutiveFails = existing.ConsecutiveFails
	target.Blacklisted = existing.Blacklisted
	target.BlacklistedUntil = existing.BlacklistedUntil
	target.LastError = existing.LastError
	target.LastFailure = existing.LastFailure
	target.LastSuccess = existing.LastSuccess
	target.LastProbeLatency = existing.LastProbeLatency
	target.InitialCheckDone = existing.InitialCheckDone
	target.Available = existing.Available
	target.Timeline = existing.Timeline
}

func (s *Store) LoadSubscriptionState() (SubscriptionState, bool, error) {
	var result SubscriptionState
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketSubscriptions)
		if bucket == nil {
			return nil
		}
		value := bucket.Get(keyGlobal)
		if value == nil {
			return nil
		}
		found = true
		return json.Unmarshal(value, &result)
	})
	return result, found, err
}

func (s *Store) QueueNode(record NodeRecord) {
	if record.ID == "" {
		return
	}
	record.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	s.pendingNodes[record.ID] = record
	s.mu.Unlock()
	s.signal()
}

func (s *Store) SaveNodeNow(record NodeRecord) error {
	if record.ID == "" {
		return errors.New("node state requires an id")
	}
	record.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if pending, ok := s.pendingNodes[record.ID]; ok && !pending.UpdatedAt.After(record.UpdatedAt) {
		delete(s.pendingNodes, record.ID)
	}
	s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketNodes)
		if existing := bucket.Get([]byte(record.ID)); existing != nil {
			var current NodeRecord
			if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(record.UpdatedAt) {
				return nil
			}
		}
		return bucket.Put([]byte(record.ID), data)
	})
}

// SaveNodesNow persists a coherent set of node snapshots in one transaction.
// It is used during reload and shutdown, where opening one transaction per node
// would make large subscription pools unnecessarily slow.
func (s *Store) SaveNodesNow(records []NodeRecord) error {
	if len(records) == 0 {
		return nil
	}
	now := time.Now().UTC()
	prepared := make(map[string]NodeRecord, len(records))
	for _, record := range records {
		if record.ID == "" {
			continue
		}
		record.UpdatedAt = now
		prepared[record.ID] = record
	}
	if len(prepared) == 0 {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	for id, record := range prepared {
		if pending, ok := s.pendingNodes[id]; ok && !pending.UpdatedAt.After(record.UpdatedAt) {
			delete(s.pendingNodes, id)
		}
	}
	s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketNodes)
		for id, record := range prepared {
			if existing := bucket.Get([]byte(id)); existing != nil {
				var current NodeRecord
				if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(record.UpdatedAt) {
					continue
				}
			}
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(id), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) QueueSubscriptionState(value SubscriptionState) {
	value.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	s.pendingSub = &value
	s.mu.Unlock()
	s.signal()
}

func (s *Store) SaveSubscriptionStateNow(value SubscriptionState) error {
	value.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.pendingSub != nil && !s.pendingSub.UpdatedAt.After(value.UpdatedAt) {
		s.pendingSub = nil
	}
	s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketSubscriptions)
		if existing := bucket.Get(keyGlobal); existing != nil {
			var current SubscriptionState
			if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(value.UpdatedAt) {
				return nil
			}
		}
		return bucket.Put(keyGlobal, data)
	})
}

func (s *Store) Prune(now time.Time) error {
	cutoff := now.Add(-s.retention)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketNodes)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var record NodeRecord
			if json.Unmarshal(value, &record) != nil {
				continue
			}
			if !record.Active && !record.LastSeen.IsZero() && record.LastSeen.Before(cutoff) {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) Flush() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	nodes := s.pendingNodes
	s.pendingNodes = make(map[string]NodeRecord)
	subscription := s.pendingSub
	s.pendingSub = nil
	s.mu.Unlock()
	if len(nodes) == 0 && subscription == nil {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		nodeBucket := tx.Bucket(bucketNodes)
		for id, record := range nodes {
			if existing := nodeBucket.Get([]byte(id)); existing != nil {
				var current NodeRecord
				if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(record.UpdatedAt) {
					continue
				}
			}
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := nodeBucket.Put([]byte(id), data); err != nil {
				return err
			}
		}
		if subscription != nil {
			subBucket := tx.Bucket(bucketSubscriptions)
			if existing := subBucket.Get(keyGlobal); existing != nil {
				var current SubscriptionState
				if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(subscription.UpdatedAt) {
					return nil
				}
			}
			data, err := json.Marshal(subscription)
			if err != nil {
				return err
			}
			if err := subBucket.Put(keyGlobal, data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.mu.Lock()
		for id, record := range nodes {
			if current, ok := s.pendingNodes[id]; !ok || current.UpdatedAt.Before(record.UpdatedAt) {
				s.pendingNodes[id] = record
			}
		}
		if subscription != nil && (s.pendingSub == nil || s.pendingSub.UpdatedAt.Before(subscription.UpdatedAt)) {
			s.pendingSub = subscription
		}
		s.mu.Unlock()
	}
	return err
}

func (s *Store) Close() error {
	var result error
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		if err := s.Flush(); err != nil {
			result = err
		}
		if err := s.db.Sync(); err != nil && result == nil {
			result = err
		}
		if err := s.db.Close(); err != nil && result == nil {
			result = err
		}
	})
	return result
}

func (s *Store) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Store) flushLoop() {
	defer close(s.done)
	timer := time.NewTimer(s.flushEvery)
	defer timer.Stop()
	armed := false
	if !timer.Stop() {
		<-timer.C
	}
	for {
		select {
		case <-s.stop:
			return
		case <-s.wake:
			if !armed {
				timer.Reset(s.flushEvery)
				armed = true
			}
		case <-timer.C:
			if err := s.Flush(); err != nil {
				log.Printf("[state] background flush failed: %v", err)
			}
			armed = false
		}
	}
}
