package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	bolt "github.com/sagernet/bbolt"
)

const (
	FileName         = ".proxy2api-state.db"
	currentVersion   = 2
	defaultFlush     = time.Second
	defaultRetention = 30 * 24 * time.Hour
)

var (
	bucketMeta          = []byte("meta")
	bucketNodes         = []byte("nodes")
	bucketSubscriptions = []byte("subscriptions")
	bucketTraffic       = []byte("traffic_daily")
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

type TrafficDay struct {
	Date          string    `json:"date"`
	UploadBytes   int64     `json:"upload_bytes"`
	DownloadBytes int64     `json:"download_bytes"`
	TotalBytes    int64     `json:"total_bytes"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type TrafficMonth struct {
	Month         string                `json:"month"`
	Days          []TrafficDay          `json:"days"`
	Projects      []ProjectTrafficMonth `json:"projects,omitempty"`
	UploadBytes   int64                 `json:"upload_bytes"`
	DownloadBytes int64                 `json:"download_bytes"`
	TotalBytes    int64                 `json:"total_bytes"`
}

type ProjectTrafficMonth struct {
	ProjectID     string       `json:"project_id"`
	ProjectName   string       `json:"project_name"`
	Days          []TrafficDay `json:"days"`
	UploadBytes   int64        `json:"upload_bytes"`
	DownloadBytes int64        `json:"download_bytes"`
	TotalBytes    int64        `json:"total_bytes"`
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

	writeMu        sync.Mutex
	mu             sync.Mutex
	pendingNodes   map[string]NodeRecord
	pendingSub     *SubscriptionState
	pendingTraffic map[string]TrafficDay
	wake           chan struct{}
	stop           chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
}

func NodeID(nodeKey string) string {
	sum := sha256.Sum256([]byte(nodeKey))
	return hex.EncodeToString(sum[:])
}

func Open(configPath string) (*Store, error) {
	if configPath == "" {
		return nil, errors.New("状态存储缺少配置路径")
	}
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建状态目录失败: %w", err)
	}
	db, err := bolt.Open(filepath.Join(dir, FileName), 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("打开状态数据库失败: %w", err)
	}
	s := &Store{
		db:             db,
		flushEvery:     defaultFlush,
		retention:      defaultRetention,
		pendingNodes:   make(map[string]NodeRecord),
		pendingTraffic: make(map[string]TrafficDay),
		wake:           make(chan struct{}, 1),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
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
		return nil, subscriptions, false, false, nil, fmt.Errorf("打开恢复状态失败: %w", err)
	}
	defer db.Close()
	foundSubscription := false
	foundCatalog := false
	err = db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket(bucketMeta); bucket != nil {
			foundCatalog = bucket.Get(keyCatalogCommitted) != nil
			if value := bucket.Get(keyCatalogSubs); value != nil {
				if err := json.Unmarshal(value, &catalogSubscriptions); err != nil {
					return fmt.Errorf("解析恢复目录中的订阅失败: %w", err)
				}
			}
		}
		if bucket := tx.Bucket(bucketNodes); bucket != nil {
			if err := bucket.ForEach(func(key, value []byte) error {
				var record NodeRecord
				if err := json.Unmarshal(value, &record); err != nil {
					return fmt.Errorf("解析恢复节点 %q 失败: %w", key, err)
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
					return fmt.Errorf("解析恢复订阅状态失败: %w", err)
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
		if _, err = tx.CreateBucketIfNotExists(bucketTraffic); err != nil {
			return err
		}
		if existing := meta.Get(keyVersion); existing != nil {
			var version int
			if err := json.Unmarshal(existing, &version); err != nil {
				return fmt.Errorf("解析状态数据库版本失败: %w", err)
			}
			if version > currentVersion {
				return fmt.Errorf("状态数据库版本 %d 高于当前支持的版本 %d", version, currentVersion)
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
				return fmt.Errorf("解析节点状态 %q 失败: %w", key, err)
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

// AddTraffic queues one measured traffic interval for the local calendar day.
func (s *Store) AddTraffic(at time.Time, uploadBytes, downloadBytes int64) {
	if s == nil || (uploadBytes <= 0 && downloadBytes <= 0) {
		return
	}
	if uploadBytes < 0 {
		uploadBytes = 0
	}
	if downloadBytes < 0 {
		downloadBytes = 0
	}
	if at.IsZero() {
		at = time.Now()
	}
	date := at.In(time.Local).Format("2006-01-02")
	s.mu.Lock()
	record := s.pendingTraffic[date]
	record.Date = date
	record.UploadBytes += uploadBytes
	record.DownloadBytes += downloadBytes
	record.TotalBytes = record.UploadBytes + record.DownloadBytes
	record.UpdatedAt = at.UTC()
	s.pendingTraffic[date] = record
	s.mu.Unlock()
	s.signal()
}

// LoadTrafficMonth returns persisted and not-yet-flushed daily totals.
func (s *Store) LoadTrafficMonth(month string) (TrafficMonth, error) {
	if s == nil {
		return TrafficMonth{}, errors.New("流量状态库未启用")
	}
	if err := validateTrafficMonth(month); err != nil {
		return TrafficMonth{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := loadTrafficMonthFromDB(s.db, month)
	if err != nil {
		return TrafficMonth{}, err
	}
	prefix := month + "-"
	s.mu.Lock()
	for date, pending := range s.pendingTraffic {
		if len(date) >= len(prefix) && date[:len(prefix)] == prefix {
			mergeTrafficDay(&result, pending)
		}
	}
	s.mu.Unlock()
	return finalizeTrafficMonth(result), nil
}

// LoadTrafficMonthSnapshot reads a stopped project's traffic history without
// starting its proxy runtime.
func LoadTrafficMonthSnapshot(configPath, month string) (TrafficMonth, error) {
	if err := validateTrafficMonth(month); err != nil {
		return TrafficMonth{}, err
	}
	path := filepath.Join(filepath.Dir(configPath), FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return TrafficMonth{Month: month, Days: []TrafficDay{}}, nil
		}
		return TrafficMonth{}, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return TrafficMonth{}, fmt.Errorf("打开流量历史失败: %w", err)
	}
	defer db.Close()
	return loadTrafficMonthFromDB(db, month)
}

func validateTrafficMonth(month string) error {
	if _, err := time.Parse("2006-01", month); err != nil {
		return fmt.Errorf("月份必须使用 YYYY-MM 格式")
	}
	return nil
}

func loadTrafficMonthFromDB(db *bolt.DB, month string) (TrafficMonth, error) {
	result := TrafficMonth{Month: month, Days: []TrafficDay{}}
	prefix := []byte(month + "-")
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketTraffic)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			var day TrafficDay
			if err := json.Unmarshal(value, &day); err != nil {
				return fmt.Errorf("解析流量记录 %q 失败: %w", key, err)
			}
			if day.Date == "" {
				day.Date = string(key)
			}
			mergeTrafficDay(&result, day)
		}
		return nil
	})
	if err != nil {
		return TrafficMonth{}, err
	}
	return finalizeTrafficMonth(result), nil
}

func mergeTrafficDay(month *TrafficMonth, addition TrafficDay) {
	for i := range month.Days {
		if month.Days[i].Date != addition.Date {
			continue
		}
		month.Days[i].UploadBytes += addition.UploadBytes
		month.Days[i].DownloadBytes += addition.DownloadBytes
		month.Days[i].TotalBytes = month.Days[i].UploadBytes + month.Days[i].DownloadBytes
		if addition.UpdatedAt.After(month.Days[i].UpdatedAt) {
			month.Days[i].UpdatedAt = addition.UpdatedAt
		}
		return
	}
	addition.TotalBytes = addition.UploadBytes + addition.DownloadBytes
	month.Days = append(month.Days, addition)
}

func finalizeTrafficMonth(month TrafficMonth) TrafficMonth {
	month.UploadBytes = 0
	month.DownloadBytes = 0
	for i := range month.Days {
		month.Days[i].TotalBytes = month.Days[i].UploadBytes + month.Days[i].DownloadBytes
		month.UploadBytes += month.Days[i].UploadBytes
		month.DownloadBytes += month.Days[i].DownloadBytes
	}
	month.TotalBytes = month.UploadBytes + month.DownloadBytes
	sort.Slice(month.Days, func(i, j int) bool { return month.Days[i].Date < month.Days[j].Date })
	return month
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
		return errors.New("节点状态缺少 ID")
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
	traffic := s.pendingTraffic
	s.pendingTraffic = make(map[string]TrafficDay)
	s.mu.Unlock()
	if len(nodes) == 0 && subscription == nil && len(traffic) == 0 {
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
			writeSubscription := true
			if existing := subBucket.Get(keyGlobal); existing != nil {
				var current SubscriptionState
				if json.Unmarshal(existing, &current) == nil && current.UpdatedAt.After(subscription.UpdatedAt) {
					writeSubscription = false
				}
			}
			if writeSubscription {
				data, err := json.Marshal(subscription)
				if err != nil {
					return err
				}
				if err := subBucket.Put(keyGlobal, data); err != nil {
					return err
				}
			}
		}
		trafficBucket := tx.Bucket(bucketTraffic)
		for date, increment := range traffic {
			current := TrafficDay{Date: date}
			if existing := trafficBucket.Get([]byte(date)); existing != nil {
				if err := json.Unmarshal(existing, &current); err != nil {
					return fmt.Errorf("解析流量记录 %q 失败: %w", date, err)
				}
			}
			current.Date = date
			current.UploadBytes += increment.UploadBytes
			current.DownloadBytes += increment.DownloadBytes
			current.TotalBytes = current.UploadBytes + current.DownloadBytes
			if increment.UpdatedAt.After(current.UpdatedAt) {
				current.UpdatedAt = increment.UpdatedAt
			}
			data, err := json.Marshal(current)
			if err != nil {
				return err
			}
			if err := trafficBucket.Put([]byte(date), data); err != nil {
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
		for date, increment := range traffic {
			pending := s.pendingTraffic[date]
			pending.Date = date
			pending.UploadBytes += increment.UploadBytes
			pending.DownloadBytes += increment.DownloadBytes
			pending.TotalBytes = pending.UploadBytes + pending.DownloadBytes
			if increment.UpdatedAt.After(pending.UpdatedAt) {
				pending.UpdatedAt = increment.UpdatedAt
			}
			s.pendingTraffic[date] = pending
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
				log.Printf("[状态] 后台写入失败: %v", err)
			}
			armed = false
		}
	}
}
