package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/sagernet/bbolt"
)

var bucketNodeUsage = []byte("node_usage")

// TrafficUsage counts bytes transferred through a node's proxy connections.
// Provider-reported subscription quotas are stored separately.
type TrafficUsage struct {
	UploadBytes   int64 `json:"upload_bytes"`
	DownloadBytes int64 `json:"download_bytes"`
	TotalBytes    int64 `json:"total_bytes"`
}

func (u TrafficUsage) plus(other TrafficUsage) TrafficUsage {
	u.UploadBytes += other.UploadBytes
	u.DownloadBytes += other.DownloadBytes
	u.TotalBytes = u.UploadBytes + u.DownloadBytes
	return u
}

// NodeUsageRecord retains the subscription that owned the connection when it
// opened. Historical usage survives node removal and subscription refreshes.
type NodeUsageRecord struct {
	NodeID          string `json:"node_id"`
	SubscriptionURL string `json:"subscription_url,omitempty"`
	TrafficUsage
}

func (r NodeUsageRecord) key() string {
	return r.NodeID + "\x00" + r.SubscriptionURL
}

type UsageTotals struct {
	Nodes         map[string]TrafficUsage
	Subscriptions map[string]TrafficUsage
}

func (t *UsageTotals) add(record NodeUsageRecord) {
	if t.Nodes == nil {
		t.Nodes = make(map[string]TrafficUsage)
	}
	t.Nodes[record.NodeID] = t.Nodes[record.NodeID].plus(record.TrafficUsage)
	if record.SubscriptionURL != "" {
		if t.Subscriptions == nil {
			t.Subscriptions = make(map[string]TrafficUsage)
		}
		t.Subscriptions[record.SubscriptionURL] = t.Subscriptions[record.SubscriptionURL].plus(record.TrafficUsage)
	}
}

func (t *UsageTotals) Merge(other UsageTotals) {
	if t.Nodes == nil {
		t.Nodes = make(map[string]TrafficUsage)
	}
	if t.Subscriptions == nil {
		t.Subscriptions = make(map[string]TrafficUsage)
	}
	for id, usage := range other.Nodes {
		t.Nodes[id] = t.Nodes[id].plus(usage)
	}
	for rawURL, usage := range other.Subscriptions {
		t.Subscriptions[rawURL] = t.Subscriptions[rawURL].plus(usage)
	}
}

// UsageCounter is captured by a connection so reloads cannot change which node
// or subscription receives its bytes. Network I/O only updates atomic counters;
// the store batches durable writes on its normal flush interval.
type UsageCounter struct {
	nodeID          string
	subscriptionURL string
	tracker         *UsageTracker
	value           atomic.Pointer[usageValue]
	onChange        func()
	flushed         TrafficUsage // guarded by Store.writeMu
}

type usageValue struct {
	generation uint64
	upload     atomic.Int64
	download   atomic.Int64
}

// current keeps existing connections usable across a reset. An increment that
// already captured the previous generation cannot restore cleared history.
func (c *UsageCounter) current() *usageValue {
	for {
		generation := c.tracker.generation.Load()
		value := c.value.Load()
		// Another goroutine may already have installed a newer generation
		// after our load. Never replace it with an older one.
		if value != nil && value.generation >= generation {
			return value
		}
		next := &usageValue{generation: generation}
		if c.value.CompareAndSwap(value, next) {
			return next
		}
	}
}

func (c *UsageCounter) Add(upload, download int64) {
	if c == nil || (upload <= 0 && download <= 0) {
		return
	}
	value := c.current()
	if upload > 0 {
		value.upload.Add(upload)
	}
	if download > 0 {
		value.download.Add(download)
	}
	if c.onChange != nil {
		c.onChange()
	}
}

func (c *UsageCounter) record() NodeUsageRecord {
	value := c.current()
	upload, download := value.upload.Load(), value.download.Load()
	return NodeUsageRecord{
		NodeID: c.nodeID, SubscriptionURL: c.subscriptionURL,
		TrafficUsage: TrafficUsage{UploadBytes: upload, DownloadBytes: download, TotalBytes: upload + download},
	}
}

type UsageTracker struct {
	counters   sync.Map // node ID + subscription URL -> *UsageCounter
	generation atomic.Uint64
	onChange   func()
}

func NewUsageTracker(onChange func()) *UsageTracker {
	return &UsageTracker{onChange: onChange}
}

func (t *UsageTracker) Counter(nodeID, subscriptionURL string) *UsageCounter {
	if t == nil || nodeID == "" {
		return nil
	}
	record := NodeUsageRecord{NodeID: nodeID, SubscriptionURL: strings.TrimSpace(subscriptionURL)}
	if existing, ok := t.counters.Load(record.key()); ok {
		return existing.(*UsageCounter)
	}
	counter := &UsageCounter{nodeID: record.NodeID, subscriptionURL: record.SubscriptionURL, tracker: t, onChange: t.onChange}
	actual, _ := t.counters.LoadOrStore(record.key(), counter)
	return actual.(*UsageCounter)
}

func (t *UsageTracker) Records() []NodeUsageRecord {
	var records []NodeUsageRecord
	if t == nil {
		return records
	}
	t.counters.Range(func(_, value any) bool {
		record := value.(*UsageCounter).record()
		if record.TotalBytes > 0 {
			records = append(records, record)
		}
		return true
	})
	sort.Slice(records, func(i, j int) bool { return records[i].key() < records[j].key() })
	return records
}

func (t *UsageTracker) Totals() UsageTotals {
	var totals UsageTotals
	if t != nil {
		t.counters.Range(func(_, value any) bool {
			record := value.(*UsageCounter).record()
			if record.TotalBytes > 0 {
				totals.add(record)
			}
			return true
		})
	}
	return totals
}

func (s *Store) UsageTracker() *UsageTracker { return s.usage }

func (s *Store) LoadTrafficUsage() (UsageTotals, error) {
	if s == nil {
		return UsageTotals{}, nil
	}
	return s.usage.Totals(), nil
}

func (s *Store) loadUsage() error {
	return s.db.View(func(tx *bolt.Tx) error {
		records, err := loadNodeUsage(tx)
		if err != nil {
			return err
		}
		for _, record := range records {
			counter := s.usage.Counter(record.NodeID, record.SubscriptionURL)
			value := counter.current()
			value.upload.Store(record.UploadBytes)
			value.download.Store(record.DownloadBytes)
			counter.flushed = record.TrafficUsage
		}
		return nil
	})
}

type usageWrite struct {
	counter *UsageCounter
	record  NodeUsageRecord
}

// pendingUsage must be called while Store.writeMu is held. Absolute snapshots
// make concurrent increments and failed database writes safe to retry.
func (s *Store) pendingUsage() []usageWrite {
	var writes []usageWrite
	s.usage.counters.Range(func(_, value any) bool {
		counter := value.(*UsageCounter)
		record := counter.record()
		if record.TrafficUsage != counter.flushed {
			writes = append(writes, usageWrite{counter: counter, record: record})
		}
		return true
	})
	return writes
}

func loadNodeUsage(tx *bolt.Tx) ([]NodeUsageRecord, error) {
	var records []NodeUsageRecord
	bucket := tx.Bucket(bucketNodeUsage)
	if bucket == nil {
		return records, nil
	}
	err := bucket.ForEach(func(key, value []byte) error {
		var record NodeUsageRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("解析节点用量记录失败: %w", err)
		}
		if record.NodeID == "" || record.UploadBytes < 0 || record.DownloadBytes < 0 {
			return fmt.Errorf("节点用量记录无效")
		}
		record.TotalBytes = record.UploadBytes + record.DownloadBytes
		records = append(records, record)
		return nil
	})
	return records, err
}

func importNodeUsage(tx *bolt.Tx, records []NodeUsageRecord) error {
	if len(records) == 0 {
		return nil
	}
	bucket, err := tx.CreateBucketIfNotExists(bucketNodeUsage)
	if err != nil {
		return err
	}
	for _, incoming := range records {
		if incoming.NodeID == "" {
			continue
		}
		incoming.SubscriptionURL = strings.TrimSpace(incoming.SubscriptionURL)
		current := NodeUsageRecord{NodeID: incoming.NodeID, SubscriptionURL: incoming.SubscriptionURL}
		if value := bucket.Get([]byte(incoming.key())); value != nil {
			if err := json.Unmarshal(value, &current); err != nil {
				return fmt.Errorf("解析节点用量记录失败: %w", err)
			}
		}
		// Restoring the same or an older backup must neither double-count usage
		// nor reduce totals already recorded on this machine.
		current.UploadBytes = max(current.UploadBytes, incoming.UploadBytes)
		current.DownloadBytes = max(current.DownloadBytes, incoming.DownloadBytes)
		current.TotalBytes = current.UploadBytes + current.DownloadBytes
		data, err := json.Marshal(current)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(incoming.key()), data); err != nil {
			return err
		}
	}
	return nil
}

// LoadTrafficUsageSnapshot reads only cumulative usage from a stopped project.
// Missing databases and databases created before usage tracking return zero.
func LoadTrafficUsageSnapshot(configPath string) (UsageTotals, error) {
	var totals UsageTotals
	path := filepath.Join(filepath.Dir(configPath), FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return totals, nil
		}
		return totals, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return totals, fmt.Errorf("打开节点用量历史失败: %w", err)
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		records, err := loadNodeUsage(tx)
		if err != nil {
			return err
		}
		for _, record := range records {
			totals.add(record)
		}
		return nil
	})
	return totals, err
}
