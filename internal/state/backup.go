package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	bolt "github.com/sagernet/bbolt"
)

// BackupSnapshot is every persisted record a backup export carries for one
// project (or for the shared catalog): node states, daily traffic totals and
// the subscription usage snapshot.
type BackupSnapshot struct {
	Nodes        []NodeRecord       `json:"nodes,omitempty"`
	Traffic      []TrafficDay       `json:"traffic,omitempty"`
	Subscription *SubscriptionState `json:"subscription,omitempty"`
}

// Empty reports whether the snapshot carries nothing worth importing.
func (s BackupSnapshot) Empty() bool {
	return len(s.Nodes) == 0 && len(s.Traffic) == 0 && s.Subscription == nil
}

// ExportSnapshot reads every record from a live store, including the writes
// still waiting in the flush buffer. It is the only correct way to snapshot a
// running project: the database file is locked by the open store.
func (s *Store) ExportSnapshot() (BackupSnapshot, error) {
	var snapshot BackupSnapshot
	if s == nil {
		return snapshot, nil
	}
	nodes, err := s.LoadNodes()
	if err != nil {
		return snapshot, err
	}
	for _, record := range nodes {
		snapshot.Nodes = append(snapshot.Nodes, record)
	}
	sortNodeRecords(snapshot.Nodes)

	traffic, err := s.loadTrafficDays()
	if err != nil {
		return snapshot, err
	}
	snapshot.Traffic = traffic

	subscriptions, found, err := s.LoadSubscriptionState()
	if err != nil {
		return snapshot, err
	}
	if found {
		snapshot.Subscription = &subscriptions
	}
	return snapshot, nil
}

func (s *Store) loadTrafficDays() ([]TrafficDay, error) {
	if s == nil {
		return nil, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	days := make(map[string]TrafficDay)
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketTraffic)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			var day TrafficDay
			if err := json.Unmarshal(value, &day); err != nil {
				return fmt.Errorf("解析流量记录 %q 失败: %w", key, err)
			}
			if day.Date == "" {
				day.Date = string(key)
			}
			day.TotalBytes = day.UploadBytes + day.DownloadBytes
			days[day.Date] = day
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	for date, pending := range s.pendingTraffic {
		current := days[date]
		current.Date = date
		current.UploadBytes += pending.UploadBytes
		current.DownloadBytes += pending.DownloadBytes
		current.TotalBytes = current.UploadBytes + current.DownloadBytes
		if pending.UpdatedAt.After(current.UpdatedAt) {
			current.UpdatedAt = pending.UpdatedAt
		}
		days[date] = current
	}
	s.mu.Unlock()
	return sortedTrafficDays(days), nil
}

// LoadBackupSnapshot reads the state database that lives next to configPath.
// A missing database means the project never ran, which is not an error.
func LoadBackupSnapshot(configPath string) (BackupSnapshot, error) {
	var snapshot BackupSnapshot
	if configPath == "" {
		return snapshot, nil
	}
	path := filepath.Join(filepath.Dir(configPath), FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return snapshot, nil
		}
		return snapshot, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return snapshot, fmt.Errorf("打开状态数据库失败: %w", err)
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket(bucketNodes); bucket != nil {
			if err := bucket.ForEach(func(key, value []byte) error {
				var record NodeRecord
				if err := json.Unmarshal(value, &record); err != nil {
					return fmt.Errorf("解析节点状态 %q 失败: %w", key, err)
				}
				snapshot.Nodes = append(snapshot.Nodes, record)
				return nil
			}); err != nil {
				return err
			}
		}
		if bucket := tx.Bucket(bucketTraffic); bucket != nil {
			days := make(map[string]TrafficDay)
			if err := bucket.ForEach(func(key, value []byte) error {
				var day TrafficDay
				if err := json.Unmarshal(value, &day); err != nil {
					return fmt.Errorf("解析流量记录 %q 失败: %w", key, err)
				}
				if day.Date == "" {
					day.Date = string(key)
				}
				day.TotalBytes = day.UploadBytes + day.DownloadBytes
				days[day.Date] = day
				return nil
			}); err != nil {
				return err
			}
			snapshot.Traffic = sortedTrafficDays(days)
		}
		if bucket := tx.Bucket(bucketSubscriptions); bucket != nil {
			if value := bucket.Get(keyGlobal); value != nil {
				var subscriptions SubscriptionState
				if err := json.Unmarshal(value, &subscriptions); err != nil {
					return fmt.Errorf("解析订阅状态失败: %w", err)
				}
				snapshot.Subscription = &subscriptions
			}
		}
		return nil
	})
	if err != nil {
		return BackupSnapshot{}, err
	}
	sortNodeRecords(snapshot.Nodes)
	return snapshot, nil
}

// ImportBackup merges a snapshot into the state database next to configPath,
// creating the database when the project has never run. The caller must make
// sure nothing else holds the database open (stop the project first).
//
// Merge rules are deliberately non-destructive: traffic days and subscription
// usage keep the larger value, so restoring an older backup never shrinks a
// counter that has already moved on.
func ImportBackup(configPath string, snapshot BackupSnapshot) error {
	if configPath == "" || snapshot.Empty() {
		return nil
	}
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("创建状态目录失败: %w", err)
	}
	path := filepath.Join(dir, FileName)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("打开状态数据库失败（项目可能仍在运行）: %w", err)
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if meta.Get(keyVersion) == nil {
			version, err := json.Marshal(currentVersion)
			if err != nil {
				return err
			}
			if err := meta.Put(keyVersion, version); err != nil {
				return err
			}
		}
		nodeBucket, err := tx.CreateBucketIfNotExists(bucketNodes)
		if err != nil {
			return err
		}
		for _, record := range snapshot.Nodes {
			if record.ID == "" {
				continue
			}
			record.UpdatedAt = time.Now().UTC()
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := nodeBucket.Put([]byte(record.ID), data); err != nil {
				return err
			}
		}
		trafficBucket, err := tx.CreateBucketIfNotExists(bucketTraffic)
		if err != nil {
			return err
		}
		for _, day := range snapshot.Traffic {
			if day.Date == "" {
				continue
			}
			current := TrafficDay{Date: day.Date}
			if existing := trafficBucket.Get([]byte(day.Date)); existing != nil {
				if err := json.Unmarshal(existing, &current); err != nil {
					return fmt.Errorf("解析流量记录 %q 失败: %w", day.Date, err)
				}
			}
			if day.UploadBytes > current.UploadBytes {
				current.UploadBytes = day.UploadBytes
			}
			if day.DownloadBytes > current.DownloadBytes {
				current.DownloadBytes = day.DownloadBytes
			}
			current.TotalBytes = current.UploadBytes + current.DownloadBytes
			if day.UpdatedAt.After(current.UpdatedAt) {
				current.UpdatedAt = day.UpdatedAt
			}
			data, err := json.Marshal(current)
			if err != nil {
				return err
			}
			if err := trafficBucket.Put([]byte(day.Date), data); err != nil {
				return err
			}
		}
		if snapshot.Subscription == nil {
			return nil
		}
		subBucket, err := tx.CreateBucketIfNotExists(bucketSubscriptions)
		if err != nil {
			return err
		}
		merged := *snapshot.Subscription
		if existing := subBucket.Get(keyGlobal); existing != nil {
			var current SubscriptionState
			if err := json.Unmarshal(existing, &current); err == nil {
				merged = mergeSubscriptionStates(current, *snapshot.Subscription)
			}
		}
		merged.UpdatedAt = time.Now().UTC()
		data, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		return subBucket.Put(keyGlobal, data)
	})
}

// mergeSubscriptionStates keeps the locally observed subscription state and
// folds in the usage figures carried by the backup, so a restore cannot erase a
// newer refresh result.
func mergeSubscriptionStates(current, incoming SubscriptionState) SubscriptionState {
	merged := current
	index := make(map[string]int, len(merged.Items))
	for i, item := range merged.Items {
		index[item.URL] = i
	}
	for _, item := range incoming.Items {
		if item.URL == "" {
			continue
		}
		position, ok := index[item.URL]
		if !ok {
			merged.Items = append(merged.Items, item)
			index[item.URL] = len(merged.Items) - 1
			continue
		}
		existing := merged.Items[position]
		if item.UpdatedAt.After(existing.UpdatedAt) {
			if item.UsedBytes < existing.UsedBytes {
				item.UsedBytes = existing.UsedBytes
			}
			if item.TotalBytes < existing.TotalBytes {
				item.TotalBytes = existing.TotalBytes
			}
			if item.ExpiresAt < existing.ExpiresAt {
				item.ExpiresAt = existing.ExpiresAt
			}
			merged.Items[position] = item
			continue
		}
		if item.UsedBytes > existing.UsedBytes {
			existing.UsedBytes = item.UsedBytes
		}
		if item.TotalBytes > existing.TotalBytes {
			existing.TotalBytes = item.TotalBytes
		}
		if item.ExpiresAt > existing.ExpiresAt {
			existing.ExpiresAt = item.ExpiresAt
		}
		merged.Items[position] = existing
	}
	return merged
}

func sortNodeRecords(records []NodeRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].Name == records[j].Name {
			return records[i].ID < records[j].ID
		}
		return records[i].Name < records[j].Name
	})
}

func sortedTrafficDays(days map[string]TrafficDay) []TrafficDay {
	result := make([]TrafficDay, 0, len(days))
	for _, day := range days {
		result = append(result, day)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Date < result[j].Date })
	return result
}
