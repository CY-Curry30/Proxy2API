package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "github.com/sagernet/bbolt"
)

func (s *Store) TrafficGeneration() uint64 { return s.usage.generation.Load() }

// ClearTrafficHistory commits the deletion before resetting live counters.
// The write lock prevents an older flush from putting cleared records back;
// pending calendar samples share the same reset boundary as connection usage.
func (s *Store) ClearTrafficHistory() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Update(clearTrafficBuckets); err != nil {
		return fmt.Errorf("清空流量历史失败: %w", err)
	}
	s.pendingTraffic = make(map[string]TrafficDay)
	s.usage.generation.Add(1)
	s.usage.counters.Range(func(_, value any) bool {
		value.(*UsageCounter).flushed = TrafficUsage{}
		return true
	})
	return nil
}

// ClearTrafficHistorySnapshot clears a stopped project's database without
// starting the project or creating a database for a project that has never run.
func ClearTrafficHistorySnapshot(configPath string) error {
	if configPath == "" {
		return errors.New("清空流量历史缺少项目配置路径")
	}
	path := filepath.Join(filepath.Dir(configPath), FileName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("打开流量历史失败: %w", err)
	}
	defer db.Close()
	return db.Update(clearTrafficBuckets)
}

func clearTrafficBuckets(tx *bolt.Tx) error {
	for _, name := range [][]byte{bucketTraffic, bucketNodeUsage} {
		if err := tx.DeleteBucket(name); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return err
		}
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	return nil
}
