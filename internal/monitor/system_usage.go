package monitor

import (
	"math"
	"sync"
)

type systemUsage struct {
	CPUPercent       float64 `json:"cpu_percent"`
	MemoryPercent    float64 `json:"memory_percent"`
	MemoryUsedBytes  uint64  `json:"memory_used_bytes"`
	MemoryTotalBytes uint64  `json:"memory_total_bytes"`
}

type systemUsageSampler struct {
	mu            sync.Mutex
	initialized   bool
	previousIdle  uint64
	previousTotal uint64
}

func (s *systemUsageSampler) sample() (systemUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idle, total, memoryUsed, memoryTotal, err := readSystemUsageCounters()
	if err != nil {
		return systemUsage{}, err
	}

	cpuPercent := calculateCPUPercent(s.previousIdle, s.previousTotal, idle, total, s.initialized)
	s.previousIdle = idle
	s.previousTotal = total
	s.initialized = true

	return systemUsage{
		CPUPercent:       cpuPercent,
		MemoryPercent:    calculateMemoryPercent(memoryUsed, memoryTotal),
		MemoryUsedBytes:  memoryUsed,
		MemoryTotalBytes: memoryTotal,
	}, nil
}

func calculateCPUPercent(previousIdle, previousTotal, idle, total uint64, initialized bool) float64 {
	if !initialized || total <= previousTotal || idle < previousIdle {
		return 0
	}
	totalDelta := total - previousTotal
	idleDelta := idle - previousIdle
	percent := (1 - float64(idleDelta)/float64(totalDelta)) * 100
	return math.Max(0, math.Min(100, percent))
}

func calculateMemoryPercent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	percent := float64(used) / float64(total) * 100
	return math.Max(0, math.Min(100, percent))
}
