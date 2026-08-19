//go:build linux

package monitor

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func readSystemUsageCounters() (idle, total, memoryUsed, memoryTotal uint64, err error) {
	stat, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	scanner := bufio.NewScanner(stat)
	if !scanner.Scan() {
		_ = stat.Close()
		return 0, 0, 0, 0, fmt.Errorf("/proc/stat 中缺少 CPU 数据")
	}
	fields := strings.Fields(scanner.Text())
	_ = stat.Close()
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, 0, 0, fmt.Errorf("/proc/stat CPU 数据格式无效")
	}
	values := make([]uint64, 0, len(fields)-1)
	for index, field := range fields[1:] {
		value, parseErr := strconv.ParseUint(field, 10, 64)
		if parseErr != nil {
			return 0, 0, 0, 0, parseErr
		}
		values = append(values, value)
		// guest and guest_nice are already included in user and nice.
		if index < 8 {
			total += value
		}
	}
	idle = values[3]
	if len(values) > 4 {
		idle += values[4]
	}

	memory, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer memory.Close()
	var available, free, buffers, cached uint64
	scanner = bufio.NewScanner(memory)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 2 {
			continue
		}
		value, parseErr := strconv.ParseUint(parts[1], 10, 64)
		if parseErr != nil {
			continue
		}
		switch strings.TrimSuffix(parts[0], ":") {
		case "MemTotal":
			memoryTotal = value * 1024
		case "MemAvailable":
			available = value * 1024
		case "MemFree":
			free = value * 1024
		case "Buffers":
			buffers = value * 1024
		case "Cached":
			cached = value * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, 0, 0, err
	}
	if available == 0 {
		available = free + buffers + cached
	}
	if available < memoryTotal {
		memoryUsed = memoryTotal - available
	}
	return idle, total, memoryUsed, memoryTotal, nil
}
