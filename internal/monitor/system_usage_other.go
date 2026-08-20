//go:build !windows && !linux

package monitor

import "errors"

func newCPUPercentReader() cpuPercentReader { return nil }

func readSystemUsageCounters() (idle, total, memoryUsed, memoryTotal uint64, err error) {
	return 0, 0, 0, 0, errors.New("当前平台不支持读取系统资源使用率")
}
