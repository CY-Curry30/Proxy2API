//go:build windows

package monitor

import (
	"fmt"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32             = windows.NewLazySystemDLL("kernel32.dll")
	getSystemTimes       = kernel32.NewProc("GetSystemTimes")
	globalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
	pdh                  = windows.NewLazySystemDLL("pdh.dll")
	pdhOpenQueryW        = pdh.NewProc("PdhOpenQueryW")
	pdhAddEnglishCounter = pdh.NewProc("PdhAddEnglishCounterW")
	pdhCollectQueryData  = pdh.NewProc("PdhCollectQueryData")
	pdhGetFormattedValue = pdh.NewProc("PdhGetFormattedCounterValue")
	pdhCloseQuery        = pdh.NewProc("PdhCloseQuery")
)

const (
	pdhFmtDouble     = 0x00000200
	pdhStatusValid   = 0x00000000
	pdhStatusNewData = 0x00000001
	// Task Manager on Windows 8 and newer uses Processor Utility, which
	// accounts for the current processor performance state and Turbo Boost.
	pdhCounterPathCPUUtility = `\Processor Information(_Total)\% Processor Utility`
	// Processor Utility is unavailable on some older Windows versions.
	pdhCounterPathCPUTime = `\Processor(_Total)\% Processor Time`
)

type pdhFormattedCounterValue struct {
	status uint32
	value  float64
}

type pdhCPUReader struct {
	query           uintptr
	counter         uintptr
	lastCollectedAt time.Time
}

func newCPUPercentReader() cpuPercentReader {
	reader, err := newPDHCPUReader()
	if err != nil {
		return nil
	}
	return reader
}

func newPDHCPUReader() (*pdhCPUReader, error) {
	var query uintptr
	status, _, _ := pdhOpenQueryW.Call(0, 0, uintptr(unsafe.Pointer(&query)))
	if status != 0 {
		return nil, fmt.Errorf("PdhOpenQueryW failed: 0x%x", status)
	}

	var counter uintptr
	var lastStatus uintptr
	for _, counterPath := range []string{pdhCounterPathCPUUtility, pdhCounterPathCPUTime} {
		path, err := windows.UTF16PtrFromString(counterPath)
		if err != nil {
			pdhCloseQuery.Call(query)
			return nil, err
		}
		status, _, _ = pdhAddEnglishCounter.Call(query, uintptr(unsafe.Pointer(path)), 0, uintptr(unsafe.Pointer(&counter)))
		if status == 0 {
			break
		}
		lastStatus = status
	}
	if counter == 0 {
		pdhCloseQuery.Call(query)
		return nil, fmt.Errorf("adding PDH CPU counter failed: 0x%x", lastStatus)
	}
	status, _, _ = pdhCollectQueryData.Call(query)
	if status != 0 {
		pdhCloseQuery.Call(query)
		return nil, fmt.Errorf("initial PDH CPU sample failed: 0x%x", status)
	}
	return &pdhCPUReader{query: query, counter: counter, lastCollectedAt: time.Now()}, nil
}

func (r *pdhCPUReader) read() (float64, bool, error) {
	// The dashboard samples once per second. Allow a small scheduling tolerance
	// so a tick arriving just under one second still uses Processor Utility,
	// while additional clients cannot force repeated sub-second PDH samples.
	if time.Since(r.lastCollectedAt) < 900*time.Millisecond {
		return 0, false, nil
	}
	status, _, _ := pdhCollectQueryData.Call(r.query)
	if status != 0 {
		return 0, false, fmt.Errorf("PDH CPU sample failed: 0x%x", status)
	}
	collectedAt := time.Now()
	r.lastCollectedAt = collectedAt
	var value pdhFormattedCounterValue
	status, _, _ = pdhGetFormattedValue.Call(
		r.counter,
		pdhFmtDouble,
		0,
		uintptr(unsafe.Pointer(&value)),
	)
	if status != 0 {
		return 0, false, fmt.Errorf("reading PDH CPU value failed: 0x%x", status)
	}
	if value.status != pdhStatusValid && value.status != pdhStatusNewData {
		return 0, false, fmt.Errorf("PDH CPU value is not valid: 0x%x", value.status)
	}
	if value.value < 0 {
		value.value = 0
	} else if value.value > 100 {
		value.value = 100
	}
	return value.value, true, nil
}

func (r *pdhCPUReader) close() {
	if r != nil && r.query != 0 {
		pdhCloseQuery.Call(r.query)
		r.query = 0
	}
}

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func readSystemUsageCounters() (idle, total, memoryUsed, memoryTotal uint64, err error) {
	var idleTime, kernelTime, userTime windows.Filetime
	result, _, callErr := getSystemTimes.Call(
		uintptr(unsafe.Pointer(&idleTime)),
		uintptr(unsafe.Pointer(&kernelTime)),
		uintptr(unsafe.Pointer(&userTime)),
	)
	if result == 0 {
		return 0, 0, 0, 0, fmt.Errorf("读取系统 CPU 使用率失败: %w", callErr)
	}

	memory := memoryStatusEx{Length: uint32(unsafe.Sizeof(memoryStatusEx{}))}
	result, _, callErr = globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memory)))
	if result == 0 {
		return 0, 0, 0, 0, fmt.Errorf("读取系统内存使用率失败: %w", callErr)
	}

	idle = filetimeTicks(idleTime)
	total = filetimeTicks(kernelTime) + filetimeTicks(userTime)
	memoryTotal = memory.TotalPhys
	if memory.AvailPhys < memoryTotal {
		memoryUsed = memoryTotal - memory.AvailPhys
	}
	return idle, total, memoryUsed, memoryTotal, nil
}

func filetimeTicks(value windows.Filetime) uint64 {
	return uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime)
}
