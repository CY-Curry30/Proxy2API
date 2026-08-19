//go:build windows

package monitor

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32             = windows.NewLazySystemDLL("kernel32.dll")
	getSystemTimes       = kernel32.NewProc("GetSystemTimes")
	globalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
)

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
