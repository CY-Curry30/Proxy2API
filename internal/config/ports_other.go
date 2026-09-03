//go:build !windows && !linux && !darwin

package config

import "fmt"

// GetSystemPortsInUse is unavailable on platforms without a supported port
// listing command. The management API still returns a structured empty result.
func GetSystemPortsInUse() ([]SystemPortInfo, error) {
	return nil, fmt.Errorf("当前平台不支持系统端口扫描")
}
