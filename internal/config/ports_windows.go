//go:build windows
// +build windows

package config

import (
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

// GetSystemPortsInUse returns ports reported by the service host's netstat.
func GetSystemPortsInUse() ([]SystemPortInfo, error) {
	cmd := exec.Command("netstat", "-ano")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("执行 netstat 失败: %w", err)
	}

	return parseNetstatOutput(string(output))
}

// parseNetstatOutput parses the output of netstat -ano on Windows. TCP and UDP
// entries are both included; a port is reported once even when it has multiple
// local addresses or connections.
func parseNetstatOutput(output string) ([]SystemPortInfo, error) {
	var ports []SystemPortInfo
	seen := make(map[uint16]bool)
	processNames := make(map[int]string)

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 || (fields[0] != "TCP" && fields[0] != "UDP") {
			continue
		}
		port, ok := parseNetstatEndpointPort(fields[1])
		if !ok || seen[port] {
			continue
		}
		pidIndex := len(fields) - 1
		pid, err := strconv.Atoi(fields[pidIndex])
		if err != nil {
			continue
		}
		state := fields[0]
		if fields[0] == "TCP" && len(fields) >= 4 {
			state = fields[3]
		}
		program, known := processNames[pid]
		if !known {
			program = getProcessName(pid)
			processNames[pid] = program
		}
		seen[port] = true
		ports = append(ports, SystemPortInfo{
			Port:    port,
			PID:     pid,
			Program: program,
			State:   state,
		})
	}

	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })
	return ports, nil
}

func parseNetstatEndpointPort(endpoint string) (uint16, bool) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || endpoint == "*:*" {
		return 0, false
	}
	colon := strings.LastIndex(endpoint, ":")
	if colon < 0 || colon == len(endpoint)-1 {
		return 0, false
	}
	port, err := strconv.ParseUint(endpoint[colon+1:], 10, 16)
	return uint16(port), err == nil && port > 0
}

// getProcessName tries to get the process name for a given PID
func getProcessName(pid int) string {
	cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Sprintf("PID:%d", pid)
	}

	// Parse CSV output: "program.exe","12345",...
	line := strings.TrimSpace(string(output))
	if line == "" {
		return fmt.Sprintf("PID:%d", pid)
	}

	// Extract first field (program name) from CSV
	parts := strings.Split(line, ",")
	if len(parts) > 0 {
		program := strings.Trim(parts[0], "\"")
		if program != "" {
			return program
		}
	}

	return fmt.Sprintf("PID:%d", pid)
}
