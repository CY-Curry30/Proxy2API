//go:build linux || darwin
// +build linux darwin

package config

import (
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// GetSystemPortsInUse returns all TCP ports currently in use on Linux/macOS using ss or netstat.
func GetSystemPortsInUse() ([]SystemPortInfo, error) {
	// Try ss first (modern Linux)
	cmd := exec.Command("ss", "-lntp")
	output, err := cmd.Output()
	if err == nil {
		return parseSSOutput(string(output))
	}

	// Fallback to netstat (older Linux/macOS)
	cmd = exec.Command("netstat", "-lntp")
	output, err = cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("执行 ss/netstat 失败: %w", err)
	}

	return parseNetstatLinuxOutput(string(output))
}

// parseSSOutput parses the output of ss -lntp. Process metadata is optional:
// ss omits it when the caller lacks permission, but the listening port is still
// useful and must remain visible.
func parseSSOutput(output string) ([]SystemPortInfo, error) {
	var ports []SystemPortInfo
	seen := make(map[uint16]bool)
	processRE := regexp.MustCompile(`users:\(\("([^"]+)",pid=(\d+)`)

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 || !strings.EqualFold(fields[0], "LISTEN") {
			continue
		}
		port, ok := parseUnixEndpointPort(fields[3])
		if !ok || seen[port] {
			continue
		}
		pid := 0
		program := ""
		if matches := processRE.FindStringSubmatch(line); len(matches) == 3 {
			pid, _ = strconv.Atoi(matches[2])
			program = matches[1]
		}
		seen[port] = true
		ports = append(ports, SystemPortInfo{Port: port, PID: pid, Program: program, State: "LISTEN"})
	}

	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })
	return ports, nil
}

// parseNetstatLinuxOutput parses the output of netstat -lntp
func parseNetstatLinuxOutput(output string) ([]SystemPortInfo, error) {
	var ports []SystemPortInfo
	seen := make(map[uint16]bool)
	processRE := regexp.MustCompile(`^(\d+)/(\S+)$`)

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 6 || !strings.HasPrefix(strings.ToLower(fields[0]), "tcp") || !strings.EqualFold(fields[5], "LISTEN") {
			continue
		}
		port, ok := parseUnixEndpointPort(fields[3])
		if !ok || seen[port] {
			continue
		}
		pid := 0
		program := ""
		if len(fields) > 6 {
			if matches := processRE.FindStringSubmatch(fields[6]); len(matches) == 3 {
				pid, _ = strconv.Atoi(matches[1])
				program = matches[2]
			}
		}
		seen[port] = true
		ports = append(ports, SystemPortInfo{Port: port, PID: pid, Program: program, State: "LISTEN"})
	}

	sort.Slice(ports, func(i, j int) bool { return ports[i].Port < ports[j].Port })
	return ports, nil
}

func parseUnixEndpointPort(endpoint string) (uint16, bool) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || strings.HasSuffix(endpoint, ":*") {
		return 0, false
	}
	colon := strings.LastIndex(endpoint, ":")
	if colon < 0 || colon == len(endpoint)-1 {
		return 0, false
	}
	port, err := strconv.ParseUint(endpoint[colon+1:], 10, 16)
	return uint16(port), err == nil && port > 0
}
