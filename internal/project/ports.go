package project

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"Proxy2API/internal/config"
)

type portOwner struct {
	Project string
	Purpose string
}

// PortRegistry is the process-wide authority for listener ownership. It is
// intentionally conservative and treats a TCP port as host-wide, regardless
// of bind address, so projects can never steal a listener from each other.
type PortRegistry struct {
	mu     sync.Mutex
	owners map[uint16]portOwner
}

func NewPortRegistry(managementListen string) *PortRegistry {
	r := &PortRegistry{owners: make(map[uint16]portOwner)}
	if _, rawPort, err := net.SplitHostPort(managementListen); err == nil {
		if parsed, err := strconv.ParseUint(rawPort, 10, 16); err == nil && parsed > 0 {
			r.owners[uint16(parsed)] = portOwner{Project: "__control__", Purpose: "management"}
		}
	}
	return r
}

// Reserve atomically replaces one project's declared listeners after proving
// that none conflict with another project or the management server.
func (r *PortRegistry) Reserve(projectID string, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("project %q has no config", projectID)
	}
	desired, err := declaredPorts(cfg)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for port, purpose := range desired {
		if owner, exists := r.owners[port]; exists && owner.Project != projectID {
			return fmt.Errorf("port %d (%s) conflicts with project %q (%s)", port, purpose, owner.Project, owner.Purpose)
		}
	}
	for port, owner := range r.owners {
		if owner.Project == projectID {
			delete(r.owners, port)
		}
	}
	for port, purpose := range desired {
		r.owners[port] = portOwner{Project: projectID, Purpose: purpose}
	}
	return nil
}

func declaredPorts(cfg *config.Config) (map[uint16]string, error) {
	ports := make(map[uint16]string)
	add := func(port uint16, purpose string) error {
		if port == 0 {
			return nil
		}
		if previous, exists := ports[port]; exists {
			return fmt.Errorf("port %d is used by both %s and %s in the same project", port, previous, purpose)
		}
		ports[port] = purpose
		return nil
	}
	if cfg.Mode == "pool" || cfg.Mode == "hybrid" {
		if err := add(cfg.Listener.Port, "listener"); err != nil {
			return nil, err
		}
	}
	if cfg.Sticky.Enabled {
		if err := add(cfg.Sticky.Port, "sticky"); err != nil {
			return nil, err
		}
	}
	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		for _, node := range cfg.Nodes {
			if err := add(node.Port, "node "+node.Name); err != nil {
				return nil, err
			}
		}
	}
	if err := add(cfg.ClashAPIPort, "internal traffic API"); err != nil {
		return nil, err
	}
	return ports, nil
}

func (r *PortRegistry) Claim(projectID string, port uint16, purpose string) error {
	if port == 0 {
		return errors.New("cannot claim port 0")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, exists := r.owners[port]; exists && owner.Project != projectID {
		return fmt.Errorf("port %d (%s) conflicts with project %q (%s)", port, purpose, owner.Project, owner.Purpose)
	}
	r.owners[port] = portOwner{Project: projectID, Purpose: purpose}
	return nil
}

func (r *PortRegistry) Release(projectID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for port, owner := range r.owners {
		if owner.Project == projectID {
			delete(r.owners, port)
		}
	}
}

func (r *PortRegistry) NextAvailable(start uint16) (uint16, error) {
	if start == 0 {
		start = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for candidate := uint32(start); candidate <= 65535; candidate++ {
		port := uint16(candidate)
		if _, reserved := r.owners[port]; reserved {
			continue
		}
		if config.IsPortAvailable("0.0.0.0", port) {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no available port at or above %d", start)
}
