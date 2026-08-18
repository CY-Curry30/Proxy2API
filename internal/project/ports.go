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

type portRecommendations struct {
	ListenerPort  uint16
	MultiPortBase uint16
	StickyPort    uint16
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
		return fmt.Errorf("项目 %q 没有配置", projectID)
	}
	desired, err := declaredPorts(cfg)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for port, purpose := range desired {
		if owner, exists := r.owners[port]; exists && owner.Project != projectID {
			return fmt.Errorf("端口 %d（%s）与项目 %q（%s）冲突", port, purpose, owner.Project, owner.Purpose)
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
			return fmt.Errorf("同一项目中的 %s 和 %s 同时使用了端口 %d", previous, purpose, port)
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
		basePortAssigned := false
		for _, node := range cfg.Nodes {
			if err := add(node.Port, "node "+node.Name); err != nil {
				return nil, err
			}
			if node.Port != 0 && node.Port == cfg.MultiPort.BasePort {
				basePortAssigned = true
			}
		}
		// A new project may not have nodes yet, but its first node will start at
		// BasePort. Reserve that future listener now so another project cannot be
		// created with the same starting port. Once a node owns BasePort, its
		// listener already represents the reservation.
		if !basePortAssigned {
			if err := add(cfg.MultiPort.BasePort, "multi-port base"); err != nil {
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
		return errors.New("不能占用端口 0")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, exists := r.owners[port]; exists && owner.Project != projectID {
		return fmt.Errorf("端口 %d（%s）与项目 %q（%s）冲突", port, purpose, owner.Project, owner.Purpose)
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
	port := r.nextAvailableLocked(start, nil)
	if port == 0 {
		return 0, fmt.Errorf("从 %d 开始没有可用端口", start)
	}
	return port, nil
}

// CreationHints returns one consistent ownership snapshot and three distinct
// ports that are currently available for a new project.
func (r *PortRegistry) CreationHints() (map[uint16]portOwner, portRecommendations) {
	r.mu.Lock()
	defer r.mu.Unlock()

	owners := make(map[uint16]portOwner, len(r.owners))
	for port, owner := range r.owners {
		if owner.Project != sharedCatalogID {
			owners[port] = owner
		}
	}

	selected := make(map[uint16]struct{}, 3)
	listenerPort := r.nextAvailableLocked(2323, selected)
	if listenerPort != 0 {
		selected[listenerPort] = struct{}{}
	}
	multiPortBase := r.nextAvailableLocked(24000, selected)
	if multiPortBase != 0 {
		selected[multiPortBase] = struct{}{}
	}
	stickyStart := uint16(2324)
	if listenerPort > 0 && listenerPort < 65535 {
		stickyStart = listenerPort + 1
	}
	stickyPort := r.nextAvailableLocked(stickyStart, selected)

	return owners, portRecommendations{
		ListenerPort:  listenerPort,
		MultiPortBase: multiPortBase,
		StickyPort:    stickyPort,
	}
}

func (r *PortRegistry) nextAvailableLocked(start uint16, excluded map[uint16]struct{}) uint16 {
	if start == 0 {
		start = 1
	}
	for candidate := uint32(start); candidate <= 65535; candidate++ {
		port := uint16(candidate)
		if _, reserved := r.owners[port]; reserved {
			continue
		}
		if _, reserved := excluded[port]; reserved {
			continue
		}
		if config.IsPortAvailable("0.0.0.0", port) {
			return port
		}
	}
	return 0
}
