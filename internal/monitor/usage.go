package monitor

import (
	"net/http"

	"Proxy2API/internal/state"
)

func (s *Server) trafficUsage() (state.UsageTotals, error) {
	if s.trafficHistory != nil {
		return s.trafficHistory.LoadTrafficUsage()
	}
	if s.mgr != nil {
		return s.mgr.TrafficUsage(), nil
	}
	return state.UsageTotals{}, nil
}

func (s *Server) handleClearTrafficHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "项目管理未启用"})
		return
	}
	if s.enqueueAsyncMutation(w, r, TaskSpec{
		Kind:      "system.traffic.clear",
		Scope:     "workspace",
		Exclusive: true,
		Message:   "正在清空所有项目的流量历史",
	}, (*Server).handleClearTrafficHistory) {
		return
	}
	if err := s.projects.ClearTrafficHistory(r.Context()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"message": "所有项目的流量历史已清空，后续流量将重新累计"})
}
