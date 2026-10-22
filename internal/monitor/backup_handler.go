package monitor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxBackupBodyBytes caps an uploaded backup document. Node states and daily
// traffic dominate the size, so the limit is generous but still bounded.
const maxBackupBodyBytes = 128 << 20

// handleBackup serves the portable export document.
//
//	GET /api/backup?project=<id>   one project (defaults to the default project)
//	GET /api/backup?scope=all      workspace settings + shared catalog + all projects
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "项目管理未启用"})
		return
	}
	query := r.URL.Query()
	all := false
	switch strings.ToLower(strings.TrimSpace(query.Get("scope"))) {
	case "all", "global", "workspace":
		all = true
	}
	if value, err := strconv.ParseBool(strings.TrimSpace(query.Get("all"))); err == nil && value {
		all = true
	}
	projectID := strings.TrimSpace(query.Get("project"))

	bundle, err := s.projects.ExportBackup(r.Context(), projectID, all)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	name := "workspace"
	if !all && len(bundle.Projects) == 1 {
		name = bundle.Projects[0].ID
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q",
		backupFilename(bundle.Scope, name, bundle.ExportedAt)))
	writeJSON(w, bundle)
}

// handleBackupImport restores an exported document.
//
//	POST /api/backup/import?mode=overwrite|skip
//	body: {"mode":"overwrite","bundle":{...}}  or a bare bundle document
func (s *Server) handleBackupImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.projects == nil {
		w.WriteHeader(http.StatusNotImplemented)
		writeJSON(w, map[string]any{"error": "项目管理未启用"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBackupBodyBytes+1))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "读取上传文件失败"})
		return
	}
	if len(body) > maxBackupBodyBytes {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		writeJSON(w, map[string]any{"error": "备份文件过大"})
		return
	}
	request, err := decodeBackupImportRequest(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	mode := strings.TrimSpace(request.Mode)
	if override := strings.TrimSpace(r.URL.Query().Get("mode")); override != "" {
		mode = override
	}
	result, err := s.projects.ImportBackup(r.Context(), request.Bundle, mode)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": err.Error(), "result": result})
		return
	}
	writeJSON(w, result)
}

// decodeBackupImportRequest accepts both the wrapped request the WebUI sends
// and a bare exported document, so a backup can be restored with curl.
func decodeBackupImportRequest(body []byte) (BackupImportRequest, error) {
	var request BackupImportRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return request, fmt.Errorf("备份文件不是合法的 JSON: %w", err)
	}
	if request.Bundle.Format != "" || request.Bundle.Projects != nil || request.Bundle.Shared != nil || request.Bundle.Workspace != nil {
		return request, nil
	}
	var bundle BackupBundle
	if err := json.Unmarshal(body, &bundle); err != nil {
		return request, fmt.Errorf("备份文件不是合法的 JSON: %w", err)
	}
	if bundle.Format == "" && bundle.Projects == nil && bundle.Shared == nil && bundle.Workspace == nil {
		return request, fmt.Errorf("备份文件缺少 format/projects 字段，可能不是 Proxy2API 导出的数据")
	}
	request.Bundle = bundle
	return request, nil
}

// backupFilename keeps the exported file name stable for callers that build it
// themselves (the WebUI names the download after the response header).
func backupFilename(scope, projectID string, at time.Time) string {
	name := projectID
	if scope == BackupScopeWorkspace || name == "" {
		name = "workspace"
	}
	return fmt.Sprintf("proxy2api-backup-%s-%s.json", name, at.Local().Format("20060102-150405"))
}
