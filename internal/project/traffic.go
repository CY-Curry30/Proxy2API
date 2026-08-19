package project

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"Proxy2API/internal/state"
)

const trafficCollectorRetryInterval = 2 * time.Second

type trafficSample struct {
	UploadRate   int64 `json:"up"`
	DownloadRate int64 `json:"down"`
}

type projectTrafficMonth struct {
	id    string
	name  string
	month state.TrafficMonth
}

func (r *Runtime) LoadTrafficMonth(month string) (state.TrafficMonth, error) {
	r.mu.RLock()
	store := r.stateStore
	configPath := r.configPath
	r.mu.RUnlock()
	if store != nil {
		return store.LoadTrafficMonth(month)
	}
	return state.LoadTrafficMonthSnapshot(configPath, month)
}

func (r *Registry) LoadTrafficMonth(month string) (state.TrafficMonth, error) {
	if _, err := time.Parse("2006-01", month); err != nil {
		return state.TrafficMonth{}, fmt.Errorf("月份必须使用 YYYY-MM 格式")
	}
	type projectTrafficSource struct {
		id      string
		name    string
		runtime *Runtime
	}
	r.mu.RLock()
	projects := make([]projectTrafficSource, 0, len(r.runtimes))
	for id, runtime := range r.runtimes {
		name := r.workspace.Projects[id].Name
		if name == "" {
			name = id
		}
		projects = append(projects, projectTrafficSource{id: id, name: name, runtime: runtime})
	}
	r.mu.RUnlock()
	sort.Slice(projects, func(i, j int) bool { return projects[i].id < projects[j].id })

	loaded := make([]projectTrafficMonth, 0, len(projects))
	for _, project := range projects {
		projectMonth, err := project.runtime.LoadTrafficMonth(month)
		if err != nil {
			return state.TrafficMonth{}, fmt.Errorf("读取项目 %q 的流量历史失败: %w", project.id, err)
		}
		loaded = append(loaded, projectTrafficMonth{id: project.id, name: project.name, month: projectMonth})
	}
	return aggregateProjectTrafficMonths(month, loaded), nil
}

func aggregateProjectTrafficMonths(month string, projects []projectTrafficMonth) state.TrafficMonth {
	days := make(map[string]state.TrafficDay)
	result := state.TrafficMonth{Month: month, Days: []state.TrafficDay{}, Projects: []state.ProjectTrafficMonth{}}
	for _, project := range projects {
		result.Projects = append(result.Projects, state.ProjectTrafficMonth{
			ProjectID:     project.id,
			ProjectName:   project.name,
			Days:          append([]state.TrafficDay(nil), project.month.Days...),
			UploadBytes:   project.month.UploadBytes,
			DownloadBytes: project.month.DownloadBytes,
			TotalBytes:    project.month.TotalBytes,
		})
		for _, projectDay := range project.month.Days {
			day := days[projectDay.Date]
			day.Date = projectDay.Date
			day.UploadBytes += projectDay.UploadBytes
			day.DownloadBytes += projectDay.DownloadBytes
			day.TotalBytes = day.UploadBytes + day.DownloadBytes
			if projectDay.UpdatedAt.After(day.UpdatedAt) {
				day.UpdatedAt = projectDay.UpdatedAt
			}
			days[day.Date] = day
		}
	}
	for _, day := range days {
		result.UploadBytes += day.UploadBytes
		result.DownloadBytes += day.DownloadBytes
		result.Days = append(result.Days, day)
	}
	result.TotalBytes = result.UploadBytes + result.DownloadBytes
	sort.Slice(result.Days, func(i, j int) bool { return result.Days[i].Date < result.Days[j].Date })
	return result
}

func collectTraffic(ctx context.Context, trafficAPI string, store *state.Store) {
	for {
		if err := collectTrafficStream(ctx, trafficAPI, store); err == nil || ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(trafficCollectorRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func collectTrafficStream(ctx context.Context, trafficAPI string, store *state.Store) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, trafficAPI, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("流量接口返回 HTTP %d", response.StatusCode)
	}

	decoder := json.NewDecoder(response.Body)
	var previous time.Time
	for {
		var sample trafficSample
		if err := decoder.Decode(&sample); err != nil {
			return err
		}
		now := time.Now()
		if !previous.IsZero() {
			elapsed := now.Sub(previous).Seconds()
			if elapsed > 0 && elapsed < 10 {
				store.AddTraffic(now, trafficBytesForInterval(sample.UploadRate, elapsed), trafficBytesForInterval(sample.DownloadRate, elapsed))
			}
		}
		previous = now
	}
}

func trafficBytesForInterval(rate int64, seconds float64) int64 {
	if rate <= 0 || seconds <= 0 {
		return 0
	}
	value := float64(rate) * seconds
	if value >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(math.Round(value))
}
