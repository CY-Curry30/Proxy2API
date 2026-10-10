package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultProxyTestTarget is the endpoint used by the proxy connectivity check
// when the system settings do not name one. It is a 204 endpoint, so a
// successful test proves the tunnel works end to end without depending on the
// content of any real website.
const DefaultProxyTestTarget = "https://www.google.com/generate_204"

// SystemProxyConfig is the process-wide upstream proxy owned by the workspace
// (projects.yaml). A subscription refresh uses it only after the direct attempt
// and the project's own proxy entry have both failed — for example when the
// workspace has no project runtime to lend its proxy, or when that proxy has no
// usable node yet.
type SystemProxyConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	URL     string `yaml:"url,omitempty" json:"url,omitempty"`
	// TestTarget is the destination the connectivity check dials through the
	// proxy. Empty means the built-in default.
	TestTarget string `yaml:"test_target,omitempty" json:"test_target,omitempty"`
}

// EffectiveURL returns the configured proxy URL, or an empty string when the
// proxy is disabled or not configured.
func (p SystemProxyConfig) EffectiveURL() string {
	if !p.Enabled {
		return ""
	}
	return strings.TrimSpace(p.URL)
}

// EffectiveTestTarget returns the connectivity-check destination, falling back
// to the built-in default.
func (p SystemProxyConfig) EffectiveTestTarget() string {
	if target := strings.TrimSpace(p.TestTarget); target != "" {
		return target
	}
	return DefaultProxyTestTarget
}

// ValidateProxyURL accepts an http/https/socks5 proxy address that carries an
// explicit host and port. Credentials are allowed inline (user:pass@host:port).
func ValidateProxyURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return errors.New("代理地址不能为空")
	}
	if !strings.Contains(trimmed, "://") {
		return errors.New("代理地址缺少协议前缀（如 http:// 或 socks5://）")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("代理地址无法解析: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "socks5", "socks5h":
	default:
		return fmt.Errorf("不支持的代理协议 %q（仅支持 http / https / socks5）", parsed.Scheme)
	}
	if parsed.Hostname() == "" {
		return errors.New("代理地址缺少主机名")
	}
	if parsed.Port() == "" {
		return errors.New("代理地址缺少端口")
	}
	return nil
}

// NewProxyHTTPClient returns an HTTP client that routes every request through
// proxyURL. Callers should validate the address with ValidateProxyURL first;
// this only rejects values that cannot be parsed into a proxy endpoint.
func NewProxyHTTPClient(proxyURL string, timeout time.Duration) (*http.Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(proxyURL))
	if err != nil {
		return nil, fmt.Errorf("解析代理地址失败: %w", err)
	}
	if parsed.Host == "" {
		return nil, errors.New("代理地址缺少主机名")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialTimeout := minDuration(timeout, 10*time.Second)
	transport := &http.Transport{
		Proxy: http.ProxyURL(parsed),
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: minDuration(timeout, 15*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

// ProxyTestResult reports the outcome of a proxy connectivity check.
type ProxyTestResult struct {
	OK         bool   `json:"ok"`
	Proxy      string `json:"proxy"`
	Target     string `json:"target"`
	StatusCode int    `json:"status_code"`
	LatencyMS  int64  `json:"latency_ms"`
	Error      string `json:"error,omitempty"`
}

// TestProxyConnectivity dials target through proxyURL and reports whether the
// proxy is usable. A 2xx/3xx response proves the tunnel works; a 4xx/5xx (for
// example 407 from a proxy that rejects the credentials) is reported as a
// failure together with the observed status code.
func TestProxyConnectivity(ctx context.Context, proxyURL, target string, timeout time.Duration) ProxyTestResult {
	result := ProxyTestResult{Proxy: strings.TrimSpace(proxyURL)}
	if err := ValidateProxyURL(proxyURL); err != nil {
		result.Error = err.Error()
		return result
	}
	target = strings.TrimSpace(target)
	if target == "" {
		target = DefaultProxyTestTarget
	}
	result.Target = target
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	client, err := NewProxyHTTPClient(proxyURL, timeout)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer client.CloseIdleConnections()

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target, nil)
	if err != nil {
		result.Error = fmt.Sprintf("创建测试请求失败: %v", err)
		return result
	}
	ApplySubscriptionRequestHeaders(req)

	start := time.Now()
	resp, err := client.Do(req)
	result.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))

	result.StatusCode = resp.StatusCode
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		result.OK = true
		return result
	}
	result.Error = fmt.Sprintf("代理返回状态码 %d", resp.StatusCode)
	return result
}
