package subscription

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"Proxy2API/internal/config"
)

// Fetch channels recorded on every subscription so the panel and the refresh
// summary can explain how the latest update was actually obtained.
const (
	FetchChannelDirect       = "direct"
	FetchChannelProjectProxy = "project_proxy"
	FetchChannelSystemProxy  = "system_proxy"
)

// FetchChannelLabel returns the user-facing name of a fetch channel.
func FetchChannelLabel(channel string) string {
	switch channel {
	case FetchChannelProjectProxy:
		return "项目代理"
	case FetchChannelSystemProxy:
		return "系统代理"
	case FetchChannelDirect:
		return "直连"
	default:
		return ""
	}
}

// ProjectProxyURL returns the proxy entry this project exposes to its own
// clients, expressed the way a subscription refresh can consume it as an
// upstream proxy:
//
//   - pool / hybrid: the shared pool listener (listener.address:listener.port)
//   - multi-port:    the first node port that carries credentials
//
// The host is rewritten to loopback when the entry listens on a wildcard,
// because the fallback fetch runs inside the same process. An empty string
// means the project currently has no proxy entry that could be borrowed.
func ProjectProxyURL(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	switch strings.TrimSpace(cfg.Mode) {
	case "", "pool", "hybrid":
		if cfg.Listener.Port == 0 {
			return ""
		}
		return buildHTTPProxyURL(cfg.Listener.Address, cfg.Listener.Port, cfg.Listener.Username, cfg.Listener.Password)
	case "multi-port":
		for _, node := range cfg.Nodes {
			if node.Disabled || node.Port == 0 {
				continue
			}
			username, password := node.Username, node.Password
			if username == "" {
				username, password = cfg.MultiPort.Username, cfg.MultiPort.Password
			}
			return buildHTTPProxyURL(cfg.MultiPort.Address, node.Port, username, password)
		}
	}
	return ""
}

func buildHTTPProxyURL(address string, port uint16, username, password string) string {
	host := loopbackHost(address)
	if host == "" || port == 0 {
		return ""
	}
	auth := ""
	if username != "" {
		auth = url.UserPassword(username, password).String() + "@"
	}
	return fmt.Sprintf("http://%s%s:%d", auth, host, port)
}

// loopbackHost normalizes a configured listen address into a host this process
// can dial. Wildcard binds are mapped to 127.0.0.1.
func loopbackHost(address string) string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return "127.0.0.1"
	}
	host := trimmed
	if splitHost, _, err := net.SplitHostPort(trimmed); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	switch host {
	case "", "*", "0.0.0.0", "::":
		return "127.0.0.1"
	}
	return host
}

// subscriptionFetchFailed reports whether an attempt produced no usable result.
// A subscription that merely reports "expired" or "quota_exhausted" answered
// successfully, so it must never trigger a proxy retry.
func subscriptionFetchFailed(result subscriptionFetchResult) bool {
	return result.err != nil
}
