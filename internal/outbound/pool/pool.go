package pool

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Proxy2API/internal/monitor"
	"Proxy2API/internal/successrate"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	// Type is the outbound type name exposed to sing-box.
	Type = "pool"
	// Tag is the default outbound tag used by builder.
	Tag = "proxy-pool"

	modeSequential = "sequential"
	modeRandom     = "random"
	modeBalance    = "balance"
	modeLatency    = "latency"
	modeSuccess    = "success"
)

// Options controls pool outbound behaviour.
type Options struct {
	Mode              string
	Members           []string
	FailureThreshold  int
	BlacklistDuration time.Duration
	// RetryEnabled toggles automatic fail-over on dial failure.
	RetryEnabled bool
	// RetryAttempts is the maximum total dial attempts (including the first).
	// Multi-member pools pick a different member per retry; single-member pools retry the same member.
	RetryAttempts int
	Metadata      map[string]MemberMeta
	// HonorStickySelection makes this pool follow the monitor's sticky-entry
	// node choice. It is enabled only for the dedicated sticky listener.
	HonorStickySelection bool
	// EntryPort identifies a shared listener in the monitor. Successful dials
	// update that listener's most recent exit node for the WebUI.
	EntryPort uint16
	// Sticky pins each client (by source IP) to a single member, only
	// re-selecting when the pinned member becomes unavailable. Pool/hybrid entry only.
	Sticky bool
	// SuccessWindowSize is the number of recent dial attempts the "success" mode
	// scores. 0 = default 64; negative = legacy cumulative counters.
	SuccessWindowSize int
	// SuccessPriorStrength is the pseudo-count that pulls a small sample toward
	// the pool mean, so one lucky dial cannot outrank hundreds of good ones.
	// 0 = default 8; negative = no smoothing.
	SuccessPriorStrength int
	// SuccessExploreEvery diverts one dial out of N to the candidate whose
	// window is the stalest, so cold and recovered nodes keep collecting
	// evidence. 0 = default 50; negative = never explore.
	SuccessExploreEvery int
	// SuccessHalfLife is the observation half-life; zero uses 30 minutes.
	SuccessHalfLife time.Duration
	// SuccessHotspotBand gates load/latency selection to similarly reliable nodes.
	// 0 = default 0.03; negative = strict reliability argmax.
	SuccessHotspotBand float64
}

// MemberMeta carries optional descriptive information for monitoring UI.
type MemberMeta struct {
	ID              string
	Order           int
	Name            string
	URI             string
	Source          string
	Username        string
	Password        string
	Mode            string
	ListenAddress   string
	Port            uint16
	SubscriptionURL string
	Suppressed      bool
	// Quarantined mirrors the project's manual quarantine ("小黑屋") state for
	// this member. It is never derived from failures.
	Quarantined    bool
	MaxConnections int
}

// Register wires the pool outbound into the registry.
func Register(registry *outbound.Registry) {
	outbound.Register[Options](registry, Type, newPool)
}

type memberState struct {
	outbound adapter.Outbound
	tag      string
	entry    *monitor.EntryHandle
	shared   *sharedMemberState
}

type poolOutbound struct {
	outbound.Adapter
	ctx               context.Context
	logger            singlog.ContextLogger
	manager           adapter.OutboundManager
	options           Options
	mode              string
	members           []*memberState
	mu                sync.Mutex
	rrCounter         atomic.Uint32
	rng               *rand.Rand
	rngMu             sync.Mutex // protects rng for random mode
	monitor           *monitor.Manager
	sharedStates      *SharedStateStore
	candidatesPool    sync.Pool
	sticky            bool
	stickyMu          sync.Mutex        // protects stickyMap
	stickyMap         map[string]string // sticky key (client source IP) -> member tag
	stickySelection   string            // last configured sticky node; protects stale pins
	lastUsedMember    *memberState      // fallback when no healthy nodes available
	lastExploration   uint64            // guarded by the shared selection mutex
	selectionCount    uint64
	explorationMember *memberState
	lastUsedMu        sync.RWMutex // protects lastUsedMember
}

func newPool(ctx context.Context, _ adapter.Router, logger singlog.ContextLogger, tag string, options Options) (adapter.Outbound, error) {
	if len(options.Members) == 0 {
		return nil, E.New("节点池至少需要一个成员")
	}
	manager := service.FromContext[adapter.OutboundManager](ctx)
	if manager == nil {
		return nil, E.New("上下文中缺少出站管理器")
	}
	monitorMgr := monitor.FromContext(ctx)
	sharedStates := sharedStateStoreFromContext(ctx)
	normalized := normalizeOptions(options)
	memberCount := len(normalized.Members)
	p := &poolOutbound{
		Adapter:      outbound.NewAdapter(Type, tag, []string{N.NetworkTCP, N.NetworkUDP}, normalized.Members),
		ctx:          ctx,
		logger:       logger,
		manager:      manager,
		options:      normalized,
		mode:         normalized.Mode,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor:      monitorMgr,
		sharedStates: sharedStates,
		sticky:       normalized.Sticky,
		candidatesPool: sync.Pool{
			New: func() any {
				return make([]*memberState, 0, memberCount)
			},
		},
	}
	if p.sticky {
		p.stickyMap = make(map[string]string)
	}

	// Register nodes immediately if monitor is available
	if monitorMgr != nil {
		if memberCount > 1 {
			logger.Info("正在向监控器注册 ", memberCount, " 个节点")
		}
		registeredCount := 0
		for _, memberTag := range normalized.Members {
			// Acquire shared state for this tag (creates if not exists)
			state := p.sharedStates.acquire(memberTag)

			meta := normalized.Metadata[memberTag]
			info := monitor.NodeInfo{
				ID:              meta.ID,
				Order:           meta.Order,
				Tag:             memberTag,
				Name:            meta.Name,
				URI:             meta.URI,
				Mode:            meta.Mode,
				ListenAddress:   meta.ListenAddress,
				Port:            meta.Port,
				SubscriptionURL: meta.SubscriptionURL,
				Source:          meta.Source,
				Username:        meta.Username,
				Password:        meta.Password,
				Suppressed:      meta.Suppressed,
				Quarantined:     meta.Quarantined,
				MaxConnections:  normalizedCapacity(meta.MaxConnections),
			}
			state.configureCapacity(meta.MaxConnections)
			entry := monitorMgr.Register(info)
			if entry != nil {
				// Attach entry to shared state so all pool instances share it
				state.attachEntry(entry)
				state.setReleaseProbe(p.makeReleaseProbeFunc(memberTag))
				registeredCount++
				// Set probe, release, and blacklist functions immediately
				entry.SetRelease(p.makeReleaseByTagFunc(memberTag))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(memberTag))
				if probeFn := p.makeProbeByTagFunc(memberTag); probeFn != nil {
					entry.SetProbe(probeFn)
				}
				if traceFn := p.makeTraceByTagFunc(memberTag); traceFn != nil {
					entry.SetTraceProbe(traceFn)
				}
				state.restore(entry)
			} else {
				logger.Warn("注册节点失败: ", memberTag)
			}
		}
		if memberCount > 1 {
			logger.Info("已向监控器注册 ", registeredCount, " 个节点")
		}
	} else {
		logger.Warn("监控管理器为空，已跳过节点注册")
	}

	return p, nil
}

func normalizeOptions(options Options) Options {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 3
	}
	if options.BlacklistDuration <= 0 {
		options.BlacklistDuration = 30 * time.Minute
	}
	if options.RetryAttempts <= 0 {
		options.RetryAttempts = 3
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	case modeSuccess, "highest-success-rate":
		options.Mode = modeSuccess
	case modeLatency, "lowest-latency":
		options.Mode = modeLatency
	case "":
		options.Mode = modeLatency
	default:
		options.Mode = modeSequential
	}
	options.SuccessHalfLife = successrate.HalfLife(options.SuccessHalfLife)
	if options.SuccessWindowSize == 0 {
		options.SuccessWindowSize = successWindowLimit
	} else if options.SuccessWindowSize > successWindowLimit {
		options.SuccessWindowSize = successWindowLimit
	}
	if options.SuccessPriorStrength == 0 {
		options.SuccessPriorStrength = defaultSuccessPriorStrength
	}
	if options.SuccessExploreEvery == 0 {
		options.SuccessExploreEvery = defaultSuccessExploreEvery
	} else if options.SuccessExploreEvery > maxSuccessExploreEvery {
		// Bounded so the uint32 modulo used by the canary can never wrap to zero.
		options.SuccessExploreEvery = maxSuccessExploreEvery
	}
	if options.SuccessHotspotBand == 0 {
		options.SuccessHotspotBand = defaultSuccessHotspotBand
	}
	return options
}

func (p *poolOutbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	p.mu.Lock()
	err := p.initializeMembersLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// Initial health checks are driven centrally by the monitor's probeAllNodes
	// (StartPeriodicHealthCheck on boot, ProbeAllNow on reload), bounded by the
	// configured probe concurrency. A per-pool startup probe re-probed the same
	// nodes and, with one pool per node, required a process-wide semaphore to
	// avoid fd exhaustion; routing all probing through the monitor removes both.
	return nil
}

// initializeMembersLocked must be called with p.mu held
func (p *poolOutbound) initializeMembersLocked() error {
	if len(p.members) > 0 {
		return nil // Already initialized
	}

	members := make([]*memberState, 0, len(p.options.Members))
	for _, tag := range p.options.Members {
		detour, loaded := p.manager.Outbound(tag)
		if !loaded {
			return E.New("节点池成员不存在: ", tag)
		}

		// Acquire shared state (creates if not exists, reuses if already created)
		state := p.sharedStates.acquire(tag)
		state.configureCapacity(p.options.Metadata[tag].MaxConnections)

		member := &memberState{
			outbound: detour,
			tag:      tag,
			shared:   state,
			entry:    state.entryHandle(),
		}

		// Connect to existing monitor entry if available
		if p.monitor != nil {
			meta := p.options.Metadata[tag]
			info := monitor.NodeInfo{
				ID:              meta.ID,
				Order:           meta.Order,
				Tag:             tag,
				Name:            meta.Name,
				URI:             meta.URI,
				Mode:            meta.Mode,
				ListenAddress:   meta.ListenAddress,
				Port:            meta.Port,
				SubscriptionURL: meta.SubscriptionURL,
				Source:          meta.Source,
				Username:        meta.Username,
				Password:        meta.Password,
				Suppressed:      meta.Suppressed,
				Quarantined:     meta.Quarantined,
				MaxConnections:  normalizedCapacity(meta.MaxConnections),
			}
			state.configureCapacity(meta.MaxConnections)
			entry := p.monitor.Register(info)
			if entry != nil {
				state.attachEntry(entry)
				state.setReleaseProbe(p.makeReleaseProbeFunc(member.tag))
				member.entry = entry
				entry.SetRelease(p.makeReleaseFunc(member))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(member.tag))
				if probe := p.makeProbeFunc(member); probe != nil {
					entry.SetProbe(probe)
				}
				if traceFn := p.makeTraceFunc(member); traceFn != nil {
					entry.SetTraceProbe(traceFn)
				}
				state.restore(entry)
			}
		}
		members = append(members, member)
	}
	p.members = members
	p.logger.Info("节点池初始化完成，共 ", len(members), " 个成员")

	return nil
}

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	p.noteApplication(destination)
	maxAttempts := p.maxAttempts()
	stickyKey := p.stickyKeyFromCtx(ctx)
	singleMember := len(p.options.Members) <= 1
	var tried map[string]bool
	if !singleMember && maxAttempts > 1 {
		tried = make(map[string]bool, maxAttempts)
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lease, err := p.acquireMember(network, tried, stickyKey)
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w（已尝试 %d 次；上次错误：%v）", err, attempt-1, lastErr)
			}
			return nil, err
		}
		member := lease.member
		started := time.Now()
		conn, dialErr := member.outbound.DialContext(ctx, network, destination)
		if dialErr != nil {
			if clientAborted(ctx, dialErr) {
				// The caller gave up before the node answered. That is no
				// evidence about node health, so it must not lower the node's
				// success rate — and retrying a cancelled request is pointless.
				lease.release()
				p.logger.Debug("客户端已取消请求，不计入 ", member.tag, " 的失败统计: ", dialErr)
				return nil, dialErr
			}
			p.recordFailure(member, dialErr, lease)
			lease.release()
			lastErr = dialErr
			if tried != nil {
				tried[member.tag] = true
			}
			if attempt < maxAttempts {
				p.logger.Warn("通过 ", member.tag, " 拨号失败（第 ", attempt, "/", maxAttempts, " 次），正在重试: ", dialErr)
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}
		if attempt > 1 {
			p.logger.Info("通过 ", member.tag, " 拨号成功，共尝试 ", attempt, " 次")
		}
		p.recordSuccess(member, time.Since(started), lease)
		return &trackedConn{Conn: conn, release: lease.release}, nil
	}
	if lastErr == nil {
		lastErr = E.New("没有可用的健康代理")
	}
	return nil, fmt.Errorf("拨号在尝试 %d 次后仍失败: %w", maxAttempts, lastErr)
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	p.noteApplication(destination)
	maxAttempts := p.maxAttempts()
	stickyKey := p.stickyKeyFromCtx(ctx)
	singleMember := len(p.options.Members) <= 1
	var tried map[string]bool
	if !singleMember && maxAttempts > 1 {
		tried = make(map[string]bool, maxAttempts)
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lease, err := p.acquireMember(N.NetworkUDP, tried, stickyKey)
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w（已尝试 %d 次；上次错误：%v）", err, attempt-1, lastErr)
			}
			return nil, err
		}
		member := lease.member
		started := time.Now()
		conn, listenErr := member.outbound.ListenPacket(ctx, destination)
		if listenErr != nil {
			if clientAborted(ctx, listenErr) {
				lease.release()
				p.logger.Debug("客户端已取消请求，不计入 ", member.tag, " 的失败统计: ", listenErr)
				return nil, listenErr
			}
			p.recordFailure(member, listenErr, lease)
			lease.release()
			lastErr = listenErr
			if tried != nil {
				tried[member.tag] = true
			}
			if attempt < maxAttempts {
				p.logger.Warn("通过 ", member.tag, " 监听数据包失败（第 ", attempt, "/", maxAttempts, " 次），正在重试: ", listenErr)
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			break
		}
		if attempt > 1 {
			p.logger.Info("通过 ", member.tag, " 监听数据包成功，共尝试 ", attempt, " 次")
		}
		p.recordSuccess(member, time.Since(started), lease)
		return &trackedPacketConn{PacketConn: conn, release: lease.release}, nil
	}
	if lastErr == nil {
		lastErr = E.New("没有可用的健康代理")
	}
	return nil, fmt.Errorf("监听数据包在尝试 %d 次后仍失败: %w", maxAttempts, lastErr)
}

// noteApplication records that an application just used this project's nodes.
// It is the signal that ends an idle cool-down, so it is recorded for every
// dial attempt — a failing request still proves the project is in use. The
// monitor may answer by clearing the cool-down and scheduling a health sweep,
// which is why this is called before any selection work.
//
// Proxy2API's own subscription fetch borrows this proxy entry when its direct
// attempt fails, so requests to a registered internal host are skipped: they are
// not client traffic and must not keep the project awake.
func (p *poolOutbound) noteApplication(destination M.Socksaddr) {
	if p.monitor == nil {
		return
	}
	if p.monitor.IsInternalDestination(destination.AddrString()) {
		return
	}
	p.monitor.MarkApplied()
}

// maxAttempts returns the configured retry budget (>=1).
func (p *poolOutbound) maxAttempts() int {
	if !p.options.RetryEnabled {
		return 1
	}
	if p.options.RetryAttempts < 1 {
		return 1
	}
	return p.options.RetryAttempts
}

// clientAborted reports whether a dial failure came from the caller cancelling
// the request rather than from the node being unhealthy. Only an explicit
// cancellation qualifies: DeadlineExceeded is ambiguous (the node may simply be
// too slow), so it keeps counting against the node.
func clientAborted(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	return ctx != nil && ctx.Err() == context.Canceled
}

// acquireMember serializes selection plus reservation across all listeners in a
// project. Network I/O happens after releasing this lock.
func (p *poolOutbound) acquireMember(network string, tried map[string]bool, stickyKey string) (*memberLease, error) {
	p.sharedStates.selectionMu.Lock()
	defer p.sharedStates.selectionMu.Unlock()
	now := time.Now()
	candidates := p.getCandidateBuffer()
	defer func() { p.putCandidateBuffer(candidates) }()
	p.mu.Lock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
	}
	candidates = p.availableMembersLocked(now, network, candidates)
	p.mu.Unlock()
	hadHealthy := len(candidates) > 0
	usable := candidates[:0]
	for _, member := range candidates {
		if member.shared.canReserve(p.mode == modeSuccess && coldMember(member), false, now) {
			usable = append(usable, member)
		}
	}
	candidates = usable
	if p.options.HonorStickySelection && p.monitor != nil {
		selectedTag := p.monitor.StickyNode()
		p.syncStickySelection(selectedTag)
		if selectedTag != "" {
			for _, member := range candidates {
				if member.tag == selectedTag {
					if lease := member.shared.reserve(member, false, false, false, now); lease != nil {
						return lease, nil
					}
				}
			}
			return nil, E.New("选定的粘性代理节点不可用或已达连接上限: ", selectedTag)
		}
	}
	if len(candidates) == 0 {
		if hadHealthy {
			return nil, E.New("可用节点均已达连接或试探上限")
		}
		p.lastUsedMu.RLock()
		fallback := p.lastUsedMember
		p.lastUsedMu.RUnlock()
		if usableFallback(fallback) && common.Contains(fallback.outbound.Network(), network) {
			if lease := fallback.shared.reserve(fallback, false, false, true, now); lease != nil {
				p.logger.Warn("无健康节点，限流试探最后成功节点: ", fallback.tag)
				return lease, nil
			}
		}
		return nil, E.New("没有可用的健康代理")
	}
	if len(tried) > 0 {
		filtered := candidates[:0]
		for _, member := range candidates {
			if !tried[member.tag] {
				filtered = append(filtered, member)
			}
		}
		if len(filtered) > 0 {
			candidates = filtered
		}
	}
	for len(candidates) > 0 {
		p.explorationMember = nil
		member := p.selectMember(candidates, stickyKey)
		if member == nil {
			break
		}
		if lease := member.shared.reserve(member, p.mode == modeSuccess && coldMember(member), p.explorationMember == member, false, now); lease != nil {
			return lease, nil
		}
		// A concurrent result may have closed the circuit after candidate filtering.
		for i, candidate := range candidates {
			if candidate == member {
				candidates = append(candidates[:i], candidates[i+1:]...)
				break
			}
		}
	}
	return nil, E.New("节点在选取期间变为不可用")
}

func (p *poolOutbound) availableMembersLocked(now time.Time, network string, buf []*memberState) []*memberState {
	result := buf[:0]
	for _, member := range p.members {
		// Check blacklist first so expiry can release a node even though entering
		// the blacklist also marks it unavailable pending the release probe.
		if member.shared != nil && member.shared.isBlacklisted(now) {
			// Log blacklisted nodes for debugging
			remaining := member.shared.blacklistRemaining(now)
			if remaining > 0 {
				p.logger.Debug("跳过已拉黑节点：", member.tag, "，剩余时间：", remaining.Round(time.Second))
			}
			continue
		}
		if member.entry != nil {
			// Manual quarantine ("小黑屋") is an operator decision, not a health
			// signal: the node is skipped regardless of probe results until it is
			// manually released. It is checked before Healthy() so a quarantined
			// node never re-enters routing on its own.
			if member.entry.Quarantined() {
				p.logger.Debug("跳过小黑屋节点：", member.tag)
				continue
			}
			if member.entry.Suppressed() || !member.entry.Healthy() {
				continue
			}
		}
		if network != "" && !common.Contains(member.outbound.Network(), network) {
			continue
		}
		result = append(result, member)
	}
	return result
}

// usableFallback reports whether a member may still serve as the degraded
// fallback used when no healthy candidate exists. Quarantined nodes are never a
// fallback: the operator deliberately parked them, so only a manual release may
// bring them back into routing.
func usableFallback(member *memberState) bool {
	if member == nil {
		return false
	}
	return member.shared != nil && (member.entry == nil || (!member.entry.Quarantined() && !member.entry.Suppressed()))
}

const stickyFallbackKey = "_global_"

// stickyKeyFromCtx returns the sticky key (client source IP) for this request,
// or "" when stickiness is disabled. Falls back to a shared global key when the
// source address cannot be determined, so such requests still pin together.
func (p *poolOutbound) stickyKeyFromCtx(ctx context.Context) string {
	if !p.sticky {
		return ""
	}
	if md := adapter.ContextFrom(ctx); md != nil && md.Source.IsValid() {
		return md.Source.AddrString()
	}
	return stickyFallbackKey
}

// selectMember picks a member, honouring stickiness when stickyKey is non-empty.
func (p *poolOutbound) selectMember(candidates []*memberState, stickyKey string) *memberState {
	if stickyKey != "" {
		return p.selectSticky(candidates, stickyKey)
	}
	return p.selectByMode(candidates)
}

// selectSticky returns the member pinned to stickyKey if it is still among the
// candidates; otherwise it selects a fresh member (by mode) and pins it. The
// pin is permanent until the member drops out of the candidate set
// (blacklisted/removed), at which point a new member is chosen and pinned.
func (p *poolOutbound) selectSticky(candidates []*memberState, stickyKey string) *memberState {
	p.stickyMu.Lock()
	defer p.stickyMu.Unlock()
	if tag, ok := p.stickyMap[stickyKey]; ok {
		for _, member := range candidates {
			if member.tag == tag {
				return member
			}
		}
		// Pinned member is no longer available: drop the stale pin and re-select.
		delete(p.stickyMap, stickyKey)
	}
	member := p.selectByMode(candidates)
	if member != nil {
		p.stickyMap[stickyKey] = member.tag
	}
	return member
}

// syncStickySelection invalidates source-IP pins whenever the configured
// sticky node changes. Clearing a specified node therefore immediately returns
// clients to lowest-latency selection instead of reviving old pins.
func (p *poolOutbound) syncStickySelection(tag string) {
	if !p.sticky {
		return
	}
	tag = strings.TrimSpace(tag)
	p.stickyMu.Lock()
	defer p.stickyMu.Unlock()
	if tag == p.stickySelection {
		return
	}
	clear(p.stickyMap)
	p.stickySelection = tag
}

// selectByMode applies the configured scheduling strategy.
func (p *poolOutbound) selectByMode(candidates []*memberState) *memberState {
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		idx := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		return candidates[idx]
	case modeBalance:
		var selected *memberState
		var minActive int32
		for _, member := range candidates {
			var active int32
			if member.shared != nil {
				active = member.shared.activeCount()
			}
			if selected == nil || active < minActive {
				selected = member
				minActive = active
			}
		}
		return selected
	case modeSuccess:
		return p.selectBySuccess(candidates, time.Now())
	case modeLatency:
		return p.selectLowestLatency(candidates)
	default:
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		return candidates[idx]
	}
}

// successCandidate is one candidate's scored sliding window.
type successCandidate struct {
	member      *memberState
	successes   float64
	attempts    float64
	rawAttempts int
	lastAt      time.Time
	score       float64
}

// selectBySuccess implements the "success" strategy:
//
//	score_i = Beta lower bound of the smoothed success rate (see successScore)
//
// S_i and n_i come from the sliding window of the last `window` dial attempts,
// p0 is the pool-wide mean success rate and m pulls small samples toward it. The
// lower confidence bound is what keeps one lucky dial from outranking hundreds
// of good ones; the window is what lets a node recover from an old bad patch.
// Within hotspotBand, capacity-normalized load and actual dial latency decide.
// Exploration is budgeted, with priority for recovered and undersampled nodes.
func (p *poolOutbound) selectBySuccess(candidates []*memberState, now time.Time) *memberState {
	if len(candidates) == 0 {
		return nil
	}
	if p.options.SuccessWindowSize < 0 {
		return p.selectBySuccessLegacy(candidates)
	}
	window := p.options.SuccessWindowSize
	priorStrength := p.options.SuccessPriorStrength
	if priorStrength < 0 {
		priorStrength = 0
	}

	stats := make([]successCandidate, 0, len(candidates))
	totalSuccess, totalAttempts := 0.0, 0.0
	for _, member := range candidates {
		_, rawAttempts, lastAt := recentOutcomes(member, window)
		var successes, attempts float64
		if member.entry != nil {
			successes, attempts = member.entry.WeightedWindow(window, p.options.SuccessHalfLife, now)
		}
		stats = append(stats, successCandidate{
			member:      member,
			successes:   successes,
			attempts:    attempts,
			rawAttempts: rawAttempts,
			lastAt:      lastAt,
		})
		totalSuccess += successes
		totalAttempts += attempts
	}

	prior := defaultSuccessPrior
	if totalAttempts > 0 {
		prior = float64(totalSuccess) / float64(totalAttempts)
	}
	for i := range stats {
		stats[i].score = successScore(stats[i].successes, stats[i].attempts, prior, float64(priorStrength))
	}

	best := 0
	for i := 1; i < len(stats); i++ {
		switch {
		case stats[i].score > stats[best].score+rateEpsilon:
		case stats[i].score < stats[best].score-rateEpsilon:
			continue
		case !preferOnTie(stats[i].member, stats[best].member):
			continue
		}
		best = i
	}

	p.selectionCount++
	if p.options.SuccessExploreEvery > 0 {
		if canary, warm := explorationCandidate(stats, best, now); canary >= 0 {
			period := uint64(p.options.SuccessExploreEvery)
			if warm {
				period = (period + 3) / 4
			} // at most 4x the configured budget
			if period < 1 {
				period = 1
			}
			if p.selectionCount-p.lastExploration >= period {
				p.lastExploration = p.selectionCount
				p.explorationMember = stats[canary].member
				return stats[canary].member
			}
		}
	}
	if band := p.options.SuccessHotspotBand; band > 0 {
		best = leastLoadedWithinBand(stats, best, band, now)
	}

	p.logger.Debug("success 调度：候选 ", len(stats), " 个，选中 ", stats[best].member.tag,
		"，得分 ", stats[best].score, "，窗口 ", stats[best].successes, "/", stats[best].attempts,
		"，池均值 ", prior)
	return stats[best].member
}

// selectBySuccessLegacy is the pre-window behaviour: strict argmax over the
// cumulative success ratio, skipping nodes that have never been dialled. Kept as
// an escape hatch (success_window: -1) so the old behaviour stays one config
// value away.
func (p *poolOutbound) selectBySuccessLegacy(candidates []*memberState) *memberState {
	var selected *memberState
	var bestRate float64
	hasRate := false
	for _, member := range candidates {
		if member.entry == nil {
			continue
		}
		rate, ok := member.entry.SuccessRate()
		if !ok {
			continue
		}
		switch {
		case !hasRate:
		case rate > bestRate+rateEpsilon:
		case rate < bestRate-rateEpsilon:
			continue
		case !preferOnTie(member, selected):
			continue
		}
		selected = member
		bestRate = rate
		hasRate = true
	}
	if selected != nil {
		p.logger.Debug("success 调度（legacy）：候选 ", len(candidates), " 个，选中 ", selected.tag,
			"，累计成功率 ", bestRate*100, "%")
		return selected
	}
	p.logger.Debug("success 调度（legacy）：候选 ", len(candidates), " 个均无成功率数据，降级为最低延迟")
	return p.selectLowestLatency(candidates)
}

// explorationCandidate prefers recovery and new nodes without duplicating
// outstanding dials. Known poor nodes get a slower retry cadence even if idle.
func explorationCandidate(stats []successCandidate, best int, now time.Time) (int, bool) {
	chosen, priority := -1, -1
	var oldest time.Time
	for i, item := range stats {
		if i == best || item.member.shared == nil {
			continue
		}
		_, _, pending, recovering, selected, next := item.member.shared.schedulingState()
		if pending > 0 || now.Before(next) {
			continue
		}
		if selected.Before(item.lastAt) {
			selected = item.lastAt
		}
		poor := item.rawAttempts >= 8 && item.attempts > 0 && item.successes/item.attempts < 0.5
		if poor && now.Sub(selected) < 5*time.Minute && !recovering {
			continue
		}
		rank := 0
		if recovering {
			rank = 3
		} else if item.rawAttempts < 8 {
			rank = 2
		} else if item.attempts < 8 {
			rank = 1
		}
		if chosen < 0 || rank > priority || (rank == priority && (selected.Before(oldest) || (selected.Equal(oldest) && item.member.tag < stats[chosen].member.tag))) {
			chosen, priority, oldest = i, rank, selected
		}
	}
	return chosen, priority >= 2
}

// Reliability gates the candidate set. Only then do capacity-normalized load
// and bounded latency cost matter. Equal-cost nodes rotate by last selection.
func leastLoadedWithinBand(stats []successCandidate, best int, band float64, now time.Time) int {
	chosen := best
	cost, last := dispatchCost(stats[best].member, now)
	threshold := stats[best].score - band
	for i := range stats {
		if i == best || stats[i].score < threshold {
			continue
		}
		candidateCost, candidateLast := dispatchCost(stats[i].member, now)
		if candidateCost < cost-rateEpsilon || (math.Abs(candidateCost-cost) <= rateEpsilon && (candidateLast < last || (candidateLast == last && stats[i].member.tag < stats[chosen].member.tag))) {
			chosen, cost, last = i, candidateCost, candidateLast
		}
	}
	return chosen
}

func dispatchCost(member *memberState, now time.Time) (float64, uint64) {
	active, capacity, _, _, _, _ := member.shared.schedulingState()
	if capacity <= 0 {
		capacity = defaultNodeCapacity
	}
	latency := time.Duration(0)
	if member.entry != nil {
		latency = member.entry.DialLatency(now)
	}
	if latency <= 0 {
		latency = measuredLatency(member)
	}
	if latency <= 0 {
		latency = 250 * time.Millisecond
	}
	latencyCost := 0.25 * float64(latency) / float64(latency+250*time.Millisecond)
	return float64(active)/float64(capacity) + latencyCost, member.shared.selectionOrder()
}

// recentOutcomes reads a member's sliding window. A member without a monitor
// entry (monitoring disabled) is treated as never dialled.
func recentOutcomes(member *memberState, window int) (successes, attempts int, last time.Time) {
	if member == nil || member.entry == nil {
		return 0, 0, time.Time{}
	}
	return member.entry.RecentWindow(window)
}

func successScore(successes, attempts, prior, priorStrength float64) float64 {
	return successrate.Score(successes, attempts, prior, priorStrength)
}

// selectLowestLatency picks the candidate with the lowest measured latency.
// Candidates without a latency reading (never probed) are deprioritized;
// if all are unmeasured, it falls back to round-robin.
func (p *poolOutbound) selectLowestLatency(candidates []*memberState) *memberState {
	var selected *memberState
	var minLatency time.Duration
	hasMeasured := false
	for _, member := range candidates {
		if member.entry == nil {
			continue
		}
		latency := member.entry.LastLatency()
		if latency <= 0 {
			continue
		}
		if !hasMeasured || latency < minLatency {
			selected = member
			minLatency = latency
			hasMeasured = true
		}
	}
	if selected != nil {
		return selected
	}
	// Fallback: no measurements yet — round-robin.
	idx := int(p.rrCounter.Add(1)-1) % len(candidates)
	return candidates[idx]
}

// rateEpsilon is the tolerance used when comparing two success rates. Rates are
// ratios of two integers, so a tie means the fractions are equal; the tolerance
// only absorbs float rounding error.
const rateEpsilon = 1e-9

// Tuning for the "success" mode. The window matches the monitor's bitmask width;
// everything below is deliberately conservative so the scheduler degrades toward
// the previous greedy behaviour instead of oscillating.
const (
	// successWindowLimit is the widest usable window (one 64-bit word).
	successWindowLimit = successrate.WindowLimit
	// defaultSuccessPriorStrength is the pseudo-count pulling a small sample
	// toward the pool mean. Eight samples' worth of doubt: a node needs roughly
	// that many dials before its own rate dominates its score.
	defaultSuccessPriorStrength = successrate.DefaultPriorStrength
	// defaultSuccessPrior is the pool mean assumed when no candidate has any
	// window data at all (fresh install, monitoring just enabled).
	defaultSuccessPrior = successrate.DefaultPrior
	// defaultSuccessExploreEvery sends one dial in fifty to the stalest node.
	defaultSuccessExploreEvery = 50
	// maxSuccessExploreEvery bounds the exploration period; larger values are
	// clamped so the uint32 modulo cannot wrap to zero.
	maxSuccessExploreEvery = 1 << 20
	// defaultSuccessHotspotBand is the score band inside which the least busy
	// node wins, in probability points (0.03 = 3 percentage points).
	defaultSuccessHotspotBand = 0.03
)

// preferOnTie orders two candidates whose success rates are indistinguishable,
// and does so deterministically: measured latency first (an unmeasured node
// loses), then the number of active connections, then the tag so the outcome
// never depends on candidate ordering.
func preferOnTie(a, b *memberState) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	aLatency, bLatency := measuredLatency(a), measuredLatency(b)
	switch {
	case aLatency > 0 && bLatency > 0 && aLatency != bLatency:
		return aLatency < bLatency
	case aLatency > 0 && bLatency <= 0:
		return true
	case aLatency <= 0 && bLatency > 0:
		return false
	}
	if aActive, bActive := activeCountOf(a), activeCountOf(b); aActive != bActive {
		return aActive < bActive
	}
	return a.tag < b.tag
}

// measuredLatency returns the last probe latency, or 0 when the node has no
// measurement (unprobed, or currently unhealthy).
func measuredLatency(member *memberState) time.Duration {
	if member == nil || member.entry == nil {
		return 0
	}
	return member.entry.LastLatency()
}

// activeCountOf returns the live connection count of a member, or 0 when the
// member has no shared state attached.
func activeCountOf(member *memberState) int32 {
	if member == nil || member.shared == nil {
		return 0
	}
	return member.shared.activeCount()
}

func (p *poolOutbound) recordFailure(member *memberState, cause error, lease *memberLease) {
	if member.shared == nil {
		p.logger.Warn("代理 ", member.tag, " 失败（无共享状态）: ", cause)
		return
	}
	failures, blacklisted, until, transient := member.shared.recordFailure(cause, p.options.FailureThreshold, p.options.BlacklistDuration, lease)
	switch {
	case transient:
		// Transient transport failures use jittered exponential backoff.
		p.logger.Warn("代理 ", member.tag, " 暂时失败，冷却至 ", until.Format("15:04:05"), ": ", cause)
		log.Printf("[节点池] %s 暂时失败，冷却至 %s: %v", member.tag, until.Format("15:04:05"), cause)
	case blacklisted:
		p.logger.Warn("代理 ", member.tag, " 已拉黑 ", p.options.BlacklistDuration, ": ", cause)
		log.Printf("⚠️  [节点池] %s 已拉黑 %s（至 %s）: %v", member.tag, p.options.BlacklistDuration, until.Format("15:04:05"), cause)
		log.Printf("    如需立即解除，请使用网页管理界面或请求：POST /api/nodes/%s/release", member.tag)
	default:
		p.logger.Warn("代理 ", member.tag, " 失败 ", failures, "/", p.options.FailureThreshold, ": ", cause)
		log.Printf("[节点池] %s 失败 %d/%d: %v", member.tag, failures, p.options.FailureThreshold, cause)
	}
}

func (p *poolOutbound) recordSuccess(member *memberState, latency time.Duration, lease *memberLease) {
	if member.shared != nil {
		member.shared.recordSuccess(latency, lease)
	}
	if p.monitor != nil && p.options.EntryPort > 0 {
		p.monitor.RecordEntryExit(p.options.EntryPort, member.tag)
	}
	// 更新最后一次成功使用的节点
	p.lastUsedMu.Lock()
	p.lastUsedMember = member
	p.lastUsedMu.Unlock()
}

func (p *poolOutbound) makeReleaseFunc(member *memberState) func() {
	return func() {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
}

// upgradeProbeConn optionally upgrades a plain TCP connection to TLS. When
// useTLS is false the connection is returned unchanged. host is used as the
// TLS SNI, and tlsInsecure controls certificate verification. A handshake failure
// (e.g. self-signed or hijacked certificate) is returned as an error so the
// caller can record it and eventually blacklist the node.
func upgradeProbeConn(ctx context.Context, conn net.Conn, host string, useTLS, tlsInsecure bool) (net.Conn, error) {
	if !useTLS {
		return conn, nil
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: tlsInsecure,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		// Return the original connection so the caller's deferred Close keeps
		// working; the failed handshake did not take over its lifetime.
		return conn, fmt.Errorf("TLS 握手失败: %w", err)
	}
	return tlsConn, nil
}

// httpProbe performs the configurable connectivity check and measures TTFB.
func httpProbe(conn net.Conn, host, path string, timeout time.Duration) (time.Duration, error) {
	request := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: Proxy2API-monitor\r\n\r\n", path, host)

	// Try to set write deadline (ignore errors for connections that don't support it)
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))

	// Record time just before sending request
	start := time.Now()

	// Send HTTP request
	if _, err := conn.Write([]byte(request)); err != nil {
		return 0, fmt.Errorf("写入请求失败: %w", err)
	}

	// Try to set read deadline (ignore errors for connections that don't support it)
	_ = conn.SetReadDeadline(time.Now().Add(timeout))

	req, _ := http.NewRequest(http.MethodGet, path, nil)
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return 0, fmt.Errorf("读取响应失败: %w", err)
	}
	defer resp.Body.Close()
	// A proxy that returns an HTTP error page is not a successful connectivity
	// probe. Keep redirects valid for custom probe targets, reject 4xx/5xx.
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		return 0, fmt.Errorf("探测目标返回状态 %s", resp.Status)
	}
	return time.Since(start), nil
}

const (
	probeLocationHost     = "www.cloudflare.com"
	probeLocationPath     = "/cdn-cgi/trace"
	probeTraceMaxRetries  = 3
	probeTraceMaxAttempts = probeTraceMaxRetries + 1

	// probeConnectivityAttempts is how many times the latency (generate_204)
	// request is issued per probe. Two attempts smooth transient jitter; the
	// measured latencies are averaged (see probeConnectivity).
	probeConnectivityAttempts = 2
)

// probeLocation obtains exit metadata through the same upstream node using a
// dedicated, fixed Cloudflare trace request. It only supplies exit metadata and
// is not a health check. Manager invokes it manually or when metadata is missing.
func (p *poolOutbound) probeLocation(ctx context.Context, member *memberState, tlsInsecure bool, attemptTimeout time.Duration) (monitor.ProbeResult, error) {
	var lastErr error
	attempts := 0
	for attempts < probeTraceMaxAttempts {
		attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		result, err := p.probeLocationOnce(attemptCtx, member, tlsInsecure, attemptTimeout)
		cancel()
		result.TraceAttempts = attempts
		if err == nil {
			result.TraceOK = true
			return result, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.New("Trace 请求失败")
	}
	traceErr := fmt.Errorf("Trace 请求在尝试 %d 次后仍失败: %w", attempts, lastErr)
	return monitor.ProbeResult{
		TraceError:    traceErr.Error(),
		TraceAttempts: attempts,
	}, traceErr
}

func (p *poolOutbound) probeLocationOnce(ctx context.Context, member *memberState, tlsInsecure bool, attemptTimeout time.Duration) (monitor.ProbeResult, error) {
	destination := M.ParseSocksaddrHostPort(probeLocationHost, 443)
	rawConn, err := dialProbeTarget(ctx, member, destination)
	if err != nil {
		return monitor.ProbeResult{}, err
	}
	stopWatchdog := watchProbeConnection(ctx, rawConn)
	defer stopWatchdog()
	defer rawConn.Close()

	conn, err := upgradeProbeConn(ctx, rawConn, probeLocationHost, true, tlsInsecure)
	if err != nil {
		return monitor.ProbeResult{}, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(attemptTimeout))
	request := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: Proxy2API-monitor\r\n\r\n", probeLocationPath, probeLocationHost)
	if _, err := conn.Write([]byte(request)); err != nil {
		return monitor.ProbeResult{}, fmt.Errorf("写入 Trace 请求失败: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(attemptTimeout))

	req, _ := http.NewRequest(http.MethodGet, probeLocationPath, nil)
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return monitor.ProbeResult{}, fmt.Errorf("读取 Trace 响应失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return monitor.ProbeResult{}, fmt.Errorf("Trace 接口返回状态 %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return monitor.ProbeResult{}, fmt.Errorf("读取 Trace 响应内容失败: %w", err)
	}
	ip, country := parseCloudflareTrace(body)
	if ip == "" || country == "" {
		return monitor.ProbeResult{}, errors.New("Trace 响应中没有有效的 IP 和地区信息")
	}
	return monitor.ProbeResult{
		IP:      ip,
		Region:  strings.ToLower(country),
		Country: country,
	}, nil
}

func parseCloudflareTrace(body []byte) (ip, country string) {
	for _, line := range strings.Split(string(body), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch key {
		case "ip":
			ip = strings.TrimSpace(value)
		case "loc":
			country = strings.ToUpper(strings.TrimSpace(value))
		}
	}
	if net.ParseIP(ip) == nil {
		ip = ""
	}
	if len(country) != 2 {
		country = ""
	}
	return ip, country
}

// watchProbeConnection closes conn when ctx expires. Some sing-box outbounds
// do not reliably unblock a pending read from context cancellation alone.
func watchProbeConnection(ctx context.Context, conn net.Conn) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

// probeConnectivity performs the configurable generate_204 request. Its child
// context caps the complete attempt, including dialing, TLS and response read.
// The request runs up to probeConnectivityAttempts times to smooth transient
// jitter: latencies are averaged when more than one attempt succeeds, and the
// sole successful measurement is used when exactly one succeeds. Only when
// every attempt fails is the error returned with a zero latency.
func (p *poolOutbound) probeConnectivity(ctx context.Context, member *memberState, destination M.Socksaddr, host, path string, useTLS, tlsInsecure bool, requestTimeout time.Duration) (time.Duration, error) {
	var (
		attemptErrs []error
		latencies   []time.Duration
	)
	for attempt := 0; attempt < probeConnectivityAttempts; attempt++ {
		if attempt > 0 && ctx.Err() != nil {
			// The overall budget is exhausted; keep the results already gathered.
			break
		}
		duration, err := p.probeConnectivityOnce(ctx, member, destination, host, path, useTLS, tlsInsecure, requestTimeout)
		if err != nil {
			attemptErrs = append(attemptErrs, err)
			continue
		}
		latencies = append(latencies, duration)
	}
	if len(latencies) == 0 {
		if len(attemptErrs) == 1 {
			return 0, attemptErrs[0]
		}
		return 0, errors.Join(attemptErrs...)
	}
	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}
	return total / time.Duration(len(latencies)), nil
}

// probeConnectivityOnce runs a single generate_204 request and returns its TTFB.
func (p *poolOutbound) probeConnectivityOnce(ctx context.Context, member *memberState, destination M.Socksaddr, host, path string, useTLS, tlsInsecure bool, requestTimeout time.Duration) (time.Duration, error) {
	start := time.Now()
	rawConn, err := dialProbeTarget(ctx, member, destination)
	if err != nil {
		return 0, err
	}
	// Watchdog: close the raw connection as soon as ctx is done so any blocked
	// Read/Write/handshake below is unblocked even when SetDeadline is a no-op.
	stopWatchdog := watchProbeConnection(ctx, rawConn)
	defer stopWatchdog()
	defer rawConn.Close()

	conn := rawConn
	// Strict mode: upgrade to TLS and verify the certificate chain so that
	// nodes whose exit hijacks TLS (self-signed certs) fail the probe.
	if conn, err = upgradeProbeConn(ctx, conn, host, useTLS, tlsInsecure); err != nil {
		return 0, err
	}

	// Perform HTTP probe to measure actual latency (TTFB).
	if _, err = httpProbe(conn, destination.AddrString(), path, requestTimeout); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// probeMember only runs generate_204. Manager independently refreshes missing
// exit metadata after committing health, outside the shared health cache.
func (p *poolOutbound) probeMember(ctx context.Context, member *memberState, destination M.Socksaddr, host, path string, useTLS, tlsInsecure bool, report func(monitor.ProbeResult)) (monitor.ProbeResult, error) {
	attemptTimeout := p.monitor.ProbeAttemptTimeout()
	result := monitor.ProbeResult{}

	requestCtx, requestCancel := context.WithTimeout(ctx, time.Duration(probeConnectivityAttempts)*attemptTimeout)
	duration, connectivityErr := p.probeConnectivity(requestCtx, member, destination, host, path, useTLS, tlsInsecure, attemptTimeout)
	requestCancel()
	if connectivityErr != nil {
		result.ConnectivityError = connectivityErr.Error()
	} else {
		result.ConnectivityOK = true
		result.Latency = duration
	}
	if report != nil {
		report(result)
	}
	if connectivityErr != nil {
		// Trace 走的是同一条链路，连通性都不通就没有必要再花一次请求。
		return result, fmt.Errorf("generate_204 探测失败: %w", connectivityErr)
	}

	return result, nil
}

// dialProbeTarget retries a transport handshake once when the remote closes it
// without a protocol response. This is common during transient Trojan TLS
// negotiation and should not immediately blacklist an otherwise healthy node.
func dialProbeTarget(ctx context.Context, member *memberState, destination M.Socksaddr) (net.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err == nil {
			return conn, nil
		}
		if conn != nil {
			_ = conn.Close()
		}
		lastErr = err
		if !isRetryableProbeHandshakeError(err) {
			return nil, err
		}
		if attempt == 1 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("重试后代理握手仍失败: %w", lastErr)
}

func isRetryableProbeHandshakeError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "connection reset") || strings.Contains(s, "reset by peer")
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context, report func(monitor.ProbeResult)) (monitor.ProbeResult, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context, report func(monitor.ProbeResult)) (monitor.ProbeResult, error) {
		destination, host, path, useTLS, tlsInsecure, ok := p.monitor.DestinationForProbe()
		if !ok {
			return monitor.ProbeResult{}, E.New("未配置探测目标")
		}
		return p.probeMember(ctx, member, destination, host, path, useTLS, tlsInsecure, report)
	}
}

// makeProbeByTagFunc creates a probe function that works before member initialization
func (p *poolOutbound) makeProbeByTagFunc(tag string) func(ctx context.Context, report func(monitor.ProbeResult)) (monitor.ProbeResult, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context, report func(monitor.ProbeResult)) (monitor.ProbeResult, error) {
		destination, host, path, useTLS, tlsInsecure, ok := p.monitor.DestinationForProbe()
		if !ok {
			return monitor.ProbeResult{}, E.New("未配置探测目标")
		}
		member, err := p.memberForTag(tag)
		if err != nil {
			return monitor.ProbeResult{}, err
		}
		return p.probeMember(ctx, member, destination, host, path, useTLS, tlsInsecure, report)
	}
}

// makeTraceFunc builds the on-demand exit-location probe for one member. It runs
// Trace alone: no generate_204 request, and no effect on health or latency.
func (p *poolOutbound) makeTraceFunc(member *memberState) func(ctx context.Context) (monitor.ProbeResult, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context) (monitor.ProbeResult, error) {
		_, _, _, _, tlsInsecure, ok := p.monitor.DestinationForProbe()
		if !ok {
			return monitor.ProbeResult{}, E.New("未配置探测目标")
		}
		attemptTimeout := p.monitor.ProbeAttemptTimeout()
		locationCtx, cancel := context.WithTimeout(ctx, time.Duration(probeTraceMaxAttempts)*attemptTimeout)
		defer cancel()
		return p.probeLocation(locationCtx, member, tlsInsecure, attemptTimeout)
	}
}

// makeTraceByTagFunc is makeTraceFunc for callers that only know the tag.
func (p *poolOutbound) makeTraceByTagFunc(tag string) func(ctx context.Context) (monitor.ProbeResult, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context) (monitor.ProbeResult, error) {
		_, _, _, _, tlsInsecure, ok := p.monitor.DestinationForProbe()
		if !ok {
			return monitor.ProbeResult{}, E.New("未配置探测目标")
		}
		member, err := p.memberForTag(tag)
		if err != nil {
			return monitor.ProbeResult{}, err
		}
		attemptTimeout := p.monitor.ProbeAttemptTimeout()
		locationCtx, cancel := context.WithTimeout(ctx, time.Duration(probeTraceMaxAttempts)*attemptTimeout)
		defer cancel()
		return p.probeLocation(locationCtx, member, tlsInsecure, attemptTimeout)
	}
}

// memberForTag resolves a pool member, initializing the member list on demand so
// probe functions registered before Start still work.
func (p *poolOutbound) memberForTag(tag string) (*memberState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			return nil, err
		}
	}
	for _, m := range p.members {
		if m.tag == tag {
			return m, nil
		}
	}
	return nil, E.New("节点池成员不存在: ", tag)
}

// makeReleaseByTagFunc creates a release function that works before member initialization
func (p *poolOutbound) makeReleaseByTagFunc(tag string) func() {
	return func() {
		p.sharedStates.release(tag)
	}
}

func (p *poolOutbound) makeReleaseProbeFunc(tag string) func() {
	return func() {
		if p.monitor != nil {
			p.monitor.ProbeAfterRelease(tag)
		}
	}
}

// makeBlacklistByTagFunc creates a blacklist function for manual ban via API
func (p *poolOutbound) makeBlacklistByTagFunc(tag string) func(time.Duration) {
	return func(duration time.Duration) {
		p.sharedStates.blacklist(tag, duration)
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (p *poolOutbound) getCandidateBuffer() []*memberState {
	if buf := p.candidatesPool.Get(); buf != nil {
		return buf.([]*memberState)
	}
	return make([]*memberState, 0, len(p.options.Members))
}

func (p *poolOutbound) putCandidateBuffer(buf []*memberState) {
	if buf == nil {
		return
	}
	const maxCached = 4096
	if cap(buf) > maxCached {
		return
	}
	p.candidatesPool.Put(buf[:0])
}
