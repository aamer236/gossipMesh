package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type AdaptiveConfig struct {
	Enabled               bool
	StableAfter           time.Duration
	MaxStabilizeInterval  time.Duration
	MaxFixFingersInterval time.Duration
	MaxCheckPredInterval  time.Duration
}

var maintenanceNames = [3]string{"stabilize", "fix_fingers", "check_predecessor"}

// MaintenancePolicy has no Chord or membership dependencies. Callers collect
// detached observations before acquiring its mutex; no RPC runs under this lock.
// Quiet local state is a heuristic, never a proof of global ring convergence.
type MaintenancePolicy struct {
	lastObserved              time.Time
	mu                        sync.Mutex
	enabled                   bool
	stableAfter               time.Duration
	base, caps                [3]time.Duration
	quietSince                time.Time
	fingerprint               string
	observed                  bool
	blockedReason, lastReason string
	wake                      [3]chan struct{}
	telemetry                 *Telemetry
	lastMode                  string
}

func NewMaintenancePolicy(cfg *Config, t *Telemetry, now time.Time) *MaintenancePolicy {
	p := &MaintenancePolicy{enabled: cfg.Adaptive.Enabled, stableAfter: cfg.Adaptive.StableAfter,
		base:       [3]time.Duration{cfg.StabilizeInterval, cfg.FixFingersInterval, cfg.CheckPredInterval},
		caps:       [3]time.Duration{cfg.Adaptive.MaxStabilizeInterval, cfg.Adaptive.MaxFixFingersInterval, cfg.Adaptive.MaxCheckPredInterval},
		quietSince: now, lastReason: "startup", blockedReason: "awaiting_observation", telemetry: t, lastMode: "fast"}
	for i := range p.wake {
		p.wake[i] = make(chan struct{}, 1)
		if p.caps[i] < p.base[i] {
			p.caps[i] = p.base[i]
		}
	}
	return p
}
func (p *MaintenancePolicy) signal() {
	for _, wake := range p.wake {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
func (p *MaintenancePolicy) Activity(now time.Time, reason string) {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	if now.Before(p.quietSince) {
		p.mu.Unlock()
		return
	}
	if now.Before(p.lastObserved) {
		now = p.lastObserved
	}
	p.quietSince = now
	p.lastReason = reason
	if p.lastMode != "fast" {
		p.lastMode = "fast"
		p.telemetry.Emit("maintenance_fast", "", map[string]interface{}{"reason": reason})
	}
	p.mu.Unlock()
	p.signal()
}
func routingFingerprint(s RoutingSnapshot) string {
	var out strings.Builder
	add := func(p *NodeRef) {
		if p == nil || p.ID == nil {
			out.WriteString("nil;")
			return
		}
		fmt.Fprintf(&out, "%s@%s;", p.ID, p.Addr)
	}
	add(s.Successor)
	add(s.Predecessor)
	out.WriteByte('|')
	for _, p := range s.Successors {
		add(p)
	}
	out.WriteByte('|')
	for _, p := range s.Fingers {
		add(p)
	}
	return out.String()
}
func (p *MaintenancePolicy) Observe(now time.Time, snapshot RoutingSnapshot, blocker string) {
	if !p.enabled {
		return
	}
	fingerprint := routingFingerprint(snapshot)
	p.mu.Lock()
	if now.Before(p.lastObserved) || now.Before(p.quietSince) {
		p.mu.Unlock()
		return
	}
	p.lastObserved = now
	changed := !p.observed || fingerprint != p.fingerprint
	p.observed = true
	p.fingerprint = fingerprint
	blockerChanged := p.blockedReason != blocker
	p.blockedReason = blocker
	if changed || blockerChanged || blocker != "" {
		p.quietSince = now
		p.lastReason = blocker
		if changed {
			p.lastReason = "routing_changed"
		}
	}
	intervals := p.intervalsLocked(now)
	mode := "fast"
	for i, d := range intervals {
		if d > p.base[i] {
			mode = "backoff"
		}
	}
	if mode != p.lastMode {
		p.lastMode = mode
		p.telemetry.Emit("maintenance_"+mode, "", map[string]interface{}{"reason": p.lastReason, "intervals_ms": intervalMap(intervals)})
	}
	p.mu.Unlock()
	// An already scheduled slow timer must notice every new blocker or topology
	// change; signals coalesce and never launch extra workers.
	if changed || blockerChanged {
		p.signal()
	}
}
func (p *MaintenancePolicy) intervalsLocked(now time.Time) [3]time.Duration {
	result := p.base
	if !p.enabled || !p.observed || p.blockedReason != "" || p.stableAfter <= 0 {
		return result
	}
	rounds := now.Sub(p.quietSince) / p.stableAfter
	if rounds > 30 {
		rounds = 30
	}
	for i := range result {
		for round := time.Duration(0); round < rounds && result[i] < p.caps[i]; round++ {
			result[i] *= 2
			if result[i] > p.caps[i] {
				result[i] = p.caps[i]
			}
		}
	}
	return result
}
func intervalMap(intervals [3]time.Duration) map[string]int64 {
	out := make(map[string]int64, 3)
	for i, d := range intervals {
		out[maintenanceNames[i]] = d.Milliseconds()
	}
	return out
}
func (p *MaintenancePolicy) Interval(index int, now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.intervalsLocked(now)[index]
}
func (p *MaintenancePolicy) Metrics() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	intervals := p.intervalsLocked(now)
	mode := "fast"
	for i, d := range intervals {
		if d > p.base[i] {
			mode = "backoff"
		}
	}
	return map[string]interface{}{"enabled": p.enabled, "mode": mode, "intervals_ms": intervalMap(intervals), "quiet_ms": now.Sub(p.quietSince).Milliseconds(), "last_reason": p.lastReason, "blocked_reason": p.blockedReason}
}

func maintenanceBlocker(node *ChordNode, table *Membership, coordinator *Coordinator, snapshot RoutingSnapshot) string {
	node.mu.RLock()
	dirty := len(node.dirty)
	node.mu.RUnlock()
	if dirty > 0 {
		return "dirty_references"
	}
	coordinator.mu.Lock()
	pending := len(coordinator.pending)
	coordinator.mu.Unlock()
	if pending > 0 {
		return "pending_repair"
	}
	table.mu.Lock()
	suspect := false
	for _, member := range table.members {
		if member.State == Suspect {
			suspect = true
			break
		}
	}
	table.mu.Unlock()
	if suspect {
		return "suspected_peer"
	}
	if snapshot.Successor == nil || len(snapshot.Successors) == 0 || !samePeer(snapshot.Successor, snapshot.Successors[0]) {
		return "incomplete_successor"
	}
	if snapshot.Successor.Addr != node.Addr && snapshot.Predecessor == nil {
		return "missing_predecessor"
	}
	refs := append([]*NodeRef{snapshot.Successor}, snapshot.Successors...)
	if snapshot.Predecessor != nil {
		refs = append(refs, snapshot.Predecessor)
	}
	refs = append(refs, snapshot.Fingers...)
	for _, ref := range refs {
		if node.validateRef(ref) != nil {
			return "incomplete_or_dead_reference"
		}
	}
	return ""
}
func (p *MaintenancePolicy) RunTask(ctx context.Context, index int, operation, observe func()) {
	if !p.enabled {
		ticker := time.NewTicker(p.base[index])
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				p.telemetry.Add("maintenance_"+maintenanceNames[index]+"_runs", 1)
				operation()
			}
		}
	}
	lastCompleted := time.Now()
	for {
		delay := time.Until(lastCompleted.Add(p.Interval(index, time.Now())))
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-p.wake[index]:
			timer.Stop()
			continue
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		p.telemetry.Add("maintenance_"+maintenanceNames[index]+"_runs", 1)
		operation()
		lastCompleted = time.Now()
		if observe != nil {
			observe()
		}
	}
}
