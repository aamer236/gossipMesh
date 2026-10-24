package main

import (
	"context"
	"sync"
	"time"
)

type repairJob struct {
	Member        Member
	Kind          string
	Priority      int
	Attempt       int
	Due           time.Time
	Sequence      uint64
	QueuedAt      time.Time
	RepairedTotal int
}
type Coordinator struct {
	mu        sync.Mutex
	node      *ChordNode
	telemetry *Telemetry
	latest    map[string]Update
	pending   map[string]repairJob
	wake      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	sequence  uint64
	completed uint64
	failures  uint64
	latencies []float64
	capacity  int
	started   bool
	closed    bool
}

func NewCoordinator(parent context.Context, node *ChordNode, t *Telemetry, capacity int) *Coordinator {
	ctx, cancel := context.WithCancel(parent)
	return &Coordinator{node: node, telemetry: t, latest: make(map[string]Update), pending: make(map[string]repairJob), wake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, capacity: capacity}
}

// Submit is idempotent and coalesces by peer. No worker network call or SWIM
// callback holds this lock. Version checks also handle out-of-order callbacks.
func (c *Coordinator) Submit(event MembershipEvent) {
	member := event.Member
	key := peerID(member.Peer)
	if key == c.node.ID.String() {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if old, ok := c.latest[key]; ok && !newer(member.Update, old) {
		c.mu.Unlock()
		return
	}
	if len(c.latest) >= c.capacity {
		if _, exists := c.latest[key]; !exists {
			c.mu.Unlock()
			c.telemetry.Emit("error", key, map[string]interface{}{"operation": "repair_queue", "error": "capacity reached"})
			return
		}
	}
	c.latest[key] = cloneUpdate(member.Update)
	// Serialize hint publication with latest-version acceptance so a delayed
	// callback cannot re-install an older hint after a newer event.
	c.node.SetPeerMembershipHint(member)
	var refs AffectedReferences
	if member.State == Dead {
		refs = c.node.MarkReferencesDirty(member.Peer)
	} else {
		refs = affected(c.node.GetRoutingSnapshot(), member.Peer)
	}
	priority := 3
	if member.State == Suspect {
		priority = 2
	}
	if member.State == Dead {
		priority = 1
	}
	if member.State == Dead && refs.Successor {
		priority = 0
	}
	c.sequence++
	c.pending[key] = repairJob{Member: cloneMember(member), Kind: event.Kind, Priority: priority, Due: time.Now(), Sequence: c.sequence, QueuedAt: time.Now()}
	c.mu.Unlock()
	c.telemetry.Emit("membership_event_handled", key, map[string]interface{}{"state": member.State, "incarnation": member.Incarnation, "event": event.Kind})
	c.telemetry.Emit("references_identified", key, map[string]interface{}{"affected": refs, "state": member.State})
	if member.State == Dead {
		c.telemetry.Add("references_invalidated", uint64(refs.Count()))
		for _, index := range refs.Fingers {
			c.telemetry.Emit("finger_dirty", key, map[string]interface{}{"index": index})
		}
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
func (c *Coordinator) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.closed {
		return
	}
	c.started = true
	c.wg.Add(1)
	go c.run()
}
func (c *Coordinator) Stop() { c.mu.Lock(); c.closed = true; c.cancel(); c.mu.Unlock(); c.wg.Wait() }
func (c *Coordinator) next() (string, repairJob, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var key string
	var job repairJob
	for id, candidate := range c.pending {
		if time.Now().Before(candidate.Due) {
			continue
		}
		if key == "" || candidate.Priority < job.Priority || (candidate.Priority == job.Priority && candidate.Sequence < job.Sequence) {
			key = id
			job = candidate
		}
	}
	return key, job, key != ""
}
func (c *Coordinator) run() {
	defer c.wg.Done()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		case <-c.wake:
		}
		for {
			if c.ctx.Err() != nil {
				return
			}
			key, job, ok := c.next()
			if !ok {
				break
			}
			started := time.Now()
			result := RepairResult{}
			if job.Member.State == Suspect {
				// Suspicion is observational. A Chord probe may validate a reference, but
				// only SWIM's version rules can clear membership suspicion.
				err := c.node.transport.PingRPC(job.Member.Peer.Addr)
				c.telemetry.Emit("suspect_reference_validation", key, map[string]interface{}{"reachable": err == nil})
			} else {
				result = c.node.RepairPeerReferences(job.Member.Peer, job.Member.Incarnation, job.Member.State, 4, c.telemetry)
			}
			attemptLatency := float64(time.Since(started).Microseconds()) / 1000
			latency := float64(time.Since(job.QueuedAt).Microseconds()) / 1000
			job.RepairedTotal += result.Repaired
			c.mu.Lock()
			current, exists := c.pending[key]
			if exists && current.Sequence == job.Sequence {
				if (len(result.Errors) > 0 && job.Attempt < 8) || (result.Pending && len(result.Errors) == 0) {
					if len(result.Errors) > 0 {
						job.Attempt++
					}
					c.sequence++
					job.Sequence = c.sequence
					job.Due = time.Now().Add(time.Duration(job.Attempt) * 25 * time.Millisecond)
					c.pending[key] = job
				} else {
					delete(c.pending, key)
					if len(result.Errors) > 0 || result.Pending {
						c.failures++
					} else {
						c.completed++
						if job.RepairedTotal > 0 && job.Member.State != Suspect {
							c.latencies = append(c.latencies, latency)
						}
					}
				}
			}
			if len(c.latencies) > 1024 {
				c.latencies = append([]float64(nil), c.latencies[len(c.latencies)-512:]...)
			}
			c.mu.Unlock()
			c.telemetry.Add("references_repaired", uint64(result.Repaired))
			kind := "repair_completed"
			if result.Repaired == 0 && !result.Pending && len(result.Errors) == 0 {
				kind = "repair_noop"
			}
			if len(result.Errors) > 0 || result.Pending {
				kind = "repair_pending"
			}
			if job.Member.State == Suspect {
				kind = "suspect_validation_completed"
			}
			c.telemetry.Emit(kind, key, map[string]interface{}{"state": job.Member.State, "incarnation": job.Member.Incarnation, "result": result, "latency_ms": latency, "attempt_ms": attemptLatency})
		}
	}
}
func (c *Coordinator) Metrics() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	latencies := append([]float64(nil), c.latencies...)
	p50, p95, p99 := calculatePercentiles(latencies)
	return map[string]interface{}{"pending_peers": len(c.pending), "completed_jobs": c.completed, "failed_jobs": c.failures, "repair_latency_p50_ms": p50, "repair_latency_p95_ms": p95, "repair_latency_p99_ms": p99}
}
