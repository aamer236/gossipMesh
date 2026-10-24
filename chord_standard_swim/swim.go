package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const wireVersion = 1
const maxSWIMBody = 8 << 20

type SWIMMessage struct {
	Version int      `json:"version"`
	M       int      `json:"m"`
	Type    string   `json:"type"`
	ProbeID string   `json:"probe_id"`
	Origin  *NodeRef `json:"origin"`
	From    Update   `json:"from"`
	Target  *Update  `json:"target,omitempty"`
	Witness *Update  `json:"witness,omitempty"`
	Updates []Update `json:"updates,omitempty"`
	View    []Update `json:"view,omitempty"`
}
type SWIMTransport interface {
	Exchange(context.Context, string, SWIMMessage) (SWIMMessage, error)
}
type SWIM struct {
	table     *Membership
	transport SWIMTransport
	telemetry *Telemetry
	lifecycle sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	handlers  chan struct{}
	closed    bool
}

func NewSWIM(table *Membership, transport SWIMTransport, t *Telemetry) *SWIM {
	return &SWIM{table: table, transport: transport, telemetry: t, handlers: make(chan struct{}, 32)}
}
func (s *SWIM) message(kind, id string, origin *NodeRef, target *Update) SWIMMessage {
	return SWIMMessage{Version: wireVersion, M: s.table.m, Type: kind, ProbeID: id, Origin: clonePeer(origin), From: s.table.Self().Update, Target: target, Updates: s.table.Piggyback()}
}
func (s *SWIM) nextID() string {
	s.table.mu.Lock()
	defer s.table.mu.Unlock()
	s.table.sequence++
	self := s.table.members[s.table.self.ID.String()]
	return fmt.Sprintf("%s/%d/%d", s.table.self.ID, self.Incarnation, s.table.sequence)
}
func samePeer(a, b *NodeRef) bool {
	return a != nil && b != nil && a.ID != nil && b.ID != nil && a.ID.Cmp(b.ID) == 0 && a.Addr == b.Addr
}
func (s *SWIM) validateMessage(msg SWIMMessage) error {
	if msg.Version != wireVersion || msg.M != s.table.m || msg.ProbeID == "" || len(msg.ProbeID) > 256 {
		return fmt.Errorf("invalid SWIM version, space or probe ID")
	}
	if msg.From.State != Alive {
		return fmt.Errorf("sender must advertise ALIVE")
	}
	if err := s.table.Validate(msg.From); err != nil {
		return err
	}
	if len(msg.View) > 0 && msg.Type != "JOIN_ACK" {
		return fmt.Errorf("full membership view is only valid in JOIN_ACK")
	}
	if msg.Origin == nil || !validID(msg.Origin.ID, s.table.m) {
		return fmt.Errorf("invalid probe origin")
	}
	if err := s.table.Validate(Update{Peer: msg.Origin, State: Alive}); err != nil {
		return err
	}
	if len(msg.Updates) > s.table.cfg.PiggybackLimit || len(msg.View) > s.table.cfg.MaxMembers {
		return fmt.Errorf("membership payload exceeds configured bound")
	}
	for _, u := range msg.Updates {
		if err := s.table.Validate(u); err != nil {
			return err
		}
	}
	for _, u := range msg.View {
		if err := s.table.Validate(u); err != nil {
			return err
		}
	}
	return nil
}
func (s *SWIM) accept(msg SWIMMessage) error {
	if _, err := s.table.Merge(msg.From); err != nil {
		return err
	}
	for _, u := range msg.Updates {
		if _, err := s.table.Merge(u); err != nil {
			return err
		}
	}
	return nil
}
func (s *SWIM) call(ctx context.Context, addr string, request SWIMMessage, expected *NodeRef, replyType string) (SWIMMessage, error) {
	reply, err := s.transport.Exchange(ctx, addr, request)
	if err != nil {
		return SWIMMessage{}, err
	}
	if err = s.validateMessage(reply); err != nil {
		return SWIMMessage{}, err
	}
	if reply.Type != replyType || reply.ProbeID != request.ProbeID || !samePeer(reply.Origin, request.Origin) || reply.From.Peer.Addr != addr || (expected != nil && !samePeer(reply.From.Peer, expected)) {
		return SWIMMessage{}, fmt.Errorf("uncorrelated ACK or sender identity mismatch")
	}
	if replyType == "ACK" {
		if reply.Witness == nil || reply.Witness.State != Alive || request.Target == nil || !samePeer(reply.Witness.Peer, request.Target.Peer) || reply.Witness.Incarnation < request.Target.Incarnation {
			return SWIMMessage{}, fmt.Errorf("ACK target identity/version mismatch")
		}
		if err = s.table.Validate(*reply.Witness); err != nil {
			return SWIMMessage{}, err
		}
	}
	if err = s.accept(reply); err != nil {
		return SWIMMessage{}, err
	}
	return reply, nil
}
func (s *SWIM) Probe(ctx context.Context, target Member) bool {
	if target.Peer == nil || samePeer(target.Peer, s.table.self) || target.State == Dead {
		return false
	}
	id := s.nextID()
	key := target.Peer.ID.String()
	s.telemetry.Emit("probe_started", key, map[string]interface{}{"probe_id": id})
	directCtx, cancel := context.WithTimeout(ctx, s.table.cfg.DirectTimeout)
	req := s.message("PING", id, s.table.self, &target.Update)
	reply, err := s.call(directCtx, target.Peer.Addr, req, target.Peer, "ACK")
	cancel()
	s.telemetry.Add("direct_probes", 1)
	if err == nil {
		s.table.Acknowledge(*reply.Witness)
		s.telemetry.Emit("direct_ack", key, map[string]interface{}{"probe_id": id})
		s.telemetry.Add("probe_successes", 1)
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	kind := "direct_error"
	var networkErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkErr) && networkErr.Timeout()) {
		kind = "direct_timeout"
		s.telemetry.Add("probe_timeouts", 1)
	}
	s.telemetry.Add("direct_failures", 1)
	s.telemetry.Emit(kind, key, map[string]interface{}{"probe_id": id, "error": err.Error()})
	helpers := s.table.Helpers(key)
	indirectCtx, indirectCancel := context.WithTimeout(ctx, 2*s.table.cfg.DirectTimeout)
	results := make(chan bool, len(helpers))
	var helpersWG sync.WaitGroup
	for _, helper := range helpers {
		helpersWG.Add(1)
		go func(helper Member) {
			defer helpersWG.Done()
			s.telemetry.Add("indirect_probes", 1)
			s.telemetry.Emit("ping_req_issued", key, map[string]interface{}{"helper": peerID(helper.Peer), "probe_id": id})
			request := s.message("PING_REQ", id, s.table.self, &target.Update)
			ack, e := s.call(indirectCtx, helper.Peer.Addr, request, helper.Peer, "ACK")
			if e == nil {
				s.table.Acknowledge(*ack.Witness)
				s.telemetry.Emit("indirect_ack", key, map[string]interface{}{"helper": peerID(helper.Peer), "probe_id": id})
				results <- true
			} else {
				kind := "indirect_error"
				var networkErr net.Error
				if errors.Is(e, context.Canceled) {
					kind = "indirect_cancelled"
				} else if errors.Is(e, context.DeadlineExceeded) || (errors.As(e, &networkErr) && networkErr.Timeout()) {
					kind = "indirect_timeout"
					s.telemetry.Add("indirect_timeouts", 1)
				}
				s.telemetry.Emit(kind, key, map[string]interface{}{"helper": peerID(helper.Peer), "probe_id": id, "error": e.Error()})
				results <- false
			}
		}(helper)
	}
	success := false
	for range helpers {
		if <-results {
			success = true
			indirectCancel()
		}
	}
	indirectCancel()
	helpersWG.Wait()
	if success {
		s.telemetry.Add("probe_successes", 1)
		return true
	}
	if ctx.Err() == nil {
		s.table.SuspectAfterFailure(target)
	}
	return false
}

// Receive has no global lock across network operations. PING_REQ handlers are
// bounded separately by the HTTP handler semaphore and direct-probe deadline.
func (s *SWIM) Receive(ctx context.Context, msg SWIMMessage) (SWIMMessage, error) {
	if err := s.validateMessage(msg); err != nil {
		return SWIMMessage{}, err
	}
	if msg.Type != "PING" && msg.Type != "PING_REQ" && msg.Type != "JOIN" {
		return SWIMMessage{}, fmt.Errorf("unsupported SWIM message type")
	}
	if (msg.Type == "JOIN" || msg.Type == "PING_REQ") && !samePeer(msg.Origin, msg.From.Peer) {
		return SWIMMessage{}, fmt.Errorf("join origin must be sender")
	}
	if msg.Type != "JOIN" {
		if msg.Target == nil {
			return SWIMMessage{}, fmt.Errorf("missing probe target")
		}
		if err := s.table.Validate(*msg.Target); err != nil {
			return SWIMMessage{}, err
		}
		if msg.Type == "PING" && !samePeer(msg.Target.Peer, s.table.self) {
			return SWIMMessage{}, fmt.Errorf("PING delivered to incorrect target")
		}
		if msg.Type == "PING_REQ" && samePeer(msg.Target.Peer, s.table.self) {
			return SWIMMessage{}, fmt.Errorf("helper cannot be the target")
		}
	}
	if err := s.accept(msg); err != nil {
		return SWIMMessage{}, err
	}
	switch msg.Type {
	case "JOIN":
		reply := s.message("JOIN_ACK", msg.ProbeID, msg.Origin, nil)
		for _, member := range s.table.Snapshot() {
			reply.View = append(reply.View, member.Update)
		}
		return reply, nil
	case "PING":
		// A suspect target learns the exact incarnation being probed even if its
		// suspicion update has already retired from the gossip buffer.
		if _, err := s.table.Merge(*msg.Target); err != nil {
			return SWIMMessage{}, err
		}
		self := s.table.Self().Update
		reply := s.message("ACK", msg.ProbeID, msg.Origin, msg.Target)
		reply.Witness = &self
		return reply, nil
	case "PING_REQ":
		target := *msg.Target
		if latest, ok := s.table.Get(peerID(target.Peer)); ok && newer(latest.Update, target) {
			target = latest.Update
		}
		probeCtx, cancel := context.WithTimeout(ctx, s.table.cfg.DirectTimeout)
		defer cancel()
		ping := s.message("PING", msg.ProbeID, msg.Origin, &target)
		ack, err := s.call(probeCtx, target.Peer.Addr, ping, target.Peer, "ACK")
		if err != nil {
			return SWIMMessage{}, fmt.Errorf("helper target probe: %w", err)
		}
		s.table.Acknowledge(*ack.Witness)
		reply := s.message("ACK", msg.ProbeID, msg.Origin, &target)
		reply.Witness = ack.Witness
		return reply, nil
	}
	return SWIMMessage{}, fmt.Errorf("unreachable SWIM type")
}
func (s *SWIM) Bootstrap(ctx context.Context, addr string) error {
	if addr == "" || addr == s.table.self.Addr {
		return nil
	}
	// The first exchange reconciles the local incarnation with any bootstrap
	// tombstone. The second advertises the resulting durable, newer ALIVE.
	for pass := 0; pass < 2; pass++ {
		request := s.message("JOIN", s.nextID(), s.table.self, nil)
		attempt, cancel := context.WithTimeout(ctx, 3*s.table.cfg.Interval)
		reply, err := s.call(attempt, addr, request, nil, "JOIN_ACK")
		cancel()
		if err != nil {
			return err
		}
		for _, u := range reply.View {
			if _, err = s.table.Merge(u); err != nil {
				return err
			}
		}
	}
	s.telemetry.Emit("node_joined", s.table.self.ID.String(), map[string]interface{}{"bootstrap": addr, "incarnation": s.table.Self().Incarnation})
	return nil
}
func (s *SWIM) Start(parent context.Context) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.cancel != nil || s.closed {
		return fmt.Errorf("SWIM already started or stopped")
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.table.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if target, ok := s.table.NextTarget(); ok {
					s.Probe(ctx, target)
				}
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		interval := s.table.cfg.Interval / 4
		if interval > 50*time.Millisecond {
			interval = 50 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.table.Expire(s.table.now())
			}
		}
	}()
	return nil
}
func (s *SWIM) Stop() {
	s.lifecycle.Lock()
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	s.lifecycle.Unlock()
	s.wg.Wait()
}
func (s *SWIM) Metrics() map[string]interface{} {
	members := s.table.Snapshot()
	active, suspected, dead := 0, 0, 0
	for _, member := range members {
		switch member.State {
		case Alive:
			active++
		case Suspect:
			suspected++
		case Dead:
			dead++
		}
	}
	s.table.mu.Lock()
	buffered := len(s.table.buffer)
	s.table.mu.Unlock()
	return map[string]interface{}{"counters": s.telemetry.Snapshot(), "membership_records": len(members), "alive_records": active, "suspect_records": suspected, "dead_records": dead, "buffered_updates": buffered, "self_incarnation": s.table.Self().Incarnation}
}
