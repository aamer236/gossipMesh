package main

import (
	"fmt"
	"sort"
)

// Writers acquire mu before hintMu, so membership versions and routing writes
// share a linearization boundary. Filter readers may take hintMu alone. No
// operation acquires mu while holding hintMu, and no RPC runs under either lock.
type RoutingHint struct {
	Peer        *NodeRef
	State       MemberState
	Incarnation uint64
}
type RoutingSnapshot struct {
	Successor, Predecessor *NodeRef
	Successors, Fingers    []*NodeRef
}
type AffectedReferences struct {
	Successor     bool  `json:"successor"`
	Predecessor   bool  `json:"predecessor"`
	SuccessorList int   `json:"successor_list_entries"`
	Fingers       []int `json:"fingers"`
}

func (a AffectedReferences) Count() int {
	n := a.SuccessorList + len(a.Fingers)
	if a.Successor {
		n++
	}
	if a.Predecessor {
		n++
	}
	return n
}
func (n *ChordNode) GetRoutingSnapshot() RoutingSnapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := RoutingSnapshot{Successor: clonePeer(n.Successor), Predecessor: clonePeer(n.Predecessor)}
	for _, ref := range n.SuccessorList {
		out.Successors = append(out.Successors, clonePeer(ref))
	}
	for _, ref := range n.FingerTable {
		out.Fingers = append(out.Fingers, clonePeer(ref))
	}
	return out
}
func (n *ChordNode) SetPeerMembershipHint(member Member) bool {
	if samePeer(member.Peer, &NodeRef{ID: n.ID, Addr: n.Addr}) {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	if n.peerHints == nil {
		n.peerHints = make(map[string]RoutingHint)
	}
	key := member.Peer.Addr
	old, ok := n.peerHints[key]
	if ok && !newer(member.Update, Update{Peer: old.Peer, State: old.State, Incarnation: old.Incarnation}) {
		return false
	}
	n.peerHints[key] = RoutingHint{Peer: clonePeer(member.Peer), State: member.State, Incarnation: member.Incarnation}
	return true
}
func (n *ChordNode) peerBlocked(addr string) bool {
	n.hintMu.RLock()
	defer n.hintMu.RUnlock()
	return n.peerHints[addr].State == Dead
}
func (n *ChordNode) hintCurrent(peer *NodeRef, inc uint64, state MemberState) bool {
	n.hintMu.RLock()
	defer n.hintMu.RUnlock()
	hint, ok := n.peerHints[peer.Addr]
	return ok && samePeer(hint.Peer, peer) && hint.Incarnation == inc && hint.State == state
}
func affected(snapshot RoutingSnapshot, peer *NodeRef) AffectedReferences {
	a := AffectedReferences{Successor: samePeer(snapshot.Successor, peer), Predecessor: samePeer(snapshot.Predecessor, peer), Fingers: []int{}}
	for _, ref := range snapshot.Successors {
		if samePeer(ref, peer) {
			a.SuccessorList++
		}
	}
	for i, ref := range snapshot.Fingers {
		if samePeer(ref, peer) {
			a.Fingers = append(a.Fingers, i)
		}
	}
	return a
}
func (n *ChordNode) MarkReferencesDirty(peer *NodeRef) AffectedReferences {
	snapshot := n.GetRoutingSnapshot()
	a := affected(snapshot, peer)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dirty == nil {
		n.dirty = make(map[int]string)
	}
	for _, index := range a.Fingers {
		if samePeer(n.FingerTable[index], peer) {
			n.dirty[index] = peer.ID.String()
		}
	}
	return a
}

func (n *ChordNode) hasDirtyReferences(key string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, owner := range n.dirty {
		if owner == key {
			return true
		}
	}
	return false
}

type RepairResult struct {
	Repaired          int      `json:"repaired"`
	Pending           bool     `json:"pending"`
	Errors            []string `json:"errors,omitempty"`
	SuccessorPromoted bool     `json:"successor_promoted"`
	BackupsValidated  bool     `json:"backups_validated"`
}

// Recompute only affected fingers. A small batch yields between peers, while a
// confirmed-dead successor is always repaired before any fingers. Membership
// supplies no candidate owner; all candidates come from local Chord references.
func (n *ChordNode) RepairPeerReferences(peer *NodeRef, inc uint64, state MemberState, batch int, t *Telemetry, refreshRequested bool) RepairResult {
	result := RepairResult{}
	if !n.hintCurrent(peer, inc, state) {
		result.Errors = append(result.Errors, "membership event superseded")
		return result
	}
	before := n.GetRoutingSnapshot()
	refs := affected(before, peer)
	refreshBackups := state == Dead && (refs.Successor || refs.SuccessorList > 0 || before.Successor == nil || refreshRequested)
	if state == Dead {
		if samePeer(before.Successor, peer) || before.Successor == nil {
			n.mu.RLock()
			expected := n.Successor
			n.mu.RUnlock()
			if expected == nil || samePeer(expected, peer) {
				n.handleSuccessorFailureIf(expected, func() bool { return n.hintCurrent(peer, inc, state) })
			}
			after := n.GetRoutingSnapshot()
			if after.Successor != nil && !samePeer(after.Successor, peer) && n.transport.PingRPC(after.Successor.Addr) == nil {
				result.SuccessorPromoted = true
				result.Repaired++
				t.Emit("successor_repair_validated", peerID(peer), map[string]interface{}{"successor": peerID(after.Successor), "address": after.Successor.Addr})
			} else {
				result.Errors = append(result.Errors, "no validated successor candidate")
			}
		}
		n.mu.Lock()
		if n.hintCurrent(peer, inc, state) {
			if samePeer(n.Predecessor, peer) {
				n.Predecessor = nil
				result.Repaired++
				t.Emit("predecessor_cleared", peerID(peer), nil)
			}
			list := make([]*NodeRef, 0, len(n.SuccessorList))
			for _, ref := range n.SuccessorList {
				if !samePeer(ref, peer) {
					list = append(list, ref)
				}
			}
			n.SuccessorList = list
		}
		n.mu.Unlock()
	}
	// Refresh the chosen successor's own backups through ordinary stabilization.
	if refreshBackups {
		n.Stabilize()
	}
	if refreshBackups {
		snapshot := n.GetRoutingSnapshot()
		valid := snapshot.Successor != nil && len(snapshot.Successors) > 0 && samePeer(snapshot.Successor, snapshot.Successors[0])
		for i, ref := range snapshot.Successors {
			if n.validateRef(ref) != nil || n.transport.PingRPC(ref.Addr) != nil {
				valid = false
				break
			}
			if i > 0 && !InInterval(ref.ID, snapshot.Successors[i-1].ID, n.ID, false, false, n.M) {
				valid = false
				break
			}
		}
		if valid {
			result.BackupsValidated = true
			t.Emit("successor_list_refreshed", peerID(peer), map[string]interface{}{"entries": len(snapshot.Successors), "criterion": "clockwise and reachable local backups"})
		} else {
			result.Errors = append(result.Errors, "successor list not yet validated")
		}
	}
	n.mu.RLock()
	indices := make([]int, 0, len(n.dirty))
	for index, key := range n.dirty {
		if key == peer.ID.String() {
			indices = append(indices, index)
		}
	}
	n.mu.RUnlock()
	sort.Ints(indices)
	for _, index := range indices {
		if !n.hintCurrent(peer, inc, state) {
			result.Errors = append(result.Errors, "membership event superseded")
			return result
		}
		if index == 0 {
			// A newer ALIVE can supersede a queued death before failover.
			// Validate the retained successor before retiring its dirty marker.
			aliveSuccessor := state == Alive && n.transport.PingRPC(peer.Addr) == nil
			n.mu.Lock()
			if n.Successor != nil && (!samePeer(n.Successor, peer) || (aliveSuccessor && n.hintCurrent(peer, inc, state))) {
				delete(n.dirty, index)
			}
			n.mu.Unlock()
			continue
		}
		if batch <= 0 {
			break
		}
		batch--
		n.mu.RLock()
		expected := n.FingerTable[index]
		n.mu.RUnlock()
		owner, _, err := n.FindSuccessor(FingerStart(n.ID, index, n.M))
		if err == nil && owner != nil && n.validateRef(owner) == nil && (owner.Addr == n.Addr || n.transport.PingRPC(owner.Addr) == nil) {
			n.mu.Lock()
			if n.FingerTable[index] == expected && n.hintCurrent(peer, inc, state) && n.validateRef(owner) == nil {
				n.FingerTable[index] = owner
				delete(n.dirty, index)
				result.Repaired++
				t.Emit("finger_repaired", peerID(peer), map[string]interface{}{"index": index, "owner": peerID(owner), "start": FingerStart(n.ID, index, n.M).String()})
			} else if !samePeer(n.FingerTable[index], peer) {
				delete(n.dirty, index)
			}
			n.mu.Unlock()
		} else {
			result.Errors = append(result.Errors, fmt.Sprintf("finger %d lookup/validation failed: %v", index, err))
		}
	}
	n.mu.RLock()
	for _, key := range n.dirty {
		if key == peer.ID.String() {
			result.Pending = true
			break
		}
	}
	n.mu.RUnlock()
	if state == Dead && affected(n.GetRoutingSnapshot(), peer).Count() > 0 {
		result.Pending = true
	}
	return result
}
