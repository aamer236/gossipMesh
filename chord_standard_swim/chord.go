// Derived from baseline_chord/chord.go at 3c581ae; baseline remains unchanged.
package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"sync"
)

// NodeRef holds the immutable identity and network address of a node.
type NodeRef struct {
	ID   *big.Int `json:"id"`
	Addr string   `json:"addr"`
}

func (nr *NodeRef) String() string {
	if nr == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Node(ID=%s, Addr=%s)", nr.ID.String(), nr.Addr)
}

// InInterval checks whether val is inside (start, end) on a ring modulo 2^m.
// Handles interval boundaries based on leftInclusive and rightInclusive flags.
// Formally per Section 4.2 of Chord paper (Stoica et al.):
// - (s, e) : strictly between s and e
// - (s, e] : s excluded, e included (key ownership)
// - [s, e) : s included, e excluded (finger intervals)
func InInterval(val, start, end *big.Int, leftInclusive, rightInclusive bool, m int) bool {
	twoM := new(big.Int).Lsh(big.NewInt(1), uint(m))
	v := new(big.Int).Mod(val, twoM)
	s := new(big.Int).Mod(start, twoM)
	e := new(big.Int).Mod(end, twoM)

	if leftInclusive && v.Cmp(s) == 0 {
		return true
	}
	if rightInclusive && v.Cmp(e) == 0 {
		return true
	}

	if s.Cmp(e) == 0 {
		// Interval (s, s) covers the entire ring except the point s itself
		return v.Cmp(s) != 0
	}

	if s.Cmp(e) < 0 {
		return v.Cmp(s) > 0 && v.Cmp(e) < 0
	}
	// s > e (wraparound 0)
	return v.Cmp(s) > 0 || v.Cmp(e) < 0
}

// FingerStart computes (n + 2^i) mod 2^m for finger table index i (0 <= i < m).
// Table 1: finger[k].start = (n + 2^{k-1}) mod 2^m, 1 <= k <= m.
func FingerStart(nodeID *big.Int, i int, m int) *big.Int {
	twoM := new(big.Int).Lsh(big.NewInt(1), uint(m))
	offset := new(big.Int).Lsh(big.NewInt(1), uint(i))
	start := new(big.Int).Add(nodeID, offset)
	return start.Mod(start, twoM)
}

// HashKey computes the SHA-1 integer ID modulo 2^m for a string key.
// Matches Section 4.2: standard base hash SHA-1 for m-bit identifiers.
func HashKey(key string, m int) *big.Int {
	h := sha1.New()
	h.Write([]byte(key))
	digest := h.Sum(nil)
	id := new(big.Int).SetBytes(digest)
	twoM := new(big.Int).Lsh(big.NewInt(1), uint(m))
	return id.Mod(id, twoM)
}

// ChordNode implements routing and lazy maintenance from SIGCOMM 2001 Sections 4-5.
type ChordNode struct {
	mu            sync.RWMutex
	hintMu        sync.RWMutex
	peerHints     map[string]RoutingHint
	dirty         map[int]string
	ID            *big.Int
	Addr          string
	M             int
	R             int
	Successor     *NodeRef
	Predecessor   *NodeRef
	SuccessorList []*NodeRef
	FingerTable   []*NodeRef
	singleton     bool
	transport     *NetworkTransport
	metrics       *MetricsCollector
}

func NewChordNode(id *big.Int, addr string, m, r int, transport *NetworkTransport, metrics *MetricsCollector) *ChordNode {
	self := &NodeRef{ID: id, Addr: addr}
	fingers := make([]*NodeRef, m)
	for i := range fingers {
		fingers[i] = self
	}
	succList := make([]*NodeRef, 1, r)
	succList[0] = self

	return &ChordNode{
		ID:            id,
		Addr:          addr,
		M:             m,
		R:             r,
		Successor:     self,
		Predecessor:   nil,
		SuccessorList: succList,
		FingerTable:   fingers,
		singleton:     true,
		transport:     transport,
		metrics:       metrics,
	}
}

// FindSuccessor finds the node responsible for keyID.
// Implementation follows Figure 4 (find_successor & find_predecessor) augmented
// with Section 5.2 failure handling:
// "If node n notices that its successor has failed, it replaces it with the first
// live entry in its successor list... All that is needed is a list of alternate
// nodes, easily found in the finger table entries preceding that of the failed node.
// If the failed node had a very low finger table index, nodes in the successor-list
// are also available as alternates."
func (n *ChordNode) FindSuccessor(keyID *big.Int) (*NodeRef, int, error) {
	return n.findSuccessor(keyID, nil)
}

// The path bounds recursive RPCs and rejects cycles in stale routing state.
// No global membership information participates in routing.
func (n *ChordNode) findSuccessor(keyID *big.Int, path []string) (*NodeRef, int, error) {
	if !validID(keyID, n.M) {
		return nil, 0, fmt.Errorf("key ID outside identifier space")
	}
	if len(path) >= 512 {
		return nil, 0, fmt.Errorf("lookup hop limit exceeded")
	}
	for _, addr := range path {
		if addr == n.Addr {
			return nil, 0, fmt.Errorf("routing cycle at %s", n.Addr)
		}
	}
	path = append(append([]string(nil), path...), n.Addr)
	n.mu.RLock()
	succ, singleton := n.Successor, n.singleton
	n.mu.RUnlock()
	if singleton && succ != nil && succ.Addr == n.Addr {
		return &NodeRef{ID: n.ID, Addr: n.Addr}, 0, nil
	}
	if keyID.Cmp(n.ID) == 0 {
		return &NodeRef{ID: n.ID, Addr: n.Addr}, 0, nil
	}
	if succ != nil && succ.Addr == n.Addr {
		return nil, 0, fmt.Errorf("joining node has not learned a remote successor")
	}
	if succ == nil {
		n.handleSuccessorFailure(nil)
		n.mu.RLock()
		succ = n.Successor
		n.mu.RUnlock()
	}
	if succ != nil && InInterval(keyID, n.ID, succ.ID, false, true, n.M) {
		if err := n.transport.PingRPC(succ.Addr); err == nil {
			return succ, 1, nil
		}
		n.handleSuccessorFailure(succ)
		n.metrics.LogRepair("lookup_successor_failure_eviction", 0)
		n.mu.RLock()
		succ = n.Successor
		n.mu.RUnlock()
		if succ != nil && InInterval(keyID, n.ID, succ.ID, false, true, n.M) {
			return succ, 1, nil
		}
	}
	candidates := n.ClosestPrecedingCandidates(keyID)
	n.mu.RLock()
	// The successor is finger zero; include it even when higher fingers are stale.
	if n.Successor != nil {
		candidates = append(candidates, n.Successor)
	}
	candidates = append(candidates, n.SuccessorList...)
	n.mu.RUnlock()
	seen := map[string]bool{n.Addr: true}
	for _, candidate := range candidates {
		if n.validateRef(candidate) != nil || seen[candidate.Addr] {
			continue
		}
		seen[candidate.Addr] = true
		res, hops, err := n.transport.findSuccessorPathRPC(candidate.Addr, keyID, path)
		if err == nil && n.validateRef(res) == nil {
			return res, hops + 1, nil
		}
		n.metrics.LogRepair("alternate_candidate_fallback", 0)
	}
	return nil, 0, fmt.Errorf("no reachable route for key %s from %s", keyID, n.Addr)
}

func validID(id *big.Int, m int) bool {
	return id != nil && m > 0 && m <= 160 && id.Sign() >= 0 && id.BitLen() <= m
}

func (n *ChordNode) validateRef(ref *NodeRef) error {
	if ref == nil || ref.Addr == "" || !validID(ref.ID, n.M) {
		return fmt.Errorf("invalid node reference")
	}
	if ref.ID.Cmp(n.ID) == 0 && ref.Addr != n.Addr {
		return fmt.Errorf("duplicate node ID %s", ref.ID)
	}
	if ref.Addr == n.Addr && ref.ID.Cmp(n.ID) != 0 {
		return fmt.Errorf("conflicting node address")
	}
	if n.peerBlocked(ref.Addr) {
		return fmt.Errorf("peer %s has a confirmed-dead membership hint", ref.Addr)
	}
	return nil
}

// Called under mu: the paper's finger[1] is this implementation's finger[0].
func (n *ChordNode) setSuccessorLocked(succ *NodeRef) {
	n.Successor = succ
	n.FingerTable[0] = succ
	if succ != nil {
		delete(n.dirty, 0)
	}
	if succ != nil && succ.Addr != n.Addr {
		n.singleton = false
	}
}

// Preserve clockwise order, uniqueness and the r-entry bound.
func (n *ChordNode) successorHints(refs []*NodeRef) []*NodeRef {
	seen := make(map[string]bool)
	result := make([]*NodeRef, 0, len(refs))
	for _, ref := range refs {
		if n.validateRef(ref) == nil && ref.Addr != n.Addr && !seen[ref.Addr] {
			seen[ref.Addr] = true
			result = append(result, ref)
		}
	}
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(n.M))
	distance := func(ref *NodeRef) *big.Int { return new(big.Int).Mod(new(big.Int).Sub(ref.ID, n.ID), modulus) }
	sort.Slice(result, func(i, j int) bool { return distance(result[i]).Cmp(distance(result[j])) < 0 })
	return result
}

// ClosestPrecedingCandidates returns candidate nodes preceding id in descending order of closeness,
// implementing Figure 4's closest_preceding_finger plus Section 5.2 alternate nodes.
func (n *ChordNode) ClosestPrecedingCandidates(id *big.Int) []*NodeRef {
	n.mu.RLock()
	defer n.mu.RUnlock()

	seen := make(map[string]bool)
	candidates := make([]*NodeRef, 0, n.M+len(n.SuccessorList))

	// Search finger table from M-1 down to 0 (Figure 4)
	for i := n.M - 1; i >= 0; i-- {
		f := n.FingerTable[i]
		if f != nil && f.Addr != "" && f.Addr != n.Addr && !seen[f.Addr] {
			if InInterval(f.ID, n.ID, id, false, false, n.M) {
				candidates = append(candidates, f)
				seen[f.Addr] = true
			}
		}
	}

	// Append successor list entries in order as alternates (Section 5.2)
	for _, s := range n.SuccessorList {
		if s != nil && s.Addr != "" && s.Addr != n.Addr && !seen[s.Addr] {
			if InInterval(s.ID, n.ID, id, false, false, n.M) {
				candidates = append(candidates, s)
				seen[s.Addr] = true
			}
		}
	}

	return candidates
}

// ClosestPrecedingNode returns the single closest node preceding id.
func (n *ChordNode) ClosestPrecedingNode(id *big.Int) *NodeRef {
	candidates := n.ClosestPrecedingCandidates(id)
	if len(candidates) > 0 {
		return candidates[0]
	}
	return &NodeRef{ID: n.ID, Addr: n.Addr}
}

// Stabilize periodically verifies immediate successor and notifies it (Figure 7 + Section 5.2).
func (n *ChordNode) Stabilize() {
	n.mu.RLock()
	succ, pred := n.Successor, n.Predecessor
	n.mu.RUnlock()
	if succ == nil {
		n.handleSuccessorFailure(nil)
		return
	}
	if succ.Addr == n.Addr {
		if pred == nil || pred.Addr == n.Addr || n.validateRef(pred) != nil {
			return
		}
		if n.transport.PingRPC(pred.Addr) != nil {
			return
		}
		n.mu.Lock()
		if n.Successor == succ {
			n.setSuccessorLocked(pred)
			n.SuccessorList = []*NodeRef{pred}
		}
		n.mu.Unlock()
		return
	}
	x, list, err := n.transport.GetPredecessorAndListRPC(succ.Addr)
	if err != nil {
		n.handleSuccessorFailure(succ)
		n.metrics.LogRepair("successor_failure_eviction", 0)
		return
	}
	chosen := succ
	if n.validateRef(x) == nil && x.Addr != n.Addr && InInterval(x.ID, n.ID, succ.ID, false, false, n.M) {
		// Build backups from the chosen successor, not the previous successor.
		if _, newList, probeErr := n.transport.GetPredecessorAndListRPC(x.Addr); probeErr == nil {
			chosen = x
			list = append([]*NodeRef{succ}, newList...)
		}
	}
	refs := append([]*NodeRef{chosen}, list...)
	hints := n.successorHints(refs)
	// Keep the selected successor first. Any closer hint is learned on a later
	// stabilization; backups must lie clockwise beyond the selected successor.
	newList := []*NodeRef{chosen}
	for _, ref := range hints {
		if ref.Addr != chosen.Addr && !InInterval(ref.ID, n.ID, chosen.ID, false, false, n.M) && len(newList) < n.R {
			newList = append(newList, ref)
		}
	}
	n.mu.Lock()
	// A lookup may already have repaired this successor while the RPC ran.
	if n.Successor != succ {
		n.mu.Unlock()
		return
	}
	if n.validateRef(chosen) != nil {
		n.mu.Unlock()
		return
	}
	n.setSuccessorLocked(chosen)
	n.SuccessorList = newList
	n.mu.Unlock()
	_ = n.transport.NotifyRPC(chosen.Addr, &NodeRef{ID: n.ID, Addr: n.Addr})
}

// Notify is called when another node candidate believes it is our predecessor (Figure 7).
func (n *ChordNode) Notify(candidate *NodeRef) error {
	if err := n.validateRef(candidate); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.validateRef(candidate) != nil {
		return fmt.Errorf("candidate is no longer eligible")
	}
	if candidate.Addr == n.Addr {
		return nil
	}
	if n.Predecessor == nil || n.Predecessor.Addr == n.Addr || InInterval(candidate.ID, n.Predecessor.ID, n.ID, false, false, n.M) {
		n.Predecessor = candidate
	}
	return nil
}

// Figure 7 selects a random non-successor finger. Stabilize owns finger zero.
func (n *ChordNode) FixFingers() {
	if n.M <= 1 {
		return
	}
	idx := 1 + rand.Intn(n.M-1)
	succ, _, err := n.FindSuccessor(FingerStart(n.ID, idx, n.M))
	if err == nil && succ != nil {
		n.mu.Lock()
		if n.validateRef(succ) == nil {
			n.FingerTable[idx] = succ
			delete(n.dirty, idx)
		}
		n.mu.Unlock()
		n.metrics.LogRepair("fix_finger", 0)
	}
}

// CheckPredecessor periodically verifies whether predecessor has failed.
func (n *ChordNode) CheckPredecessor() {
	n.mu.RLock()
	pred := n.Predecessor
	n.mu.RUnlock()

	if pred == nil || pred.Addr == n.Addr {
		return
	}

	if err := n.transport.PingRPC(pred.Addr); err != nil {
		n.mu.Lock()
		if n.Predecessor != nil && n.Predecessor.Addr == pred.Addr {
			n.Predecessor = nil
		}
		n.mu.Unlock()
		n.metrics.LogRepair("check_predecessor_failed", 0)
	}
}

// Join enters a Chord network using an existing bootstrap node (Figure 7).
func (n *ChordNode) Join(bootstrapAddr string) error {
	if bootstrapAddr == "" || bootstrapAddr == n.Addr {
		// Figure 6 / Singleton initialization
		self := &NodeRef{ID: n.ID, Addr: n.Addr}
		n.mu.Lock()
		n.setSuccessorLocked(self)
		n.singleton = true
		n.Predecessor = nil
		n.SuccessorList = []*NodeRef{self}
		for i := range n.FingerTable {
			n.FingerTable[i] = self
		}
		n.mu.Unlock()
		return nil
	}

	// Figure 7: predecessor = nil; successor = n'.find_successor(n);
	succ, _, err := n.transport.FindSuccessorRPC(bootstrapAddr, n.ID)
	if err != nil {
		return fmt.Errorf("failed to join ring via %s: %w", bootstrapAddr, err)
	}

	if err := n.validateRef(succ); err != nil {
		return fmt.Errorf("invalid join successor: %w", err)
	}
	n.mu.Lock()
	n.setSuccessorLocked(succ)
	n.singleton = false
	for i := 1; i < n.M; i++ {
		n.FingerTable[i] = nil
	}
	n.SuccessorList = []*NodeRef{succ}
	n.Predecessor = nil
	n.mu.Unlock()
	return nil
}

// handleSuccessorFailure evicts the dead successor and promotes the next live candidate (Section 5.2).
func (n *ChordNode) handleSuccessorFailure(failedSucc *NodeRef) {
	n.handleSuccessorFailureIf(failedSucc, nil)
}

// Coordinator jobs supply a version guard. Ordinary Chord failures retain the
// baseline pointer comparison. The guard is rechecked at commit, after probes.
func (n *ChordNode) handleSuccessorFailureIf(failedSucc *NodeRef, stillValid func() bool) {
	n.mu.RLock()
	current := n.Successor
	if current != failedSucc {
		n.mu.RUnlock()
		return
	}
	refs := append([]*NodeRef(nil), n.SuccessorList...)
	refs = append(refs, n.FingerTable...)
	n.mu.RUnlock()
	candidates := n.successorHints(refs)
	live := make([]*NodeRef, 0, n.R)
	for _, ref := range candidates {
		if failedSucc != nil && ref.Addr == failedSucc.Addr {
			continue
		}
		// Never hold the routing-state lock while waiting for a network timeout.
		if n.transport.PingRPC(ref.Addr) == nil {
			live = append(live, ref)
			if len(live) == n.R {
				break
			}
		}
	}
	n.mu.Lock()
	if n.Successor != current || (stillValid != nil && !stillValid()) {
		n.mu.Unlock()
		return
	}
	// A candidate may have received its own DEAD hint while its ping was in
	// flight. Revalidate eligibility without I/O under the commit lock.
	eligible := make([]*NodeRef, 0, len(live))
	for _, ref := range live {
		if n.validateRef(ref) == nil {
			eligible = append(eligible, ref)
		}
	}
	live = eligible
	n.SuccessorList = live
	var promoted *NodeRef
	if len(live) > 0 {
		n.setSuccessorLocked(live[0])
		promoted = live[0]
	} else {
		// Losing contact never proves singleton ownership.
		n.setSuccessorLocked(nil)
		n.singleton = false
	}
	n.mu.Unlock()
	// Record every actual, ping-validated promotion, including lookup-triggered
	// and indirect repairs initiated by another participant's coordinator.
	if promoted != nil && n.transport != nil && n.transport.telemetry != nil {
		n.transport.telemetry.Emit("successor_promoted", peerID(failedSucc), map[string]interface{}{"successor": peerID(promoted), "address": promoted.Addr, "cause": "validated_chord_fallback", "confirmed_dead_hint": failedSucc != nil && n.peerBlocked(failedSucc.Addr)})
	}

}

// GetState returns a snapshot of the node's internal state.
func (n *ChordNode) GetState() map[string]interface{} {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fingers := make([]map[string]interface{}, 0, len(n.FingerTable))
	for i, f := range n.FingerTable {
		if f != nil {
			fingers = append(fingers, map[string]interface{}{
				"index": i,
				"id":    f.ID.String(),
				"addr":  f.Addr,
			})
		}
	}

	succList := make([]map[string]interface{}, 0, len(n.SuccessorList))
	for _, s := range n.SuccessorList {
		if s != nil {
			succList = append(succList, map[string]interface{}{
				"id":   s.ID.String(),
				"addr": s.Addr,
			})
		}
	}

	var predMap interface{} = nil
	if n.Predecessor != nil {
		predMap = map[string]interface{}{
			"id":   n.Predecessor.ID.String(),
			"addr": n.Predecessor.Addr,
		}
	}

	var succMap interface{} = nil
	if n.Successor != nil {
		succMap = map[string]interface{}{
			"id":   n.Successor.ID.String(),
			"addr": n.Successor.Addr,
		}
	}

	return map[string]interface{}{
		"id":             n.ID.String(),
		"m":              n.M,
		"r":              n.R,
		"addr":           n.Addr,
		"successor":      succMap,
		"predecessor":    predMap,
		"successor_list": succList,
		"fingers":        fingers,
	}
}
