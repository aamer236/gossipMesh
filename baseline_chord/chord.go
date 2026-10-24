package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
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

// ChordNode represents a Chord ring participant adhering to the SIGCOMM 2001 paper.
type ChordNode struct {
	mu            sync.RWMutex
	ID            *big.Int
	Addr          string
	M             int
	R             int
	Successor     *NodeRef
	Predecessor   *NodeRef
	SuccessorList []*NodeRef
	FingerTable   []*NodeRef
	nextFinger    int
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
		nextFinger:    0,
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
	n.mu.RLock()
	succ := n.Successor
	selfAddr := n.Addr
	n.mu.RUnlock()

	// 1. Single node ring
	if succ == nil || succ.Addr == selfAddr {
		return &NodeRef{ID: n.ID, Addr: n.Addr}, 1, nil
	}

	// 2. Base case: keyID is in (n, successor]
	if InInterval(keyID, n.ID, succ.ID, false, true, n.M) {
		// Section 5.2: If successor failed, replace with first live entry in successor list
		if err := n.transport.PingRPC(succ.Addr); err != nil {
			n.handleSuccessorFailure(succ)
			n.metrics.LogRepair("lookup_successor_failure_eviction", 0)
			n.mu.RLock()
			succ = n.Successor
			n.mu.RUnlock()
			if succ.Addr == selfAddr || InInterval(keyID, n.ID, succ.ID, false, true, n.M) {
				return succ, 1, nil
			}
		} else {
			return succ, 1, nil
		}
	}

	// 3. Greedy routing with Section 5.2 alternate node fallback:
	candidates := n.ClosestPrecedingCandidates(keyID)
	for _, candidate := range candidates {
		res, hops, err := n.transport.FindSuccessorRPC(candidate.Addr, keyID)
		if err == nil && res != nil {
			return res, hops + 1, nil
		}
		// Candidate failed; try next alternate candidate in the list
		n.metrics.LogRepair("alternate_candidate_fallback", 0)
	}

	// 4. If all preceding candidates failed, query live successors in the successor-list
	n.mu.RLock()
	succList := make([]*NodeRef, len(n.SuccessorList))
	copy(succList, n.SuccessorList)
	n.mu.RUnlock()

	for _, s := range succList {
		if s != nil && s.Addr != "" && s.Addr != n.Addr {
			res, hops, err := n.transport.FindSuccessorRPC(s.Addr, keyID)
			if err == nil && res != nil {
				return res, hops + 1, nil
			}
		}
	}

	// Fallback to self
	return &NodeRef{ID: n.ID, Addr: n.Addr}, 1, nil
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
	succ := n.Successor
	self := &NodeRef{ID: n.ID, Addr: n.Addr}
	n.mu.RUnlock()

	if succ == nil || succ.Addr == n.Addr {
		// Single node ring; if predecessor set, adopt it as successor
		n.mu.Lock()
		if n.Predecessor != nil && n.Predecessor.Addr != n.Addr {
			n.Successor = n.Predecessor
			n.SuccessorList = []*NodeRef{n.Predecessor}
		}
		n.mu.Unlock()
		return
	}

	// Query successor for its predecessor and successor list
	x, succList, err := n.transport.GetPredecessorAndListRPC(succ.Addr)
	if err != nil {
		// Section 5.2: Successor failed; replace it with first live entry in successor list
		n.handleSuccessorFailure(succ)
		n.metrics.LogRepair("successor_failure_eviction", 0)
		return
	}

	// Figure 7: if x in (n, successor) then successor = x
	if x != nil && x.Addr != "" && x.Addr != n.Addr && InInterval(x.ID, n.ID, succ.ID, false, false, n.M) {
		n.mu.Lock()
		n.Successor = x
		succ = x
		n.mu.Unlock()
	}

	// Section 5.2: Update successor list
	n.mu.Lock()
	newList := make([]*NodeRef, 0, n.R)
	newList = append(newList, succ)
	for _, s := range succList {
		if s != nil && s.Addr != "" && s.Addr != n.Addr && len(newList) < n.R {
			duplicate := false
			for _, existing := range newList {
				if existing.Addr == s.Addr {
					duplicate = true
					break
				}
			}
			if !duplicate {
				newList = append(newList, s)
			}
		}
	}
	n.SuccessorList = newList
	n.mu.Unlock()

	// Figure 7: successor.notify(n)
	_ = n.transport.NotifyRPC(succ.Addr, self)
}

// Notify is called when another node candidate believes it is our predecessor (Figure 7).
func (n *ChordNode) Notify(candidate *NodeRef) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if candidate == nil || candidate.Addr == n.Addr {
		return
	}

	// Figure 7: if (predecessor is nil or candidate in (predecessor, n)) predecessor = candidate
	if n.Predecessor == nil || n.Predecessor.Addr == n.Addr ||
		InInterval(candidate.ID, n.Predecessor.ID, n.ID, false, false, n.M) {
		n.Predecessor = candidate
	}
}

// FixFingers periodically refreshes one entry in the finger table (Figure 7).
func (n *ChordNode) FixFingers() {
	n.mu.Lock()
	idx := n.nextFinger
	n.nextFinger = (n.nextFinger + 1) % n.M
	nodeID := n.ID
	n.mu.Unlock()

	start := FingerStart(nodeID, idx, n.M)
	succ, _, err := n.FindSuccessor(start)
	if err == nil && succ != nil {
		n.mu.Lock()
		n.FingerTable[idx] = succ
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
		n.Successor = self
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

	n.mu.Lock()
	n.Successor = succ
	n.SuccessorList = []*NodeRef{succ}
	n.FingerTable[0] = succ
	n.Predecessor = nil
	n.mu.Unlock()
	return nil
}

// handleSuccessorFailure evicts the dead successor and promotes the next live candidate (Section 5.2).
func (n *ChordNode) handleSuccessorFailure(failedSucc *NodeRef) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Filter out failed successor from successor list
	filtered := make([]*NodeRef, 0, len(n.SuccessorList))
	for _, s := range n.SuccessorList {
		if s != nil && s.Addr != failedSucc.Addr && s.Addr != n.Addr {
			filtered = append(filtered, s)
		}
	}
	n.SuccessorList = filtered

	// Find the next responsive candidate in the successor list
	for _, candidate := range n.SuccessorList {
		if err := n.transport.PingRPC(candidate.Addr); err == nil {
			n.Successor = candidate
			n.FingerTable[0] = candidate
			return
		}
	}

	// If no successors left, fall back to self
	self := &NodeRef{ID: n.ID, Addr: n.Addr}
	n.Successor = self
	n.SuccessorList = []*NodeRef{self}
	n.FingerTable[0] = self
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
		"addr":           n.Addr,
		"successor":      succMap,
		"predecessor":    predMap,
		"successor_list": succList,
		"fingers":        fingers,
	}
}
