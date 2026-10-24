package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type MemberState string

const (
	Alive   MemberState = "ALIVE"
	Suspect MemberState = "SUSPECT"
	Dead    MemberState = "DEAD"
)

type Update struct {
	Peer        *NodeRef    `json:"peer"`
	State       MemberState `json:"state"`
	Incarnation uint64      `json:"incarnation"`
}
type Member struct {
	Update
	ChangedAt    time.Time `json:"changed_at"`
	SuspectUntil time.Time `json:"suspect_until,omitempty"`
	LastAck      time.Time `json:"last_ack,omitempty"`
	ackVersion   uint64
}
type MembershipEvent struct {
	Member   Member
	Previous MemberState
	Kind     string
}
type gossipEntry struct {
	Update Update
	Sent   int
	Order  uint64
}
type Membership struct {
	mu        sync.Mutex
	self      *NodeRef
	m         int
	cfg       SWIMConfig
	members   map[string]Member
	addresses map[string]string
	buffer    map[string]gossipEntry
	round     []string
	sequence  uint64
	order     uint64
	rng       *rand.Rand
	now       func() time.Time
	telemetry *Telemetry
	onEvent   func(MembershipEvent)
}

func newBig(id *big.Int) *big.Int {
	if id == nil {
		return nil
	}
	return new(big.Int).Set(id)
}
func clonePeer(peer *NodeRef) *NodeRef {
	if peer == nil {
		return nil
	}
	return &NodeRef{ID: newBig(peer.ID), Addr: peer.Addr}
}
func stateRank(state MemberState) int {
	switch state {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Dead:
		return 2
	}
	return -1
}
func newer(next, old Update) bool {
	return next.Incarnation > old.Incarnation || (next.Incarnation == old.Incarnation && stateRank(next.State) > stateRank(old.State))
}
func cloneUpdate(u Update) Update { u.Peer = clonePeer(u.Peer); return u }
func cloneMember(m Member) Member { m.Update = cloneUpdate(m.Update); return m }
func NewMembership(self *NodeRef, m int, cfg SWIMConfig, t *Telemetry) (*Membership, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if self == nil || !validID(self.ID, m) {
		return nil, fmt.Errorf("invalid local identity")
	}
	if err := validAddress(self.Addr); err != nil {
		return nil, err
	}
	inc, err := loadIncarnation(cfg.StateFile, self)
	if err != nil {
		return nil, err
	}
	table := &Membership{self: clonePeer(self), m: m, cfg: cfg, members: make(map[string]Member), addresses: make(map[string]string), buffer: make(map[string]gossipEntry), rng: rand.New(rand.NewSource(cfg.Seed)), now: time.Now, telemetry: t}
	member := Member{Update: Update{Peer: clonePeer(self), State: Alive, Incarnation: inc}, ChangedAt: table.now()}
	table.members[self.ID.String()] = member
	table.addresses[self.Addr] = self.ID.String()
	table.queueLocked(member.Update)
	return table, nil
}

// Persistence is synchronous before publishing any self incarnation. Atomic rename
// prevents partial state; one process must own a given identity/state file.
func loadIncarnation(path string, self *NodeRef) (uint64, error) {
	if path == "" {
		return 0, nil
	} // deterministic in-memory unit tests only
	var saved struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
		Inc  uint64 `json:"incarnation"`
	}
	raw, err := os.ReadFile(path)
	inc := uint64(0)
	if err == nil {
		if err = json.Unmarshal(raw, &saved); err != nil {
			return 0, fmt.Errorf("invalid incarnation file: %w", err)
		}
		if saved.ID != self.ID.String() || saved.Addr != self.Addr {
			return 0, fmt.Errorf("incarnation file belongs to another identity")
		}
		if saved.Inc == math.MaxUint64 {
			return 0, fmt.Errorf("incarnation exhausted")
		}
		inc = saved.Inc + 1
	} else if !os.IsNotExist(err) {
		return 0, err
	}
	if err = saveIncarnation(path, self, inc); err != nil {
		return 0, err
	}
	return inc, nil
}
func saveIncarnation(path string, self *NodeRef, inc uint64) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(map[string]interface{}{"id": self.ID.String(), "addr": self.Addr, "incarnation": inc})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".incarnation-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
func (m *Membership) validateLocked(u Update) error {
	if u.Peer == nil || !validID(u.Peer.ID, m.m) || stateRank(u.State) < 0 {
		return fmt.Errorf("invalid membership update")
	}
	if err := validAddress(u.Peer.Addr); err != nil {
		return err
	}
	key := u.Peer.ID.String()
	if old, ok := m.members[key]; ok && old.Peer.Addr != u.Peer.Addr {
		return fmt.Errorf("identity address binding changed for %s", key)
	}
	if id, ok := m.addresses[u.Peer.Addr]; ok && id != key {
		return fmt.Errorf("address already belongs to %s", id)
	}
	if _, ok := m.members[key]; !ok && len(m.members) >= m.cfg.MaxMembers {
		return fmt.Errorf("membership capacity reached")
	}
	return nil
}
func (m *Membership) Validate(u Update) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.validateLocked(u)
}
func (m *Membership) Merge(u Update) (bool, error) { return m.merge(u, nil) }
func (m *Membership) merge(u Update, guard *Member) (bool, error) {
	m.telemetry.Emit("update_received", peerID(u.Peer), map[string]interface{}{"state": u.State, "incarnation": u.Incarnation})
	m.mu.Lock()
	if err := m.validateLocked(u); err != nil {
		m.mu.Unlock()
		m.telemetry.Emit("error", peerID(u.Peer), map[string]interface{}{"operation": "merge", "error": err.Error()})
		return false, err
	}
	refutedSelf := false
	key := u.Peer.ID.String()
	old, known := m.members[key]
	if guard != nil && (!known || old.State == Dead || old.Incarnation != guard.Incarnation || old.ackVersion != guard.ackVersion) {
		m.mu.Unlock()
		return false, nil
	}
	if key == m.self.ID.String() {
		// Only the subject advances its own incarnation. Equal ALIVE echoes cannot
		// erase suspicion. A higher-incarnation self refutation/rejoin can supersede
		// a tombstone; this is the documented stable-identity restart extension.
		if u.State != Alive && u.Incarnation >= old.Incarnation {
			if u.Incarnation == math.MaxUint64 {
				m.mu.Unlock()
				return false, fmt.Errorf("incarnation exhausted")
			}
			refutedSelf = true
			u = Update{Peer: clonePeer(m.self), State: Alive, Incarnation: u.Incarnation + 1}
			if err := saveIncarnation(m.cfg.StateFile, m.self, u.Incarnation); err != nil {
				m.mu.Unlock()
				return false, err
			}
		} else if u.State == Alive && u.Incarnation > old.Incarnation {
			// Bootstrap reconciliation: never adopt a remote self version as ours;
			// persist a newer version before advertising the local process.
			if u.Incarnation == math.MaxUint64 {
				m.mu.Unlock()
				return false, fmt.Errorf("incarnation exhausted")
			}
			u.Incarnation++
			if err := saveIncarnation(m.cfg.StateFile, m.self, u.Incarnation); err != nil {
				m.mu.Unlock()
				return false, err
			}
		} else {
			m.mu.Unlock()
			return false, nil
		}
	}
	if known && !newer(u, old.Update) {
		m.mu.Unlock()
		m.telemetry.Emit("update_ignored", key, map[string]interface{}{"state": u.State, "incarnation": u.Incarnation})
		return false, nil
	}
	now := m.now()
	member := Member{Update: cloneUpdate(u), ChangedAt: now, LastAck: old.LastAck, ackVersion: old.ackVersion}
	if u.State == Suspect {
		member.SuspectUntil = now.Add(m.cfg.SuspicionTimeout)
	}
	m.members[key] = member
	m.addresses[u.Peer.Addr] = key
	m.queueLocked(member.Update)
	if !known && key != m.self.ID.String() && u.State != Dead {
		// New members enter the remaining traversal at a random position.
		pos := m.rng.Intn(len(m.round) + 1)
		m.round = append(m.round, "")
		copy(m.round[pos+1:], m.round[pos:])
		m.round[pos] = key
	}
	callback := m.onEvent
	m.mu.Unlock()
	kind := "peer_alive"
	if u.State == Suspect {
		kind = "peer_suspected"
	} else if u.State == Dead {
		kind = "peer_dead"
	} else if !known {
		kind = "peer_added"
	} else if old.State == Suspect {
		kind = "peer_refuted"
	} else if old.State == Dead {
		kind = "peer_rejoined"
	}
	if refutedSelf {
		kind = "peer_refuted"
	}
	if u.State == Suspect {
		m.telemetry.Add("suspicions", 1)
	}
	if u.State == Dead {
		m.telemetry.Add("confirmed_failures", 1)
	}
	if (known && old.State == Suspect && u.State == Alive) || (key == m.self.ID.String() && u.State == Alive) {
		m.telemetry.Add("refutations", 1)
	}
	m.telemetry.Emit("update_merged", key, map[string]interface{}{"state": u.State, "incarnation": u.Incarnation})
	m.telemetry.Emit(kind, key, map[string]interface{}{"previous": old.State, "state": u.State, "incarnation": u.Incarnation})
	if callback != nil {
		callback(MembershipEvent{Member: cloneMember(member), Previous: old.State, Kind: kind})
	}
	return true, nil
}
func peerID(peer *NodeRef) string {
	if peer == nil || peer.ID == nil {
		return ""
	}
	return peer.ID.String()
}
func (m *Membership) queueLocked(u Update) {
	key := u.Peer.ID.String()
	if old, ok := m.buffer[key]; ok && !newer(u, old.Update) {
		return
	}
	if len(m.buffer) >= m.cfg.BufferLimit {
		if _, exists := m.buffer[key]; !exists {
			var victim string
			var chosen gossipEntry
			for id, entry := range m.buffer {
				if victim == "" || entry.Sent > chosen.Sent || (entry.Sent == chosen.Sent && entry.Order < chosen.Order) {
					victim = id
					chosen = entry
				}
			}
			delete(m.buffer, victim)
			m.telemetry.Emit("update_retired", victim, map[string]interface{}{"reason": "capacity"})
		}
	}
	m.order++
	m.buffer[key] = gossipEntry{Update: cloneUpdate(u), Order: m.order}
}
func (m *Membership) Piggyback() []Update {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]gossipEntry, 0, len(m.buffer))
	for _, entry := range m.buffer {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Sent == entries[j].Sent {
			return entries[i].Order < entries[j].Order
		}
		return entries[i].Sent < entries[j].Sent
	})
	limit := m.cfg.RetransmitFactor * int(math.Ceil(math.Log2(float64(len(m.members)+1))))
	out := make([]Update, 0, m.cfg.PiggybackLimit)
	for _, entry := range entries {
		if len(out) == m.cfg.PiggybackLimit {
			break
		}
		key := entry.Update.Peer.ID.String()
		out = append(out, cloneUpdate(entry.Update))
		entry.Sent++
		m.telemetry.Emit("update_piggybacked", key, map[string]interface{}{"state": entry.Update.State, "transmissions": entry.Sent})
		if entry.Sent >= limit {
			delete(m.buffer, key)
			m.telemetry.Emit("update_retired", key, map[string]interface{}{"reason": "retransmit_limit"})
		} else {
			m.buffer[key] = entry
		}
	}
	return out
}
func (m *Membership) Self() Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneMember(m.members[m.self.ID.String()])
}
func (m *Membership) Get(key string) (Member, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.members[key]
	return cloneMember(v), ok
}
func (m *Membership) Snapshot() []Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Member, 0, len(m.members))
	for _, v := range m.members {
		out = append(out, cloneMember(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer.ID.Cmp(out[j].Peer.ID) < 0 })
	return out
}
func (m *Membership) NextTarget() (Member, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.round) == 0 {
		for id, v := range m.members {
			if id != m.self.ID.String() && v.State != Dead {
				m.round = append(m.round, id)
			}
		}
		sort.Strings(m.round)
		m.rng.Shuffle(len(m.round), func(i, j int) { m.round[i], m.round[j] = m.round[j], m.round[i] })
	}
	for len(m.round) > 0 {
		key := m.round[0]
		m.round = m.round[1:]
		if v, ok := m.members[key]; ok && v.State != Dead && key != m.self.ID.String() {
			return cloneMember(v), true
		}
	}
	return Member{}, false
}
func (m *Membership) Helpers(target string) []Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Member, 0, len(m.members))
	for id, v := range m.members {
		if id != target && id != m.self.ID.String() && v.State != Dead {
			out = append(out, cloneMember(v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer.ID.Cmp(out[j].Peer.ID) < 0 })
	m.rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if len(out) > m.cfg.IndirectHelpers {
		out = out[:m.cfg.IndirectHelpers]
	}
	return out
}
func (m *Membership) Acknowledge(target Update) {
	_, err := m.Merge(target)
	if err != nil {
		m.telemetry.Emit("error", peerID(target.Peer), map[string]interface{}{"operation": "ack_merge", "error": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := target.Peer.ID.String()
	member := m.members[key]
	if member.Incarnation == target.Incarnation && member.State == Alive {
		member.LastAck = m.now()
		member.ackVersion++
		m.members[key] = member
	}
}
func (m *Membership) SuspectAfterFailure(start Member) {
	_, err := m.merge(Update{Peer: start.Peer, State: Suspect, Incarnation: start.Incarnation}, &start)
	if err != nil {
		m.telemetry.Emit("error", peerID(start.Peer), map[string]interface{}{"operation": "suspect", "error": err.Error()})
	}
}
func (m *Membership) Expire(now time.Time) {
	m.mu.Lock()
	expired := make([]Update, 0)
	for id, member := range m.members {
		if id != m.self.ID.String() && member.State == Suspect && !now.Before(member.SuspectUntil) {
			expired = append(expired, Update{Peer: clonePeer(member.Peer), State: Dead, Incarnation: member.Incarnation})
		}
	}
	m.mu.Unlock()
	for _, u := range expired {
		if _, err := m.Merge(u); err != nil {
			m.telemetry.Emit("error", peerID(u.Peer), map[string]interface{}{"operation": "expire", "error": err.Error()})
		}
	}
}
