package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testSWIMConfig() SWIMConfig {
	return SWIMConfig{Interval: 60 * time.Millisecond, DirectTimeout: 10 * time.Millisecond, IndirectHelpers: 2, SuspicionTimeout: 300 * time.Millisecond, PiggybackLimit: 3, BufferLimit: 16, MaxMembers: 128, RetransmitFactor: 2, Seed: 7}
}
func testPeer(id int64) *NodeRef {
	return &NodeRef{ID: big.NewInt(id), Addr: fmt.Sprintf("127.0.0.1:%d", 24000+id)}
}
func testMembership(t *testing.T, id int64) *Membership {
	t.Helper()
	m, err := NewMembership(testPeer(id), 8, testSWIMConfig(), NewTelemetry(fmt.Sprint(id), "quiet", nil))
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func mergeForTest(t *testing.T, m *Membership, u Update) {
	t.Helper()
	if _, err := m.Merge(u); err != nil {
		t.Fatal(err)
	}
}

type fakeSWIMNetwork struct {
	mu      sync.Mutex
	nodes   map[string]*SWIM
	blocked map[string]bool
	corrupt bool
	entered chan struct{}
	wait    bool
}

func (n *fakeSWIMNetwork) Exchange(ctx context.Context, addr string, request SWIMMessage) (SWIMMessage, error) {
	n.mu.Lock()
	target := n.nodes[addr]
	blocked := n.blocked[request.From.Peer.Addr+">"+addr] || n.blocked[addr]
	corrupt := n.corrupt
	entered, wait := n.entered, n.wait
	n.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if wait {
		<-ctx.Done()
		return SWIMMessage{}, ctx.Err()
	}
	if blocked || target == nil {
		return SWIMMessage{}, context.DeadlineExceeded
	}
	reply, err := target.Receive(ctx, request)
	if corrupt {
		reply.ProbeID = "old-round"
		if reply.Witness != nil {
			reply.Witness.Incarnation = 99
		}
	}
	return reply, err
}
func fakeCluster(t *testing.T, count int) ([]*SWIM, *fakeSWIMNetwork) {
	t.Helper()
	network := &fakeSWIMNetwork{nodes: make(map[string]*SWIM), blocked: make(map[string]bool)}
	nodes := make([]*SWIM, count)
	for i := range nodes {
		m := testMembership(t, int64(i))
		nodes[i] = NewSWIM(m, network, m.telemetry)
		network.nodes[m.self.Addr] = nodes[i]
	}
	for _, node := range nodes {
		for _, other := range nodes {
			if node != other {
				mergeForTest(t, node.table, other.table.Self().Update)
			}
		}
	}
	return nodes, network
}
func TestSWIMDirectAndIndirectProbes(t *testing.T) {
	for _, indirect := range []bool{false, true} {
		t.Run(fmt.Sprint(indirect), func(t *testing.T) {
			nodes, network := fakeCluster(t, 3)
			if indirect {
				network.blocked[nodes[0].table.self.Addr+">"+nodes[1].table.self.Addr] = true
			}
			target, _ := nodes[0].table.Get("1")
			if !nodes[0].Probe(context.Background(), target) {
				t.Fatal("healthy target probe failed")
			}
			member, _ := nodes[0].table.Get("1")
			if member.State != Alive {
				t.Fatal(member.State)
			}
			counters := nodes[0].telemetry.Snapshot()
			if indirect && counters["indirect_probes"] == 0 {
				t.Fatal("indirect path was not exercised")
			}
			if !indirect && counters["direct_ack"] != 1 {
				t.Fatal(counters)
			}
		})
	}
}
func TestSWIMFailureSuspicionAndExpiry(t *testing.T) {
	nodes, network := fakeCluster(t, 3)
	m := nodes[0].table
	now := time.Unix(100, 0)
	m.now = func() time.Time { return now }
	network.blocked[nodes[1].table.self.Addr] = true
	target, _ := m.Get("1")
	if nodes[0].Probe(context.Background(), target) {
		t.Fatal("failed target acknowledged")
	}
	member, _ := m.Get("1")
	if member.State != Suspect {
		t.Fatalf("must suspect before death, got %s", member.State)
	}
	m.Expire(now.Add(m.cfg.SuspicionTimeout - time.Nanosecond))
	member, _ = m.Get("1")
	if member.State != Suspect {
		t.Fatal("expired early")
	}
	m.Expire(now.Add(m.cfg.SuspicionTimeout))
	m.Expire(now.Add(time.Hour))
	member, _ = m.Get("1")
	if member.State != Dead {
		t.Fatal("suspicion never expired")
	}
	if m.telemetry.Snapshot()["confirmed_failures"] != 1 {
		t.Fatal("duplicate death counted")
	}
}
func TestSWIMSelfRefutationAndReordering(t *testing.T) {
	nodes, _ := fakeCluster(t, 2)
	a, b := nodes[0], nodes[1]
	mergeForTest(t, a.table, Update{Peer: b.table.self, State: Suspect, Incarnation: 0})
	target, _ := a.table.Get("1")
	if !a.Probe(context.Background(), target) {
		t.Fatal("refutation probe failed")
	}
	current, _ := a.table.Get("1")
	if current.State != Alive || current.Incarnation == 0 {
		t.Fatalf("no newer self ALIVE: %+v", current)
	}
	mergeForTest(t, a.table, Update{Peer: b.table.self, State: Dead, Incarnation: 0})
	mergeForTest(t, a.table, Update{Peer: b.table.self, State: Suspect, Incarnation: 0})
	after, _ := a.table.Get("1")
	if after.State != Alive || after.Incarnation != current.Incarnation {
		t.Fatal("stale update reversed refutation")
	}
	if b.telemetry.Snapshot()["refutations"] == 0 {
		t.Fatal("self refutation not observed")
	}
}
func TestSWIMVersionPrecedenceAndDeathIdempotence(t *testing.T) {
	m := testMembership(t, 0)
	peer := testPeer(1)
	events := 0
	m.onEvent = func(e MembershipEvent) { events++ }
	for _, u := range []Update{{Peer: peer, State: Alive, Incarnation: 2}, {Peer: peer, State: Suspect, Incarnation: 2}, {Peer: peer, State: Alive, Incarnation: 2}, {Peer: peer, State: Dead, Incarnation: 2}, {Peer: peer, State: Dead, Incarnation: 2}, {Peer: peer, State: Alive, Incarnation: 1}} {
		mergeForTest(t, m, u)
	}
	member, _ := m.Get("1")
	if member.State != Dead || events != 3 {
		t.Fatalf("incorrect precedence/events %s %d", member.State, events)
	}
	mergeForTest(t, m, Update{Peer: peer, State: Alive, Incarnation: 3})
	member, _ = m.Get("1")
	if member.State != Alive {
		t.Fatal("new incarnation could not rejoin")
	}
}
func TestSWIMGossipBufferBoundDedupAndRetirement(t *testing.T) {
	m := testMembership(t, 0)
	m.cfg.BufferLimit = 6
	for i := int64(1); i < 30; i++ {
		mergeForTest(t, m, Update{Peer: testPeer(i), State: Alive})
	}
	if len(m.buffer) != 6 || len(m.Snapshot()) != 30 {
		t.Fatal("buffer bound confused with full membership")
	}
	updates := m.Piggyback()
	if len(updates) > m.cfg.PiggybackLimit {
		t.Fatal("piggyback overflow")
	}
	before := m.buffer["29"].Sent
	mergeForTest(t, m, Update{Peer: testPeer(29), State: Alive})
	if m.buffer["29"].Sent != before {
		t.Fatal("duplicate reset retransmissions")
	}
	for i := 0; i < 100; i++ {
		m.Piggyback()
	}
	if len(m.buffer) != 0 {
		t.Fatal("gossip never retired")
	}
	if m.telemetry.Snapshot()["update_retired"] == 0 {
		t.Fatal("retirement not observable")
	}
}
func TestSWIMTargetCoverageIncludesSuspectsExcludesSelfAndDead(t *testing.T) {
	m := testMembership(t, 0)
	for i := int64(1); i <= 6; i++ {
		mergeForTest(t, m, Update{Peer: testPeer(i), State: Alive})
	}
	mergeForTest(t, m, Update{Peer: testPeer(2), State: Suspect})
	mergeForTest(t, m, Update{Peer: testPeer(3), State: Dead})
	for round := 0; round < 3; round++ {
		seen := map[string]bool{}
		for i := 0; i < 5; i++ {
			target, ok := m.NextTarget()
			if !ok {
				t.Fatal("missing target")
			}
			key := peerID(target.Peer)
			if key == "0" || key == "3" || seen[key] {
				t.Fatalf("invalid selection %s", key)
			}
			seen[key] = true
		}
		if !seen["2"] {
			t.Fatal("suspect excluded from probing")
		}
	}
}
func TestSWIMRejectsStaleACKAndIdentityMismatch(t *testing.T) {
	nodes, network := fakeCluster(t, 2)
	network.corrupt = true
	target, _ := nodes[0].table.Get("1")
	if nodes[0].Probe(context.Background(), target) {
		t.Fatal("old probe ACK completed newer round")
	}
	member, _ := nodes[0].table.Get("1")
	if member.Incarnation == 99 {
		t.Fatal("uncorrelated response merged")
	}
	bad := nodes[1].table.Self().Update
	bad.Peer = clonePeer(bad.Peer)
	bad.Peer.Addr = testPeer(9).Addr
	if _, err := nodes[0].table.Merge(bad); err == nil {
		t.Fatal("identity address rebinding accepted")
	}
	request := nodes[0].message("PING", "test", nodes[0].table.self, &target.Update)
	request.Target.Peer = testPeer(9)
	if _, err := nodes[1].Receive(context.Background(), request); err == nil {
		t.Fatal("misdirected PING accepted")
	}
}
func TestSWIMACKGuardAndSuspicionDeadlineNotExtended(t *testing.T) {
	m := testMembership(t, 0)
	peer := testPeer(1)
	mergeForTest(t, m, Update{Peer: peer, State: Alive})
	start, _ := m.Get("1")
	m.Acknowledge(start.Update)
	m.SuspectAfterFailure(start)
	current, _ := m.Get("1")
	if current.State != Alive {
		t.Fatal("newer ACK ignored by failed older probe")
	}
	now := time.Unix(500, 0)
	m.now = func() time.Time { return now }
	mergeForTest(t, m, Update{Peer: peer, State: Suspect})
	current, _ = m.Get("1")
	deadline := current.SuspectUntil
	now = now.Add(time.Second)
	mergeForTest(t, m, Update{Peer: peer, State: Suspect})
	current, _ = m.Get("1")
	if !current.SuspectUntil.Equal(deadline) {
		t.Fatal("duplicate suspicion extended deadline")
	}
}
func TestSWIMShutdownCancelsInFlightProbe(t *testing.T) {
	nodes, network := fakeCluster(t, 2)
	network.wait = true
	network.entered = make(chan struct{}, 1)
	if err := nodes[0].Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-network.entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	done := make(chan struct{})
	go func() { nodes[0].Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel probe")
	}
	current, _ := nodes[0].table.Get("1")
	if current.State != Alive {
		t.Fatal("shutdown falsely suspected peer")
	}
	nodes[0].Stop()
}
func TestSWIMDurableRestartAndBootstrapTombstone(t *testing.T) {
	cfg := testSWIMConfig()
	cfg.StateFile = filepath.Join(t.TempDir(), "incarnation.json")
	self := testPeer(1)
	telemetry := NewTelemetry("1", "quiet", nil)
	first, err := NewMembership(self, 8, cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	mergeForTest(t, first, Update{Peer: self, State: Suspect})
	restarted, err := NewMembership(self, 8, cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Self().Incarnation <= first.Self().Incarnation {
		t.Fatal("restart incarnation did not increase")
	}
	root := testMembership(t, 0)
	mergeForTest(t, root, Update{Peer: self, State: Dead, Incarnation: 20})
	net := &fakeSWIMNetwork{nodes: make(map[string]*SWIM), blocked: make(map[string]bool)}
	a := NewSWIM(root, net, root.telemetry)
	b := NewSWIM(restarted, net, telemetry)
	net.nodes[root.self.Addr] = a
	net.nodes[self.Addr] = b
	if err = b.Bootstrap(context.Background(), root.self.Addr); err != nil {
		t.Fatal(err)
	}
	member, _ := root.Get("1")
	if member.State != Alive || member.Incarnation <= 20 {
		t.Fatal("bootstrap tombstone permanently excluded restart")
	}
}
func TestSWIMHTTPBootstrapAndPayloadValidation(t *testing.T) {
	newNode := func(id int64) (*SWIM, *httptest.Server) {
		server := httptest.NewUnstartedServer(nil)
		self := &NodeRef{ID: big.NewInt(id), Addr: server.Listener.Addr().String()}
		m, err := NewMembership(self, 8, testSWIMConfig(), NewTelemetry(fmt.Sprint(id), "quiet", nil))
		if err != nil {
			t.Fatal(err)
		}
		s := NewSWIM(m, NewHTTPSWIMTransport(m.telemetry), m.telemetry)
		server.Config.Handler = s // direct HTTP handler needs /swim/rpc only in this fixture
		server.Start()
		t.Cleanup(server.Close)
		return s, server
	}
	a, _ := newNode(0)
	b, serverB := newNode(1)
	if err := b.Bootstrap(context.Background(), a.table.self.Addr); err != nil {
		t.Fatal(err)
	}
	target, _ := a.table.Get("1")
	if !a.Probe(context.Background(), target) {
		t.Fatal("actual HTTP direct probe failed")
	}
	metrics := a.telemetry.Snapshot()
	if metrics["swim_payload_bytes_sent"] == 0 || metrics["swim_payload_bytes_received"] == 0 {
		t.Fatal("SWIM payload counters empty")
	}
	request := a.message("PING", "bad", a.table.self, &target.Update)
	raw, _ := json.Marshal(request)
	badVersion := request
	badVersion.Version = 99
	badRaw, _ := json.Marshal(badVersion)
	client := &http.Client{Timeout: time.Second}
	for _, body := range [][]byte{append(append([]byte(nil), raw...), []byte(" null")...), badRaw, []byte(`{"unknown":true}`)} {
		response, err := client.Post(serverB.URL+"/swim/rpc", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("malformed payload accepted: HTTP %d", response.StatusCode)
		}
	}
}

func TestSWIMStoppedInstanceCannotRestartTimers(t *testing.T) {
	nodes, _ := fakeCluster(t, 2)
	nodes[0].Stop()
	if err := nodes[0].Start(context.Background()); err == nil {
		t.Fatal("stopped SWIM restarted timers")
	}
}
func TestSWIMNewAliveCancelsSuspicionAndPersistenceErrorsSurface(t *testing.T) {
	m := testMembership(t, 0)
	peer := testPeer(1)
	mergeForTest(t, m, Update{Peer: peer, State: Suspect})
	old, _ := m.Get("1")
	mergeForTest(t, m, Update{Peer: peer, State: Alive, Incarnation: 1})
	m.Expire(old.SuspectUntil.Add(time.Hour))
	current, _ := m.Get("1")
	if current.State != Alive {
		t.Fatal("cancelled suspicion expired")
	}
	m.cfg.StateFile = t.TempDir() // a directory is not a valid incarnation file
	if _, err := m.Merge(Update{Peer: m.self, State: Suspect}); err == nil {
		t.Fatal("failed persistence was silently accepted")
	}
	if m.Self().Incarnation != 0 {
		t.Fatal("published an incarnation that was not persisted")
	}
}

func TestRecentAckCannotComeFromGossipOrSurviveTransitions(t *testing.T) {
	m := testMembership(t, 0)
	now := time.Unix(100, 0)
	m.now = func() time.Time { return now }
	u := Update{Peer: testPeer(1), State: Alive, Incarnation: 1}
	mergeForTest(t, m, u)
	if m.RecentAck(u.Peer) {
		t.Fatal("gossip counted as ACK")
	}
	m.Acknowledge(u)
	if !m.RecentAck(u.Peer) {
		t.Fatal("ACK not shared")
	}
	now = now.Add(m.cfg.Interval)
	if m.RecentAck(u.Peer) {
		t.Fatal("expired ACK shared")
	}
	m.Acknowledge(u)
	u.State = Suspect
	mergeForTest(t, m, u)
	if m.RecentAck(u.Peer) {
		t.Fatal("suspected peer used old ACK")
	}
	u.State = Alive
	u.Incarnation++
	mergeForTest(t, m, u)
	if m.RecentAck(u.Peer) {
		t.Fatal("new incarnation inherited old ACK")
	}
	m.Acknowledge(u)
	if !m.RecentAck(u.Peer) {
		t.Fatal("new valid ACK ignored")
	}
	u.State = Dead
	mergeForTest(t, m, u)
	if m.RecentAck(u.Peer) {
		t.Fatal("dead peer used old ACK")
	}
}
