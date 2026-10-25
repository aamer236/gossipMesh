package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func chordFixture(t *testing.T, id int64) (*ChordNode, *httptest.Server) {
	t.Helper()
	metrics := NewMetricsCollector()
	tr := NewNetworkTransport(40*time.Millisecond, metrics)
	n := NewChordNode(big.NewInt(id), "127.0.0.1:1", 8, 4, tr, metrics)
	tr.SetNode(n)
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc/find_successor", tr.handleFindSuccessor)
	mux.HandleFunc("/rpc/ping", tr.handlePing)
	mux.HandleFunc("/rpc/notify", tr.handleNotify)
	mux.HandleFunc("/rpc/predecessor_and_list", tr.handlePredecessorAndList)
	server := httptest.NewServer(mux)
	n.Addr = strings.TrimPrefix(server.URL, "http://")
	if err := n.Join(""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return n, server
}
func chordRing(t *testing.T) ([]*ChordNode, []*httptest.Server) {
	t.Helper()
	var nodes []*ChordNode
	var servers []*httptest.Server
	for _, id := range []int64{0, 64, 128, 192} {
		n, s := chordFixture(t, id)
		if len(nodes) > 0 {
			if err := n.Join(nodes[0].Addr); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
		servers = append(servers, s)
	}
	for round := 0; round < 160; round++ {
		for _, node := range nodes {
			node.CheckPredecessor()
			node.Stabilize()
			node.FixFingers()
		}
	}
	return nodes, servers
}
func await(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not complete")
		case <-tick.C:
		}
	}
}
func eventFor(peer *ChordNode, state MemberState, inc uint64) MembershipEvent {
	return MembershipEvent{Member: Member{Update: Update{Peer: &NodeRef{ID: peer.ID, Addr: peer.Addr}, State: state, Incarnation: inc}}, Kind: "test"}
}
func TestGossipMeshSuspicionIsReversible(t *testing.T) {
	nodes, _ := chordRing(t)
	node, target := nodes[0], nodes[1]
	telemetry := NewTelemetry("0", "quiet", nil)
	controller := NewCoordinator(context.Background(), node, telemetry, 128)
	controller.Start()
	defer controller.Stop()
	before := node.GetRoutingSnapshot()
	controller.Submit(eventFor(target, Suspect, 0))
	after := node.GetRoutingSnapshot()
	if node.peerBlocked(target.Addr) || !samePeer(before.Successor, after.Successor) {
		t.Fatal("SUSPECT evicted a successor")
	}
	node.mu.RLock()
	dirty := len(node.dirty)
	node.mu.RUnlock()
	if dirty != 0 {
		t.Fatal("SUSPECT invalidated fingers")
	}
	controller.Submit(eventFor(target, Alive, 1))
	controller.Submit(eventFor(target, Suspect, 0))
	if !node.hintCurrent(&NodeRef{ID: target.ID, Addr: target.Addr}, 1, Alive) {
		t.Fatal("stale suspicion replaced refutation")
	}
	for key := int64(0); key < 256; key++ {
		owner, _, err := node.FindSuccessor(big.NewInt(key))
		if err != nil || owner == nil || owner.ID.Int64() != ((key+63)/64*64)%256 {
			t.Fatalf("unusable after refutation key %d owner %v err %v", key, owner, err)
		}
	}
}
func TestGossipMeshDeathRepairsOnlyAffectedReferencesAndDeduplicates(t *testing.T) {
	nodes, servers := chordRing(t)
	node, target := nodes[0], nodes[1]
	telemetry := NewTelemetry("0", "quiet", nil)
	controller := NewCoordinator(context.Background(), node, telemetry, 128)
	defer controller.Stop()
	before := node.GetRoutingSnapshot()
	refs := affected(before, &NodeRef{ID: target.ID, Addr: target.Addr})
	if !refs.Successor || len(refs.Fingers) < 2 {
		t.Fatal("fixture did not expose multiple references")
	}
	for i := 0; i < 500; i++ {
		controller.Submit(eventFor(target, Dead, 0))
	}
	if len(controller.pending) != 1 || telemetry.Snapshot()["membership_event_handled"] != 1 {
		t.Fatal("duplicate death events created work")
	}
	node.mu.RLock()
	dirty := make([]int, 0, len(node.dirty))
	for index := range node.dirty {
		dirty = append(dirty, index)
	}
	node.mu.RUnlock()
	sort.Ints(dirty)
	if len(dirty) != len(refs.Fingers) {
		t.Fatalf("not all affected fingers scheduled: %v", dirty)
	}
	servers[1].Close()
	controller.Start()
	await(t, func() bool { controller.mu.Lock(); defer controller.mu.Unlock(); return len(controller.pending) == 0 })
	after := node.GetRoutingSnapshot()
	if !samePeer(after.Successor, &NodeRef{ID: nodes[2].ID, Addr: nodes[2].Addr}) {
		t.Fatalf("successor not promoted: %v", after.Successor)
	}
	if affected(after, &NodeRef{ID: target.ID, Addr: target.Addr}).Count() != 0 {
		t.Fatal("dead references remain")
	}
	affectedIndex := map[int]bool{}
	for _, index := range refs.Fingers {
		affectedIndex[index] = true
	}
	for index, ref := range before.Fingers {
		if !affectedIndex[index] && !samePeer(ref, after.Fingers[index]) {
			t.Fatalf("unrelated finger %d changed", index)
		}
	}
	if telemetry.Snapshot()["successor_repair_validated"] == 0 || telemetry.Snapshot()["finger_repaired"] == 0 {
		t.Fatal("repair path not observable")
	}
}
func TestGossipMeshMembershipCallbackAndRestart(t *testing.T) {
	nodes, _ := chordRing(t)
	node, target := nodes[0], nodes[1]
	telemetry := NewTelemetry("0", "quiet", nil)
	controller := NewCoordinator(context.Background(), node, telemetry, 128)
	defer controller.Stop()
	cfg := testSWIMConfig()
	membership, err := NewMembership(&NodeRef{ID: node.ID, Addr: node.Addr}, 8, cfg, telemetry)
	if err != nil {
		t.Fatal(err)
	}
	membership.onEvent = controller.Submit
	peer := &NodeRef{ID: target.ID, Addr: target.Addr}
	mergeForTest(t, membership, Update{Peer: peer, State: Alive})
	mergeForTest(t, membership, Update{Peer: peer, State: Suspect})
	mergeForTest(t, membership, Update{Peer: peer, State: Dead})
	if !node.peerBlocked(target.Addr) {
		t.Fatal("real SWIM event did not reach Chord")
	}
	mergeForTest(t, membership, Update{Peer: peer, State: Alive, Incarnation: 1})
	mergeForTest(t, membership, Update{Peer: peer, State: Dead})
	if node.peerBlocked(target.Addr) {
		t.Fatal("stale tombstone blocked restarted peer")
	}
	owner, _, err := node.FindSuccessor(big.NewInt(32))
	if err != nil || owner == nil || owner.Addr != target.Addr {
		t.Fatal("restored liveness not usable")
	}
}
func TestGossipMeshRepairConcurrentWithMaintenance(t *testing.T) {
	nodes, servers := chordRing(t)
	node, target := nodes[0], nodes[1]
	telemetry := NewTelemetry("0", "quiet", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.transport.ctx = ctx
	controller := NewCoordinator(ctx, node, telemetry, 128)
	controller.Start()
	defer controller.Stop()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				node.Stabilize()
				node.FixFingers()
				node.CheckPredecessor()
			}
		}
	}()
	servers[1].Close()
	controller.Submit(eventFor(target, Dead, 0))
	await(t, func() bool {
		return affected(node.GetRoutingSnapshot(), &NodeRef{ID: target.ID, Addr: target.Addr}).Count() == 0
	})
	cancel()
	wg.Wait()
	if owner, _, err := node.FindSuccessor(big.NewInt(32)); err == nil && owner != nil && owner.Addr == target.Addr {
		t.Fatal("returned known dead owner")
	}
}
func TestConfigurationRejectsInvalidValues(t *testing.T) {
	for _, pair := range [][2]string{{"M", "0"}, {"NODE_ID", "-1"}, {"SWIM_INTERVAL_MS", "10"}, {"SWIM_MAX_PIGGYBACK", "0"}, {"SWIM_INDIRECT_HELPERS", "99"}, {"LOG_LEVEL", "unknown"}, {"SWIM_SUSPICION_TIMEOUT_MS", "1"}, {"PORT", "bad"}} {
		t.Run(pair[0], func(t *testing.T) {
			t.Setenv(pair[0], pair[1])
			if _, err := loadConfig(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestGossipMeshSuccessfulBatchesDoNotExhaustRetryBudget(t *testing.T) {
	nodes, servers := chordRing(t)
	node, target := nodes[0], nodes[1]
	for _, n := range nodes {
		original := n.FingerTable
		n.M = 48
		n.FingerTable = make([]*NodeRef, 48)
		copy(n.FingerTable, original)
		for index := 8; index < 48; index++ {
			n.FingerTable[index] = &NodeRef{ID: nodes[0].ID, Addr: nodes[0].Addr}
		}
	}
	for index := range node.FingerTable {
		node.FingerTable[index] = &NodeRef{ID: target.ID, Addr: target.Addr}
	}
	telemetry := NewTelemetry("0", "quiet", nil)
	controller := NewCoordinator(context.Background(), node, telemetry, 128)
	defer controller.Stop()
	servers[1].Close()
	controller.Submit(eventFor(target, Dead, 0))
	controller.Start()
	await(t, func() bool { controller.mu.Lock(); defer controller.mu.Unlock(); return len(controller.pending) == 0 })
	if affected(node.GetRoutingSnapshot(), &NodeRef{ID: target.ID, Addr: target.Addr}).Count() != 0 {
		t.Fatal("successful batches stopped after failure retry limit")
	}
	if telemetry.Snapshot()["finger_repaired"] < 40 {
		t.Fatal("large dirty set was not refreshed")
	}
}
func TestRuntimeStartsAndShutsDownAllWorkers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_, portText, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portText)
	listener.Close()
	cfg := &Config{Port: port, AdvertiseAddr: addr, NodeID: big.NewInt(0), M: 8, R: 4, StabilizeInterval: 20 * time.Millisecond, FixFingersInterval: 10 * time.Millisecond, CheckPredInterval: 20 * time.Millisecond, RPCTimeout: 40 * time.Millisecond, SWIM: testSWIMConfig(), LogLevel: "quiet"}
	cfg.SWIM.StateFile = t.TempDir() + "/incarnation.json"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- run(ctx, cfg) }()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	await(t, func() bool {
		response, e := client.Get("http://" + addr + "/health")
		if e != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop")
	}
	client.CloseIdleConnections()
	replacement, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("listener leaked:", err)
	}
	replacement.Close()
}

func TestGossipMeshFalseSuspicionThroughRealSWIMProbe(t *testing.T) {
	nodes, _ := chordRing(t)
	node, target := nodes[0], nodes[1]
	telemetry := NewTelemetry("0", "quiet", nil)
	controller := NewCoordinator(context.Background(), node, telemetry, 128)
	controller.Start()
	defer controller.Stop()
	network := &fakeSWIMNetwork{nodes: make(map[string]*SWIM), blocked: make(map[string]bool)}
	actors := make([]*SWIM, len(nodes))
	for index, n := range nodes {
		observer := NewTelemetry(n.ID.String(), "quiet", nil)
		m, err := NewMembership(&NodeRef{ID: n.ID, Addr: n.Addr}, n.M, testSWIMConfig(), observer)
		if err != nil {
			t.Fatal(err)
		}
		actors[index] = NewSWIM(m, network, observer)
		network.nodes[n.Addr] = actors[index]
	}
	actors[0].table.onEvent = controller.Submit
	for _, actor := range actors {
		for _, other := range actors {
			if actor != other {
				mergeForTest(t, actor.table, other.table.Self().Update)
			}
		}
	}
	network.blocked[target.Addr] = true
	snapshot, _ := actors[0].table.Get(target.ID.String())
	if actors[0].Probe(context.Background(), snapshot) {
		t.Fatal("dropped SWIM traffic acknowledged")
	}
	member, _ := actors[0].table.Get(target.ID.String())
	if member.State != Suspect {
		t.Fatal("did not suspect target")
	}
	if node.peerBlocked(target.Addr) || !samePeer(node.GetRoutingSnapshot().Successor, &NodeRef{ID: target.ID, Addr: target.Addr}) {
		t.Fatal("false suspicion evicted live Chord successor")
	}
	delete(network.blocked, target.Addr)
	member, _ = actors[0].table.Get(target.ID.String())
	if !actors[0].Probe(context.Background(), member) {
		t.Fatal("live target did not refute")
	}
	current, _ := actors[0].table.Get(target.ID.String())
	if current.State != Alive || current.Incarnation == 0 || node.peerBlocked(target.Addr) {
		t.Fatal("refutation did not clear routing hint")
	}
	owner, _, err := node.FindSuccessor(big.NewInt(32))
	if err != nil || owner == nil || owner.Addr != target.Addr {
		t.Fatal("routing unusable after false suspicion")
	}
}

func TestGossipMeshInFlightRepairRespectsNewerMembership(t *testing.T) {
	for _, refute := range []bool{true, false} {
		t.Run(fmt.Sprint(refute), func(t *testing.T) {
			node, _ := chordFixture(t, 0)
			failed := &NodeRef{ID: big.NewInt(64), Addr: "127.0.0.1:24564"}
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
				w.WriteHeader(200)
			}))
			defer server.Close()
			defer once.Do(func() { close(release) })
			candidate := &NodeRef{ID: big.NewInt(128), Addr: server.Listener.Addr().String()}
			node.transport.client.Timeout = time.Second
			node.mu.Lock()
			node.setSuccessorLocked(failed)
			node.SuccessorList = []*NodeRef{failed, candidate}
			node.mu.Unlock()
			node.SetPeerMembershipHint(Member{Update: Update{Peer: failed, State: Dead}})
			finished := make(chan struct{})
			go func() {
				node.handleSuccessorFailureIf(failed, func() bool { return node.hintCurrent(failed, 0, Dead) })
				close(finished)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("candidate ping never started")
			}
			if refute {
				node.SetPeerMembershipHint(Member{Update: Update{Peer: failed, State: Alive, Incarnation: 1}})
			} else {
				node.SetPeerMembershipHint(Member{Update: Update{Peer: candidate, State: Dead}})
			}
			once.Do(func() { close(release) })
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("repair did not finish")
			}
			snapshot := node.GetRoutingSnapshot()
			if refute && !samePeer(snapshot.Successor, failed) {
				t.Fatal("stale DEAD job evicted a refuted/alive successor")
			}
			if !refute && samePeer(snapshot.Successor, candidate) {
				t.Fatal("installed a candidate whose DEAD hint arrived during its ping")
			}
		})
	}
}

func TestUnrelatedMembershipDoesNotScheduleRepairs(t *testing.T) {
	node, _ := chordFixture(t, 0)
	target, _ := chordFixture(t, 64)
	c := NewCoordinator(context.Background(), node, NewTelemetry("0", "quiet", nil), 128)
	defer c.Stop()
	for _, state := range []MemberState{Alive, Suspect, Dead} {
		c.Submit(eventFor(target, state, 0))
		if len(c.pending) != 0 {
			t.Fatal("unrelated membership scheduled network work")
		}
	}
	if !node.peerBlocked(target.Addr) {
		t.Fatal("skipped job lost DEAD hint")
	}
	c.Submit(eventFor(target, Alive, 1))
	if node.peerBlocked(target.Addr) || len(c.pending) != 0 {
		t.Fatal("rejoin hint not applied without work")
	}
}

func TestQueuedDeathSupersededByAliveRetiresDirtySuccessor(t *testing.T) {
	node, _ := chordFixture(t, 0)
	target, _ := chordFixture(t, 64)
	node.mu.Lock()
	node.setSuccessorLocked(&NodeRef{ID: target.ID, Addr: target.Addr})
	node.mu.Unlock()
	c := NewCoordinator(context.Background(), node, NewTelemetry("0", "quiet", nil), 128)
	defer c.Stop()
	c.Submit(eventFor(target, Dead, 0))
	c.Submit(eventFor(target, Alive, 1))
	c.Start()
	await(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) == 0 })
	if node.hasDirtyReferences("64") || node.peerBlocked(target.Addr) {
		t.Fatal("refutation left stale work")
	}
}
func TestPredecessorCheckSharesOnlyFreshAcknowledgement(t *testing.T) {
	node, _ := chordFixture(t, 0)
	target, server := chordFixture(t, 64)
	peer := &NodeRef{ID: target.ID, Addr: target.Addr}
	table := testMembership(t, 0)
	now := time.Unix(100, 0)
	table.now = func() time.Time { return now }
	table.Acknowledge(Update{Peer: peer, State: Alive})
	node.transport.swim = NewSWIM(table, nil, table.telemetry)
	node.mu.Lock()
	node.Predecessor = peer
	node.mu.Unlock()
	server.Close()
	node.CheckPredecessor()
	if node.GetRoutingSnapshot().Predecessor == nil {
		t.Fatal("fresh ACK not reused")
	}
	now = now.Add(table.cfg.Interval)
	node.CheckPredecessor()
	if node.GetRoutingSnapshot().Predecessor != nil {
		t.Fatal("expired ACK suppressed failed check")
	}
}

func TestBackupValidationIsRetriedAfterDeadReferenceRemoved(t *testing.T) {
	node, _ := chordFixture(t, 0)
	var pings int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rpc/ping" && atomic.AddInt32(&pings, 1) == 1 {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/rpc/predecessor_and_list" {
			json.NewEncoder(w).Encode(PredAndListResp{M: 8})
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	healthy := &NodeRef{ID: big.NewInt(64), Addr: server.Listener.Addr().String()}
	failed := &ChordNode{ID: big.NewInt(128), Addr: "127.0.0.1:24128"}
	node.mu.Lock()
	node.setSuccessorLocked(healthy)
	node.SuccessorList = []*NodeRef{healthy, {ID: failed.ID, Addr: failed.Addr}}
	node.mu.Unlock()
	c := NewCoordinator(context.Background(), node, NewTelemetry("0", "quiet", nil), 128)
	defer c.Stop()
	c.Submit(eventFor(failed, Dead, 0))
	c.Start()
	await(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) == 0 })
	if atomic.LoadInt32(&pings) < 2 {
		t.Fatal("removed reference lost outstanding backup validation")
	}
	if c.failures != 0 || c.completed != 1 {
		t.Fatal("backup retry failed")
	}
}
