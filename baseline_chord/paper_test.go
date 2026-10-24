package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"
)

func oracle(nodes []*ChordNode, key int64) *ChordNode {
	for _, n := range nodes {
		if n.ID.Int64() >= key {
			return n
		}
	}
	return nodes[0]
}

func assertTables(nodes []*ChordNode) error {
	for i, n := range nodes {
		state := n.GetState()
		succ := state["successor"]
		if succ == nil || succ.(map[string]interface{})["addr"] != nodes[(i+1)%len(nodes)].Addr {
			return fmt.Errorf("%s successor", n.ID)
		}
		if len(nodes) > 1 {
			pred := state["predecessor"]
			if pred == nil || pred.(map[string]interface{})["addr"] != nodes[(i+len(nodes)-1)%len(nodes)].Addr {
				return fmt.Errorf("%s predecessor", n.ID)
			}
		}
		fingers := state["fingers"].([]map[string]interface{})
		if len(fingers) != n.M {
			return fmt.Errorf("%s incomplete fingers", n.ID)
		}
		for _, finger := range fingers {
			start := FingerStart(n.ID, finger["index"].(int), n.M).Int64()
			if finger["addr"] != oracle(nodes, start).Addr {
				return fmt.Errorf("%s finger %v", n.ID, finger["index"])
			}
		}
		list := state["successor_list"].([]map[string]interface{})
		want := n.R
		if want > len(nodes)-1 {
			want = len(nodes) - 1
		}
		if len(nodes) == 1 {
			want = 1
		}
		if len(list) != want {
			return fmt.Errorf("%s list length %d, want %d", n.ID, len(list), want)
		}
		for offset, ref := range list {
			if ref["addr"] != nodes[(i+offset+1)%len(nodes)].Addr {
				return fmt.Errorf("%s backup %d", n.ID, offset)
			}
		}
	}
	return nil
}

func converge(t *testing.T, nodes []*ChordNode) {
	t.Helper()
	for round := 0; round < 600; round++ {
		for _, n := range nodes {
			n.CheckPredecessor()
			n.Stabilize()
			n.FixFingers()
		}
		if assertTables(nodes) == nil {
			return
		}
	}
	t.Fatalf("did not converge: %v", assertTables(nodes))
}

func TestPaperConcurrentJoinsAndFailureRecovery(t *testing.T) {
	for _, size := range []int{8, 16, 32} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(size)))
			ids := rng.Perm(256)[:size]
			nodes := make([]*ChordNode, size)
			servers := make([]*httptest.Server, size)
			for i, id := range ids {
				nodes[i], servers[i] = testNode(t, int64(id), 4)
			}
			var wg sync.WaitGroup
			for _, n := range nodes[1:] {
				wg.Add(1)
				go func(n *ChordNode) {
					defer wg.Done()
					if err := n.Join(nodes[0].Addr); err != nil {
						t.Errorf("join: %v", err)
					}
				}(n)
			}
			wg.Wait()
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID.Cmp(nodes[j].ID) < 0 })
			converge(t, nodes)
			verify := func(live []*ChordNode) {
				t.Helper()
				count := 0
				maxHops := 0
				sumHops := 0
				for _, n := range live {
					for k := int64(0); k < 256; k++ {
						got, hops, err := n.FindSuccessor(big.NewInt(k))
						if err != nil || got == nil || got.Addr != oracle(live, k).Addr {
							t.Fatalf("source %s key %d owner %v err %v", n.ID, k, got, err)
						}
						if hops > maxHops {
							maxHops = hops
						}
						sumHops += hops
						count++
					}
				}
				t.Logf("%d correct lookups; mean hops %.2f, max %d", count, float64(sumHops)/float64(count), maxHops)
			}
			verify(nodes)
			// Fail two adjacent successors, preserving a connected ring and backups.
			dead := map[string]bool{nodes[1].Addr: true, nodes[2].Addr: true}
			for _, s := range servers {
				if dead[s.Listener.Addr().String()] {
					s.Close()
				}
			}
			live := make([]*ChordNode, 0, size-2)
			for _, n := range nodes {
				if !dead[n.Addr] {
					live = append(live, n)
				}
			}
			converge(t, live)
			verify(live)
		})
	}
}

func TestPaperSuccessorAndFingerZeroStayTogether(t *testing.T) {
	a, _ := testNode(t, 0, 4)
	b, _ := testNode(t, 128, 4)
	if err := b.Join(a.Addr); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 6; round++ {
		a.Stabilize()
		b.Stabilize()
	}
	for _, n := range []*ChordNode{a, b} {
		if n.Successor.Addr != n.FingerTable[0].Addr {
			t.Fatal("successor differs from finger zero")
		}
	}
}

func TestPaperNewSuccessorKeepsOldSuccessorAsBackup(t *testing.T) {
	a, _ := testNode(t, 0, 4)
	b, _ := testNode(t, 32, 4)
	c, _ := testNode(t, 64, 4)
	d, _ := testNode(t, 128, 4)
	ref := func(n *ChordNode) *NodeRef { return &NodeRef{ID: n.ID, Addr: n.Addr} }
	a.Successor = ref(c)
	a.SuccessorList = []*NodeRef{ref(c), ref(d)}
	c.Predecessor = ref(b)
	c.Successor = ref(d)
	c.SuccessorList = []*NodeRef{ref(d), ref(a)}
	b.Successor = ref(c)
	b.SuccessorList = []*NodeRef{ref(c), ref(d), ref(a)}
	a.Stabilize()
	if a.Successor.Addr != b.Addr || len(a.SuccessorList) < 2 || a.SuccessorList[1].Addr != c.Addr {
		t.Fatalf("lost immediate backup: %v", a.GetState())
	}
}

func TestPaperDuplicateIDAndMixedSpacesRejected(t *testing.T) {
	a, _ := testNode(t, 32, 4)
	b, _ := testNode(t, 32, 4)
	if err := b.Join(a.Addr); err == nil {
		t.Fatal("accepted duplicate ID")
	}
	b.M = 7
	if err := b.Join(a.Addr); err == nil {
		t.Fatal("accepted mixed identifier spaces")
	}
}

func TestPaperLostContactIsNotSingleton(t *testing.T) {
	a, _ := testNode(t, 0, 4)
	b, s := testNode(t, 128, 4)
	if err := b.Join(a.Addr); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		a.Stabilize()
		b.Stabilize()
	}
	s.Close()
	a.CheckPredecessor()
	a.Stabilize()
	for i := 0; i < 3; i++ {
		if owner, _, err := a.FindSuccessor(big.NewInt(64)); err == nil {
			t.Fatalf("lost contact but claimed owner %v", owner)
		}
	}
}

func TestPaperLookupRejectsCycle(t *testing.T) {
	a, _ := testNode(t, 0, 4)
	rr := httptest.NewRecorder()
	body := FindSuccessorReq{ID: "10", M: 8, Path: []string{a.Addr}}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/rpc/find_successor", bytes.NewReader(raw))
	a.transport.handleFindSuccessor(rr, req)
	if rr.Code == 200 {
		t.Fatal("cyclic lookup succeeded")
	}
}

func TestPaperConcurrentMaintenanceAndLookups(t *testing.T) {
	var nodes []*ChordNode
	for _, id := range []int64{0, 32, 64, 128} {
		n, _ := testNode(t, id, 4)
		if len(nodes) > 0 {
			if err := n.Join(nodes[0].Addr); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
	}
	converge(t, nodes)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *ChordNode) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					n.Stabilize()
					n.CheckPredecessor()
					n.FixFingers()
				}
			}
		}(n)
	}
	for _, n := range nodes {
		for k := int64(0); k < 256; k++ {
			got, _, err := n.FindSuccessor(big.NewInt(k))
			if err != nil || got == nil || got.Addr != oracle(nodes, k).Addr {
				t.Errorf("concurrent lookup key=%d got=%v err=%v", k, got, err)
			}
		}
	}
	close(stop)
	wg.Wait()
}

func TestPaperFailedProbeDoesNotHoldStateLock(t *testing.T) {
	a, _ := testNode(t, 0, 4)
	entered := make(chan struct{})
	release := make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(503) }))
	defer blocked.Close()
	a.Successor = &NodeRef{ID: big.NewInt(32), Addr: "failed"}
	a.SuccessorList = []*NodeRef{a.Successor, {ID: big.NewInt(64), Addr: blocked.Listener.Addr().String()}}
	done := make(chan struct{})
	go func() { a.handleSuccessorFailure(a.Successor); close(done) }()
	<-entered
	read := make(chan struct{})
	go func() { a.GetState(); close(read) }()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Error("network wait held state lock")
	}
	close(release)
	<-done
}

func TestPaperEndpointRestoreAndRejoin(t *testing.T) {
	var nodes []*ChordNode
	var crashed *httptest.Server
	for i := 0; i < 8; i++ {
		n, s := testNode(t, int64(i*32), 4)
		if i > 0 {
			if err := n.Join(nodes[0].Addr); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
		if i == 3 {
			crashed = s
		}
	}
	converge(t, nodes)
	crashed.Close()
	live := append(append([]*ChordNode(nil), nodes[:3]...), nodes[4:]...)
	converge(t, live)
	listener, err := net.Listen("tcp", nodes[3].Addr)
	if err != nil {
		t.Fatal(err)
	}
	restored := &httptest.Server{Listener: listener, Config: &http.Server{Handler: crashed.Config.Handler}}
	restored.Start()
	t.Cleanup(restored.Close)
	// Restore the saved state first, then exercise explicit lazy rejoin too.
	converge(t, nodes)
	if err := nodes[3].Join(nodes[0].Addr); err != nil {
		t.Fatal(err)
	}
	converge(t, nodes)
	for _, n := range nodes {
		for k := int64(0); k < 256; k++ {
			got, _, err := n.FindSuccessor(big.NewInt(k))
			if err != nil || got == nil || got.Addr != oracle(nodes, k).Addr {
				t.Fatalf("restored lookup key=%d got=%v err=%v", k, got, err)
			}
		}
	}
}

func TestNearestRankPercentiles(t *testing.T) {
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i + 1)
	}
	a, b, c := calculatePercentiles(data)
	if a != 50 || b != 95 || c != 99 {
		t.Fatalf("percentiles %v %v %v", a, b, c)
	}
}
