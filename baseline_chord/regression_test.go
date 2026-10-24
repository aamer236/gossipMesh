package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testNode(t *testing.T, id int64, r int) (*ChordNode, *httptest.Server) {
	t.Helper()
	metrics := NewMetricsCollector()
	tr := NewNetworkTransport(100*time.Millisecond, metrics)
	n := NewChordNode(big.NewInt(id), "pending", 8, r, tr, metrics)
	tr.SetNode(n)
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc/find_successor", tr.handleFindSuccessor)
	mux.HandleFunc("/rpc/predecessor_and_list", tr.handlePredecessorAndList)
	mux.HandleFunc("/rpc/notify", tr.handleNotify)
	mux.HandleFunc("/rpc/ping", tr.handlePing)
	s := httptest.NewServer(mux)
	n.Addr = strings.TrimPrefix(s.URL, "http://")
	n.Join("")
	t.Cleanup(s.Close)
	return n, s
}

func TestRegressionExhaustiveHealthyAndCrash(t *testing.T) {
	var nodes []*ChordNode
	var servers []*httptest.Server
	for i := 0; i < 8; i++ {
		n, s := testNode(t, int64(i*32), 4)
		if i > 0 {
			if err := n.Join(nodes[0].Addr); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
		servers = append(servers, s)
	}
	check := func(label string, live []*ChordNode) {
		t.Helper()
		wrong := 0
		for _, n := range live {
			for k := 0; k < 256; k++ {
				want := live[0].ID.Int64()
				for _, v := range live {
					if v.ID.Int64() >= int64(k) {
						want = v.ID.Int64()
						break
					}
				}
				got, _, err := n.FindSuccessor(big.NewInt(int64(k)))
				if err != nil || got == nil || got.ID.Int64() != want {
					wrong++
					if wrong < 4 {
						t.Logf("%s source=%s key=%d got=%v err=%v want=%d", label, n.ID, k, got, err, want)
					}
				}
			}
		}
		if wrong > 0 {
			t.Errorf("%s: %d incorrect lookups", label, wrong)
		} else {
			t.Logf("%s: %d correct lookups", label, len(live)*256)
		}
	}
	for round := 0; round < 32; round++ {
		for _, n := range nodes {
			n.CheckPredecessor()
			n.Stabilize()
			n.FixFingers()
		}
	}
	check("healthy", nodes)
	servers[1].Close()
	servers[2].Close()
	live := append([]*ChordNode{nodes[0]}, nodes[3:]...)
	for round := 0; round < 32; round++ {
		for _, n := range live {
			n.CheckPredecessor()
			n.Stabilize()
			n.FixFingers()
		}
	}
	check("two adjacent crashes", live)
}

func TestRegressionExhaustedSuccessorsIgnoreLiveFingers(t *testing.T) {
	a, _ := testNode(t, 0, 2)
	b, sb := testNode(t, 32, 2)
	c, sc := testNode(t, 64, 2)
	d, _ := testNode(t, 128, 2)
	a.Successor = &NodeRef{ID: b.ID, Addr: b.Addr}
	a.SuccessorList = []*NodeRef{a.Successor, {ID: c.ID, Addr: c.Addr}}
	a.FingerTable[7] = &NodeRef{ID: d.ID, Addr: d.Addr}
	d.Successor = &NodeRef{ID: a.ID, Addr: a.Addr}
	d.SuccessorList = []*NodeRef{d.Successor}
	sb.Close()
	sc.Close()
	got, _, err := a.FindSuccessor(big.NewInt(16))
	if err == nil && got.ID.Int64() != 128 {
		t.Fatalf("wrong successful owner=%s; want live finger 128 or explicit routing error", got.ID)
	}
}

func TestRegressionFailedRoutingMustNotSucceed(t *testing.T) {
	n, _ := testNode(t, 0, 4)
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer dead.Close()
	ref := &NodeRef{ID: big.NewInt(32), Addr: strings.TrimPrefix(dead.URL, "http://")}
	n.Successor = ref
	n.SuccessorList = []*NodeRef{ref}
	n.FingerTable[0] = ref
	got, _, err := n.FindSuccessor(big.NewInt(100))
	if err == nil {
		t.Fatalf("all remote routes failed but lookup returned owner=%s with nil error", got.ID)
	}
}

func TestRegressionListenerReportsBindFailure(t *testing.T) {
	n, s := testNode(t, 0, 4)
	var port int
	fmt.Sscanf(strings.TrimPrefix(s.URL, "http://127.0.0.1:"), "%d", &port)
	if err := n.transport.StartListener(port); err == nil {
		n.transport.StopListener()
		t.Fatal("StartListener returned nil for an occupied port")
	}
}

func TestRegressionRejectInvalidNodeID(t *testing.T) {
	t.Setenv("M", "8")
	t.Setenv("NODE_ID", "256")
	if cfg, err := loadConfig(); err == nil {
		t.Fatalf("accepted out-of-range ID %s in m=%d space", cfg.NodeID, cfg.M)
	}
}

func TestRegressionNotifyWithoutID(t *testing.T) {
	n, _ := testNode(t, 10, 4)
	n.Predecessor = &NodeRef{ID: big.NewInt(0), Addr: "pred"}
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("malformed notify panicked: %v", p)
		}
	}()
	rr := httptest.NewRecorder()
	n.transport.handleNotify(rr, httptest.NewRequest("POST", "/rpc/notify", strings.NewReader(`{"m":8,"candidate":{"addr":"bad"}}`)))
	if rr.Code == 200 {
		t.Error("accepted candidate with missing ID")
	}
}

func TestRegressionInboundBytesCounted(t *testing.T) {
	n, s := testNode(t, 10, 4)
	caller := NewNetworkTransport(time.Second, NewMetricsCollector())
	caller.SetNode(n)
	if err := caller.NotifyRPC(n.Addr, &NodeRef{ID: big.NewInt(1), Addr: "peer"}); err != nil {
		t.Fatal(err)
	}
	_ = s
	var data map[string]interface{}
	rr := httptest.NewRecorder()
	n.transport.handleMetrics(rr, httptest.NewRequest("GET", "/metrics", nil))
	json.Unmarshal(rr.Body.Bytes(), &data)
	network := data["network"].(map[string]interface{})
	if network["bytes_received"].(float64) == 0 || network["bytes_sent"].(float64) == 0 {
		t.Fatalf("handled real RPC but byte counters=%v", network)
	}
}
