package main

import (
	"math/big"
	"testing"
	"time"
)

func TestInInterval(t *testing.T) {
	m := 3 // Ring 0..7
	// (1, 5) non-inclusive -> 2, 3, 4
	if !InInterval(big.NewInt(3), big.NewInt(1), big.NewInt(5), false, false, m) {
		t.Errorf("Expected 3 in (1, 5)")
	}
	if InInterval(big.NewInt(1), big.NewInt(1), big.NewInt(5), false, false, m) {
		t.Errorf("Expected 1 NOT in (1, 5)")
	}
	if InInterval(big.NewInt(5), big.NewInt(1), big.NewInt(5), false, false, m) {
		t.Errorf("Expected 5 NOT in (1, 5)")
	}
	if !InInterval(big.NewInt(5), big.NewInt(1), big.NewInt(5), false, true, m) {
		t.Errorf("Expected 5 in (1, 5]")
	}
	if !InInterval(big.NewInt(1), big.NewInt(1), big.NewInt(5), true, false, m) {
		t.Errorf("Expected 1 in [1, 5)")
	}

	// Wraparound: (6, 2) -> 7, 0, 1
	if !InInterval(big.NewInt(7), big.NewInt(6), big.NewInt(2), false, false, m) {
		t.Errorf("Expected 7 in (6, 2)")
	}
	if !InInterval(big.NewInt(0), big.NewInt(6), big.NewInt(2), false, false, m) {
		t.Errorf("Expected 0 in (6, 2)")
	}
	if InInterval(big.NewInt(4), big.NewInt(6), big.NewInt(2), false, false, m) {
		t.Errorf("Expected 4 NOT in (6, 2)")
	}

	// Full turn: (3, 3)
	if !InInterval(big.NewInt(5), big.NewInt(3), big.NewInt(3), false, false, m) {
		t.Errorf("Expected 5 in (3, 3)")
	}
	if InInterval(big.NewInt(3), big.NewInt(3), big.NewInt(3), false, false, m) {
		t.Errorf("Expected 3 NOT in (3, 3)")
	}
	if !InInterval(big.NewInt(3), big.NewInt(3), big.NewInt(3), false, true, m) {
		t.Errorf("Expected 3 in (3, 3]")
	}
}

func TestFingerStart(t *testing.T) {
	m := 6 // 0..63
	nodeID := big.NewInt(10)
	// finger 0: (10 + 1) mod 64 = 11
	f0 := FingerStart(nodeID, 0, m)
	if f0.Int64() != 11 {
		t.Errorf("Finger 0 expected 11, got %d", f0.Int64())
	}
	// finger 5: (10 + 32) mod 64 = 42
	f5 := FingerStart(nodeID, 5, m)
	if f5.Int64() != 42 {
		t.Errorf("Finger 5 expected 42, got %d", f5.Int64())
	}
}

func TestLocalRingIntegration(t *testing.T) {
	m := 16
	r := 4

	// Start 3 nodes on local ports
	ports := []int{19001, 19002, 19003}
	nodes := make([]*ChordNode, len(ports))
	transports := make([]*NetworkTransport, len(ports))

	for i, p := range ports {
		metrics := NewMetricsCollector()
		tr := NewNetworkTransport(500*time.Millisecond, metrics)
		id := big.NewInt(int64((i + 1) * 1000))
		addr := trClientAddr(p)
		n := NewChordNode(id, addr, m, r, tr, metrics)
		tr.SetNode(n)
		_ = tr.StartListener(p)
		nodes[i] = n
		transports[i] = tr
	}
	defer func() {
		for _, tr := range transports {
			tr.StopListener()
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Node 0 creates ring
	_ = nodes[0].Join("")

	// Node 1 joins via Node 0
	err := nodes[1].Join(nodes[0].Addr)
	if err != nil {
		t.Fatalf("Node 1 join failed: %v", err)
	}

	// Node 2 joins via Node 0
	err = nodes[2].Join(nodes[0].Addr)
	if err != nil {
		t.Fatalf("Node 2 join failed: %v", err)
	}

	// Run stabilization rounds
	for round := 0; round < 6; round++ {
		for _, n := range nodes {
			n.Stabilize()
			n.FixFingers()
		}
	}

	// Verify lookup for a key
	succ, hops, err := nodes[0].FindSuccessor(big.NewInt(1500))
	if err != nil {
		t.Fatalf("FindSuccessor failed: %v", err)
	}
	// Key 1500 should be owned by Node 2 (ID=2000)
	if succ.ID.Int64() != 2000 {
		t.Errorf("Expected successor 2000 for key 1500, got %s", succ.ID.String())
	}
	if hops < 1 {
		t.Errorf("Expected at least 1 hop, got %d", hops)
	}
}

func TestFailureRecovery(t *testing.T) {
	m := 16
	r := 4

	// Ring of 4 nodes: 1000, 2000, 3000, 4000
	ports := []int{19011, 19012, 19013, 19014}
	nodes := make([]*ChordNode, len(ports))
	transports := make([]*NetworkTransport, len(ports))

	for i, p := range ports {
		metrics := NewMetricsCollector()
		tr := NewNetworkTransport(200*time.Millisecond, metrics)
		id := big.NewInt(int64((i + 1) * 1000))
		addr := trClientAddr(p)
		n := NewChordNode(id, addr, m, r, tr, metrics)
		tr.SetNode(n)
		_ = tr.StartListener(p)
		nodes[i] = n
		transports[i] = tr
	}
	defer func() {
		for _, tr := range transports {
			tr.StopListener()
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Ring formation
	_ = nodes[0].Join("")
	for i := 1; i < len(nodes); i++ {
		_ = nodes[i].Join(nodes[0].Addr)
	}

	// Stabilize
	for round := 0; round < 8; round++ {
		for _, n := range nodes {
			n.Stabilize()
			n.FixFingers()
		}
	}

	// Now abruptly stop Node 1 (ID=2000)
	transports[1].StopListener()

	// Query for key 1500 starting from Node 0 (whose immediate successor was Node 1)
	// Section 5.2: Should fall back to successor list and return Node 2 (ID=3000)
	succ, _, err := nodes[0].FindSuccessor(big.NewInt(1500))
	if err != nil {
		t.Fatalf("FindSuccessor failed after node failure: %v", err)
	}
	if succ.ID.Int64() != 3000 {
		t.Errorf("Expected live successor 3000 after 2000 died, got %s", succ.ID.String())
	}
}

func trClientAddr(port int) string {
	return "127.0.0.1:" + big.NewInt(int64(port)).String()
}
