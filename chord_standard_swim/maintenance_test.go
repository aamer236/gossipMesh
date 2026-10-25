package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func maintenanceConfig() *Config {
	return &Config{StabilizeInterval: 20 * time.Millisecond, FixFingersInterval: 10 * time.Millisecond, CheckPredInterval: 30 * time.Millisecond,
		Adaptive: AdaptiveConfig{Enabled: true, StableAfter: time.Second, MaxStabilizeInterval: 100 * time.Millisecond, MaxFixFingersInterval: 80 * time.Millisecond, MaxCheckPredInterval: 150 * time.Millisecond}}
}
func TestMaintenanceBackoffIsQuietBoundedAndInterrupted(t *testing.T) {
	now := time.Unix(100, 0)
	p := NewMaintenancePolicy(maintenanceConfig(), NewTelemetry("0", "quiet", nil), now)
	snapshot := RoutingSnapshot{Successor: testPeer(1), Predecessor: testPeer(2), Successors: []*NodeRef{testPeer(1)}, Fingers: []*NodeRef{testPeer(1)}}
	if p.Interval(0, now.Add(time.Hour)) != 20*time.Millisecond {
		t.Fatal("backoff before observation")
	}
	p.Observe(now, snapshot, "")
	if p.Interval(0, now.Add(time.Second-time.Nanosecond)) != 20*time.Millisecond {
		t.Fatal("early backoff")
	}
	if p.Interval(0, now.Add(time.Second)) != 40*time.Millisecond {
		t.Fatal("no first backoff")
	}
	if p.Interval(0, now.Add(2*time.Second)) != 80*time.Millisecond {
		t.Fatal("no second backoff")
	}
	if p.Interval(0, now.Add(time.Hour)) != 100*time.Millisecond {
		t.Fatal("cap exceeded")
	}
	// Equal peer values in new pointers must not reset a quiet period.
	snapshot.Successor = clonePeer(snapshot.Successor)
	p.Observe(now.Add(2*time.Second), snapshot, "")
	if p.Interval(0, now.Add(2*time.Second)) != 80*time.Millisecond {
		t.Fatal("pointer change interpreted as topology churn")
	}
	for _, reason := range []string{"peer_added", "peer_dead", "peer_refuted", "chord_rpc_failure"} {
		p.Activity(now.Add(3*time.Second), reason)
		if p.Interval(0, now.Add(3*time.Second)) != 20*time.Millisecond {
			t.Fatalf("%s did not accelerate", reason)
		}
	}
	p.Observe(now.Add(4*time.Second), snapshot, "pending_repair")
	if p.Interval(0, now.Add(time.Hour)) != 20*time.Millisecond {
		t.Fatal("backoff despite repair blocker")
	}
	p.Observe(now.Add(5*time.Second), snapshot, "")
	if p.Interval(0, now.Add(5*time.Second)) != 20*time.Millisecond {
		t.Fatal("quiet period did not resume after blocker")
	}
	snapshot.Fingers[0] = testPeer(3)
	p.Observe(now.Add(6*time.Second), snapshot, "")
	if p.Interval(0, now.Add(6*time.Second)) != 20*time.Millisecond {
		t.Fatal("routing change did not accelerate")
	}
}
func TestMaintenanceBlockers(t *testing.T) {
	node, _ := chordFixture(t, 0)
	table := testMembership(t, 0)
	c := NewCoordinator(context.Background(), node, table.telemetry, 128)
	defer c.Stop()
	snapshot := node.GetRoutingSnapshot()
	if got := maintenanceBlocker(node, table, c, snapshot); got != "" {
		t.Fatal(got)
	}
	node.mu.Lock()
	node.dirty = map[int]string{1: "2"}
	node.mu.Unlock()
	if got := maintenanceBlocker(node, table, c, snapshot); got != "dirty_references" {
		t.Fatal(got)
	}
	node.mu.Lock()
	delete(node.dirty, 1)
	node.mu.Unlock()
	c.mu.Lock()
	c.pending["2"] = repairJob{}
	c.mu.Unlock()
	if got := maintenanceBlocker(node, table, c, snapshot); got != "pending_repair" {
		t.Fatal(got)
	}
	c.mu.Lock()
	delete(c.pending, "2")
	c.mu.Unlock()
	mergeForTest(t, table, Update{Peer: testPeer(2), State: Suspect})
	if got := maintenanceBlocker(node, table, c, snapshot); got != "suspected_peer" {
		t.Fatal(got)
	}
	mergeForTest(t, table, Update{Peer: testPeer(2), State: Alive, Incarnation: 1})
	snapshot.Fingers[1] = nil
	if got := maintenanceBlocker(node, table, c, snapshot); got != "incomplete_or_dead_reference" {
		t.Fatal(got)
	}
}
func TestMaintenanceBroadcastWakesAllWorkersAndStops(t *testing.T) {
	cfg := maintenanceConfig()
	cfg.Adaptive.MaxStabilizeInterval = 3 * time.Second
	cfg.Adaptive.MaxFixFingersInterval = 3 * time.Second
	cfg.Adaptive.MaxCheckPredInterval = 3 * time.Second
	p := NewMaintenancePolicy(cfg, NewTelemetry("0", "quiet", nil), time.Now().Add(-time.Hour))
	p.Observe(time.Now().Add(-time.Hour), RoutingSnapshot{}, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	calls := make(chan int, 12)
	var running [3]int32
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.RunTask(ctx, i, func() {
				if atomic.AddInt32(&running[i], 1) != 1 {
					t.Error("maintenance overlapped itself")
				}
				calls <- i
				atomic.AddInt32(&running[i], -1)
			}, nil)
		}(i)
	}
	select {
	case <-calls:
		t.Fatal("slow task ran early")
	case <-time.After(40 * time.Millisecond):
	}
	for i := 0; i < 1000; i++ {
		p.Activity(time.Now(), "peer_dead")
	}
	seen := map[int]bool{}
	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	for len(seen) < 3 {
		select {
		case i := <-calls:
			seen[i] = true
		case <-deadline.C:
			t.Fatal("not all slow workers accelerated")
		}
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not stop")
	}
}
func TestAdaptiveConfigurationValidation(t *testing.T) {
	for _, setting := range [][2]string{{"ADAPTIVE_MAINTENANCE", "bad"}, {"ADAPTIVE_STABLE_AFTER_MS", "0"}, {"ADAPTIVE_MAX_STABILIZE_MS", "0"}} {
		t.Run(setting[0], func(t *testing.T) {
			t.Setenv(setting[0], setting[1])
			if _, err := loadConfig(); err == nil {
				t.Fatal("invalid adaptive config accepted")
			}
		})
	}
	t.Setenv("ADAPTIVE_MAINTENANCE", "true")
	t.Setenv("ADAPTIVE_MAX_STABILIZE_MS", "1")
	if _, err := loadConfig(); err == nil {
		t.Fatal("maximum below fast interval accepted")
	}
}

func TestMaintenanceIgnoresStaleObservationsAfterActivity(t *testing.T) {
	now := time.Unix(100, 0)
	p := NewMaintenancePolicy(maintenanceConfig(), NewTelemetry("0", "quiet", nil), now)
	s := RoutingSnapshot{Successor: testPeer(1)}
	p.Observe(now, s, "")
	p.Activity(now.Add(10*time.Second), "peer_dead")
	p.Observe(now.Add(time.Second), RoutingSnapshot{}, "dirty_references")
	p.Activity(now.Add(time.Second), "old_activity")
	if p.Interval(0, now.Add(10*time.Second)) != 20*time.Millisecond {
		t.Fatal("old observation moved quiet period backward")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastReason != "peer_dead" {
		t.Fatal("stale observation replaced latest activity")
	}
}

func TestSlowMaintenanceDoesNotBlockOtherWorkers(t *testing.T) {
	cfg := maintenanceConfig()
	p := NewMaintenancePolicy(cfg, NewTelemetry("0", "quiet", nil), time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	others := make(chan int, 10)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); p.RunTask(ctx, 0, func() { close(entered); <-ctx.Done() }, nil) }()
	for _, index := range []int{1, 2} {
		go func(index int) {
			defer wg.Done()
			p.RunTask(ctx, index, func() {
				select {
				case others <- index:
				default:
				}
			}, nil)
		}(index)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow task did not start")
	}
	seen := map[int]bool{}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(seen) < 2 {
		select {
		case i := <-others:
			seen[i] = true
		case <-deadline.C:
			t.Fatal("slow stabilization blocked another task")
		}
	}
	cancel()
	wg.Wait()
}
