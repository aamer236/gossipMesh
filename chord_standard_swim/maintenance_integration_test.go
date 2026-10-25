package main

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveRuntimeReportsBackoffAndReleasesListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"PORT": port, "ADVERTISE_ADDR": addr, "NODE_ID": "0", "M": "8", "R": "4",
		"BOOTSTRAP_ADDR": "", "LOG_LEVEL": "quiet", "RPC_TIMEOUT_MS": "100",
		"STABILIZE_INTERVAL_MS": "20", "FIX_FINGERS_INTERVAL_MS": "20", "CHECK_PRED_INTERVAL_MS": "20",
		"ADAPTIVE_MAINTENANCE": "true", "ADAPTIVE_STABLE_AFTER_MS": "20",
		"ADAPTIVE_MAX_STABILIZE_MS": "80", "ADAPTIVE_MAX_FIX_FINGERS_MS": "80", "ADAPTIVE_MAX_CHECK_PRED_MS": "80",
		"SWIM_INTERVAL_MS": "60", "SWIM_DIRECT_TIMEOUT_MS": "10", "SWIM_SUSPICION_TIMEOUT_MS": "300",
		"SWIM_STATE_FILE": filepath.Join(t.TempDir(), "incarnation.json"),
	} {
		t.Setenv(name, value)
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- run(ctx, cfg) }()
	var stopped sync.Once
	stop := func() {
		stopped.Do(func() {
			cancel()
			select {
			case err := <-finished:
				if err != nil {
					t.Error("adaptive runtime:", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("adaptive runtime did not stop")
			}
		})
	}
	t.Cleanup(stop)
	httpTransport := &http.Transport{}
	defer httpTransport.CloseIdleConnections()
	client := &http.Client{Transport: httpTransport, Timeout: 100 * time.Millisecond}
	var latest struct {
		Maintenance struct {
			Enabled       bool             `json:"enabled"`
			Mode          string           `json:"mode"`
			Intervals     map[string]int64 `json:"intervals_ms"`
			BlockedReason string           `json:"blocked_reason"`
		} `json:"maintenance"`
		SWIM struct {
			Counters map[string]uint64 `json:"counters"`
		} `json:"swim"`
	}
	await(t, func() bool {
		response, err := client.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&latest) != nil {
			return false
		}
		return latest.Maintenance.Enabled && latest.Maintenance.Mode == "backoff" &&
			latest.Maintenance.BlockedReason == "" &&
			latest.SWIM.Counters["maintenance_stabilize_runs"] > 0 &&
			latest.SWIM.Counters["maintenance_fix_fingers_runs"] > 0 &&
			latest.SWIM.Counters["maintenance_check_predecessor_runs"] > 0
	})
	for _, name := range maintenanceNames {
		if got := latest.Maintenance.Intervals[name]; got <= 20 || got > 80 {
			t.Errorf("%s interval = %d ms, want backoff in (20,80]", name, got)
		}
	}
	stop()
	replacement, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("adaptive listener leaked:", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveChordRPCFailureResetsBackoffExceptCancellation(t *testing.T) {
	var received atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		http.Error(w, "controlled peer failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	for _, cancelled := range []bool{false, true} {
		t.Run("cancelled="+strconv.FormatBool(cancelled), func(t *testing.T) {
			telemetry := NewTelemetry("0", "quiet", nil)
			cfg := &Config{
				StabilizeInterval: 20 * time.Millisecond, FixFingersInterval: 20 * time.Millisecond, CheckPredInterval: 20 * time.Millisecond,
				Adaptive: AdaptiveConfig{Enabled: true, StableAfter: time.Minute,
					MaxStabilizeInterval: 160 * time.Millisecond, MaxFixFingersInterval: 160 * time.Millisecond, MaxCheckPredInterval: 160 * time.Millisecond},
			}
			old := time.Now().Add(-3 * time.Minute)
			policy := NewMaintenancePolicy(cfg, telemetry, old)
			self := &NodeRef{ID: big.NewInt(0), Addr: "127.0.0.1:1"}
			snapshot := RoutingSnapshot{Successor: self, Successors: []*NodeRef{self}, Fingers: []*NodeRef{self}}
			policy.Observe(old, snapshot, "")
			policy.Observe(time.Now(), snapshot, "")
			before := policy.Metrics()
			if before["mode"] != "backoff" {
				t.Fatal("fixture did not reach backoff:", before)
			}
			transport := NewNetworkTransport(100*time.Millisecond, NewMetricsCollector())
			defer transport.client.CloseIdleConnections()
			transport.maintenance, transport.telemetry = policy, telemetry
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport.ctx = ctx
			if cancelled {
				cancel()
			}
			requestsBefore := received.Load()
			if err := transport.PingRPC(strings.TrimPrefix(server.URL, "http://")); err == nil {
				t.Fatal("failed RPC returned success")
			}
			after := policy.Metrics()
			if cancelled {
				if after["mode"] != "backoff" || after["last_reason"] != before["last_reason"] || telemetry.Snapshot()["maintenance_fast"] != 0 {
					t.Fatal("shutdown cancellation triggered maintenance activity:", after)
				}
				if received.Load() != requestsBefore {
					t.Fatal("cancelled transport sent an RPC")
				}
			} else {
				if after["mode"] != "fast" || after["last_reason"] != "chord_rpc_failure" || policy.Interval(0, time.Now()) != cfg.StabilizeInterval {
					t.Fatal("failed peer RPC did not restore fast mode:", after)
				}
				if received.Load() != requestsBefore+1 {
					t.Fatal("fixture did not perform the real peer RPC")
				}
			}
		})
	}
}
