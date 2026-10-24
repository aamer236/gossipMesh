package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func run(ctx context.Context, cfg *Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	telemetry := NewTelemetry(cfg.NodeID.String(), cfg.LogLevel, os.Stdout)
	metrics := NewMetricsCollector()
	transport := NewNetworkTransport(cfg.RPCTimeout, metrics)
	transport.ctx = ctx
	node := NewChordNode(cfg.NodeID, cfg.AdvertiseAddr, cfg.M, cfg.R, transport, metrics)
	transport.SetNode(node)
	table, err := NewMembership(&NodeRef{ID: cfg.NodeID, Addr: cfg.AdvertiseAddr}, cfg.M, cfg.SWIM, telemetry)
	if err != nil {
		return err
	}
	swimTransport := NewHTTPSWIMTransport(telemetry)
	swim := NewSWIM(table, swimTransport, telemetry)
	coordinator := NewCoordinator(ctx, node, telemetry, cfg.SWIM.MaxMembers)
	table.onEvent = coordinator.Submit
	transport.swim = swim
	transport.coordinator = coordinator
	transport.telemetry = telemetry
	if err = transport.StartListener(cfg.Port); err != nil {
		return err
	}
	defer transport.StopListener()
	coordinator.Start()
	defer coordinator.Stop()
	// Cancellation propagates to Chord requests before waiting for repair workers.
	defer cancel()
	telemetry.Emit("node_started", cfg.NodeID.String(), map[string]interface{}{"addr": cfg.AdvertiseAddr, "incarnation": table.Self().Incarnation, "variant": "chord_standard_swim"})
	joined := false
	for attempt := 0; attempt < 10; attempt++ {
		if ctx.Err() != nil {
			return nil
		}
		err = swim.Bootstrap(ctx, cfg.BootstrapAddr)
		if err == nil {
			err = node.Join(cfg.BootstrapAddr)
		}
		if err == nil {
			joined = true
			break
		}
		telemetry.Emit("join_retry", cfg.NodeID.String(), map[string]interface{}{"error": err.Error(), "attempt": attempt + 1})
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	if !joined {
		return fmt.Errorf("failed to bootstrap/join: %w", err)
	}
	if err = swim.Start(ctx); err != nil {
		return err
	}
	defer swim.Stop()
	transport.setReady(true)
	var maintenance sync.WaitGroup
	for _, task := range []struct {
		interval time.Duration
		run      func()
	}{{cfg.StabilizeInterval, node.Stabilize}, {cfg.FixFingersInterval, node.FixFingers}, {cfg.CheckPredInterval, node.CheckPredecessor}} {
		maintenance.Add(1)
		go func(interval time.Duration, operation func()) {
			defer maintenance.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					operation()
				}
			}
		}(task.interval, task.run)
	}
	<-ctx.Done()
	transport.setReady(false)
	cancel()
	swim.Stop()
	coordinator.Stop()
	transport.StopListener()
	maintenance.Wait()
	swimTransport.client.CloseIdleConnections()
	transport.client.CloseIdleConnections()
	telemetry.Emit("node_stopped", cfg.NodeID.String(), nil)
	return nil
}
func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "node:", err)
		os.Exit(1)
	}
}
