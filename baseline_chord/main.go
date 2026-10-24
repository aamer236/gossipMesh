package main

import (
	"fmt"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Config struct {
	Port                int
	AdvertiseAddr       string
	NodeID              *big.Int
	BootstrapAddr       string
	M                   int
	R                   int
	StabilizeInterval   time.Duration
	FixFingersInterval  time.Duration
	CheckPredInterval   time.Duration
	RPCTimeout          time.Duration
}

func loadConfig() (*Config, error) {
	port := 8000
	if pStr := os.Getenv("PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil {
			port = p
		}
	} else if pStr := os.Getenv("LISTEN_PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil {
			port = p
		}
	}

	advertiseAddr := os.Getenv("ADVERTISE_ADDR")
	if advertiseAddr == "" {
		host, err := os.Hostname()
		if err == nil && host != "" {
			advertiseAddr = fmt.Sprintf("%s:%d", host, port)
		} else {
			advertiseAddr = fmt.Sprintf("127.0.0.1:%d", port)
		}
	}

	m := 160
	if mStr := os.Getenv("M"); mStr != "" {
		if val, err := strconv.Atoi(mStr); err == nil && val > 0 {
			m = val
		}
	}

	r := 4
	if rStr := os.Getenv("R"); rStr != "" {
		if val, err := strconv.Atoi(rStr); err == nil && val > 0 {
			r = val
		}
	}

	var nodeID *big.Int
	if idStr := os.Getenv("NODE_ID"); idStr != "" {
		var ok bool
		nodeID, ok = new(big.Int).SetString(idStr, 10)
		if !ok {
			return nil, fmt.Errorf("invalid NODE_ID environment variable: %s", idStr)
		}
	} else {
		nodeID = HashKey(advertiseAddr, m)
	}

	bootstrapAddr := os.Getenv("BOOTSTRAP_ADDR")

	stabMs := 200
	if sStr := os.Getenv("STABILIZE_INTERVAL_MS"); sStr != "" {
		if val, err := strconv.Atoi(sStr); err == nil && val > 0 {
			stabMs = val
		}
	}

	fixMs := 100
	if fStr := os.Getenv("FIX_FINGERS_INTERVAL_MS"); fStr != "" {
		if val, err := strconv.Atoi(fStr); err == nil && val > 0 {
			fixMs = val
		}
	}

	checkMs := 300
	if cStr := os.Getenv("CHECK_PRED_INTERVAL_MS"); cStr != "" {
		if val, err := strconv.Atoi(cStr); err == nil && val > 0 {
			checkMs = val
		}
	}

	rpcTimeoutMs := 1000
	if tStr := os.Getenv("RPC_TIMEOUT_MS"); tStr != "" {
		if val, err := strconv.Atoi(tStr); err == nil && val > 0 {
			rpcTimeoutMs = val
		}
	}

	return &Config{
		Port:               port,
		AdvertiseAddr:      advertiseAddr,
		NodeID:             nodeID,
		BootstrapAddr:      bootstrapAddr,
		M:                  m,
		R:                  r,
		StabilizeInterval:  time.Duration(stabMs) * time.Millisecond,
		FixFingersInterval: time.Duration(fixMs) * time.Millisecond,
		CheckPredInterval:  time.Duration(checkMs) * time.Millisecond,
		RPCTimeout:         time.Duration(rpcTimeoutMs) * time.Millisecond,
	}, nil
}

// InitializeNode reads environment and wires together ChordNode, Transport, and Metrics.
func InitializeNode() (*ChordNode, *NetworkTransport, *MetricsCollector, *Config, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	metrics := NewMetricsCollector()
	transport := NewNetworkTransport(cfg.RPCTimeout, metrics)
	node := NewChordNode(cfg.NodeID, cfg.AdvertiseAddr, cfg.M, cfg.R, transport, metrics)
	transport.SetNode(node)

	return node, transport, metrics, cfg, nil
}

func main() {
	node, transport, _, cfg, err := InitializeNode()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Initialization error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[Chord Node] Starting ID=%s Addr=%s Bootstrap=%s M=%d R=%d\n",
		node.ID.String(), node.Addr, cfg.BootstrapAddr, cfg.M, cfg.R)

	// Start HTTP listener
	if err := transport.StartListener(cfg.Port); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start listener on port %d: %v\n", cfg.Port, err)
		os.Exit(1)
	}

	// Wait briefly for listener to be active
	time.Sleep(100 * time.Millisecond)

	// Join ring
	if cfg.BootstrapAddr != "" && cfg.BootstrapAddr != cfg.AdvertiseAddr {
		fmt.Printf("[Chord Node] Attempting to join ring via %s...\n", cfg.BootstrapAddr)
		// Retry join up to 10 times in case bootstrap node container is still booting
		joined := false
		for attempt := 1; attempt <= 10; attempt++ {
			if err := node.Join(cfg.BootstrapAddr); err == nil {
				joined = true
				fmt.Printf("[Chord Node] Successfully joined ring via %s (attempt %d)\n", cfg.BootstrapAddr, attempt)
				break
			} else {
				fmt.Printf("[Chord Node] Join attempt %d failed: %v. Retrying...\n", attempt, err)
				time.Sleep(1 * time.Second)
			}
		}
		if !joined {
			fmt.Fprintf(os.Stderr, "[Chord Node] Could not join bootstrap node %s after 10 attempts.\n", cfg.BootstrapAddr)
			os.Exit(1)
		}
	} else {
		fmt.Println("[Chord Node] Initializing as root/singleton bootstrap node.")
		_ = node.Join("")
	}

	// Background maintenance routines
	stopCh := make(chan struct{})

	// 1. Stabilize ticker
	go func() {
		ticker := time.NewTicker(cfg.StabilizeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				node.Stabilize()
			case <-stopCh:
				return
			}
		}
	}()

	// 2. Fix fingers ticker
	go func() {
		ticker := time.NewTicker(cfg.FixFingersInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				node.FixFingers()
			case <-stopCh:
				return
			}
		}
	}()

	// 3. Check predecessor ticker
	go func() {
		ticker := time.NewTicker(cfg.CheckPredInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				node.CheckPredecessor()
			case <-stopCh:
				return
			}
		}
	}()

	// Wait for OS shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	fmt.Printf("\n[Chord Node] Received signal %v, shutting down gracefully...\n", sig)
	close(stopCh)
	transport.StopListener()
	fmt.Println("[Chord Node] Shutdown complete.")
}

func isPortAvailable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}
