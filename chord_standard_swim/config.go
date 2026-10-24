package main

import (
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type SWIMConfig struct {
	Interval         time.Duration
	DirectTimeout    time.Duration
	IndirectHelpers  int
	SuspicionTimeout time.Duration
	PiggybackLimit   int
	BufferLimit      int
	MaxMembers       int
	RetransmitFactor int
	StateFile        string
	Seed             int64
}
type Config struct {
	Port               int
	AdvertiseAddr      string
	NodeID             *big.Int
	BootstrapAddr      string
	M                  int
	R                  int
	StabilizeInterval  time.Duration
	FixFingersInterval time.Duration
	CheckPredInterval  time.Duration
	RPCTimeout         time.Duration
	SWIM               SWIMConfig
	LogLevel           string
}

func envInt(name string, def, min, max int) (int, error) {
	text := os.Getenv(name)
	if text == "" {
		return def, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s must be an integer in [%d,%d]", name, min, max)
	}
	return value, nil
}
func loadConfig() (*Config, error) {
	cfg := &Config{}
	var err error
	cfg.Port, err = envInt("PORT", 8000, 1, 65535)
	if err != nil {
		return nil, err
	}
	cfg.M, err = envInt("M", 160, 1, 160)
	if err != nil {
		return nil, err
	}
	cfg.R, err = envInt("R", 4, 1, 4096)
	if err != nil {
		return nil, err
	}
	cfg.AdvertiseAddr = os.Getenv("ADVERTISE_ADDR")
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	}
	if err = validAddress(cfg.AdvertiseAddr); err != nil {
		return nil, fmt.Errorf("ADVERTISE_ADDR: %w", err)
	}
	cfg.NodeID = HashKey(cfg.AdvertiseAddr, cfg.M)
	if text := os.Getenv("NODE_ID"); text != "" {
		var ok bool
		cfg.NodeID, ok = new(big.Int).SetString(text, 10)
		if !ok {
			return nil, fmt.Errorf("NODE_ID must be a decimal integer")
		}
	}
	if !validID(cfg.NodeID, cfg.M) {
		return nil, fmt.Errorf("NODE_ID must be in [0,2^M)")
	}
	cfg.BootstrapAddr = os.Getenv("BOOTSTRAP_ADDR")
	if cfg.BootstrapAddr != "" {
		if err = validAddress(cfg.BootstrapAddr); err != nil {
			return nil, fmt.Errorf("BOOTSTRAP_ADDR: %w", err)
		}
	}
	durations := []struct {
		name string
		def  int
		out  *time.Duration
	}{
		{"STABILIZE_INTERVAL_MS", 200, &cfg.StabilizeInterval}, {"FIX_FINGERS_INTERVAL_MS", 100, &cfg.FixFingersInterval},
		{"CHECK_PRED_INTERVAL_MS", 300, &cfg.CheckPredInterval}, {"RPC_TIMEOUT_MS", 500, &cfg.RPCTimeout},
		{"SWIM_INTERVAL_MS", 600, &cfg.SWIM.Interval}, {"SWIM_DIRECT_TIMEOUT_MS", 100, &cfg.SWIM.DirectTimeout},
		{"SWIM_SUSPICION_TIMEOUT_MS", 3000, &cfg.SWIM.SuspicionTimeout},
	}
	for _, entry := range durations {
		value, e := envInt(entry.name, entry.def, 1, 300000)
		if e != nil {
			return nil, e
		}
		*entry.out = time.Duration(value) * time.Millisecond
	}
	cfg.SWIM.IndirectHelpers, err = envInt("SWIM_INDIRECT_HELPERS", 3, 0, 8)
	if err != nil {
		return nil, err
	}
	cfg.SWIM.PiggybackLimit, err = envInt("SWIM_MAX_PIGGYBACK", 6, 1, 64)
	if err != nil {
		return nil, err
	}
	cfg.SWIM.BufferLimit, err = envInt("SWIM_MAX_UPDATES", 256, 1, 16384)
	if err != nil {
		return nil, err
	}
	cfg.SWIM.MaxMembers, err = envInt("SWIM_MAX_MEMBERS", 4096, 2, 16384)
	if err != nil {
		return nil, err
	}
	cfg.SWIM.RetransmitFactor, err = envInt("SWIM_RETRANSMIT_FACTOR", 4, 1, 32)
	if err != nil {
		return nil, err
	}
	cfg.SWIM.StateFile = os.Getenv("SWIM_STATE_FILE")
	if cfg.SWIM.StateFile == "" {
		cfg.SWIM.StateFile = filepath.Join(os.TempDir(), "gossipmesh-swim-"+cfg.NodeID.String()+"-"+strconv.Itoa(cfg.Port)+".json")
	}
	cfg.SWIM.Seed = time.Now().UnixNano()
	cfg.LogLevel = os.Getenv("LOG_LEVEL")
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.LogLevel != "info" && cfg.LogLevel != "debug" && cfg.LogLevel != "error" && cfg.LogLevel != "quiet" {
		return nil, fmt.Errorf("LOG_LEVEL must be debug, info, error or quiet")
	}
	if err = cfg.SWIM.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
func (c SWIMConfig) Validate() error {
	if c.DirectTimeout <= 0 || c.Interval < 3*c.DirectTimeout {
		return fmt.Errorf("SWIM_INTERVAL_MS must be at least three direct timeouts")
	}
	if c.SuspicionTimeout < 2*c.Interval {
		return fmt.Errorf("SWIM_SUSPICION_TIMEOUT_MS must be at least two protocol intervals")
	}
	if c.IndirectHelpers < 0 || c.IndirectHelpers > 8 || c.PiggybackLimit < 1 || c.PiggybackLimit > 64 || c.BufferLimit < c.PiggybackLimit || c.MaxMembers < 2 || c.RetransmitFactor < 1 {
		return fmt.Errorf("invalid SWIM helper/buffer/membership bounds")
	}
	return nil
}
func validAddress(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return fmt.Errorf("expected host:port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("port must be in [1,65535]")
	}
	return nil
}
