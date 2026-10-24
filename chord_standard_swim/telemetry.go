package main

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Event history is bounded; counters are cumulative. Callbacks never run under
// the membership or routing lock. Logs carry local wall time for observation,
// not for distributed version ordering.
type Event struct {
	Time    time.Time              `json:"time"`
	Local   string                 `json:"local_id"`
	Target  string                 `json:"target_id,omitempty"`
	Type    string                 `json:"type"`
	Details map[string]interface{} `json:"details,omitempty"`
}
type Telemetry struct {
	mu       sync.Mutex
	local    string
	now      func() time.Time
	writer   io.Writer
	level    string
	counters map[string]uint64
	events   []Event
}

func NewTelemetry(local, level string, writer io.Writer) *Telemetry {
	return &Telemetry{local: local, now: time.Now, writer: writer, level: level, counters: make(map[string]uint64)}
}
func (t *Telemetry) Add(name string, n uint64) { t.mu.Lock(); t.counters[name] += n; t.mu.Unlock() }
func (t *Telemetry) Emit(kind, target string, details map[string]interface{}) {
	event := Event{Time: t.now().UTC(), Local: t.local, Target: target, Type: kind, Details: details}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counters[kind]++
	if len(t.events) >= 2048 {
		copy(t.events, t.events[1:])
		t.events = t.events[:2047]
	}
	t.events = append(t.events, event)
	debug := kind == "probe_started" || kind == "direct_ack" || kind == "update_received" || kind == "update_ignored" || kind == "update_piggybacked" || kind == "update_retired" || kind == "update_merged"
	if t.writer != nil && t.level != "quiet" && (t.level == "debug" || (!debug && t.level == "info") || kind == "error") {
		if err := json.NewEncoder(t.writer).Encode(event); err != nil {
			t.counters["log_write_errors"]++
		}
	}
}
func (t *Telemetry) Snapshot() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]uint64, len(t.counters))
	for k, v := range t.counters {
		out[k] = v
	}
	return out
}
func (t *Telemetry) Events() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Event(nil), t.events...)
}
