package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// NetworkTransport handles incoming and outgoing HTTP-based RPCs for Chord.
type NetworkTransport struct {
	client        *http.Client
	ctx           context.Context
	swim          *SWIM
	coordinator   *Coordinator
	telemetry     *Telemetry
	ready         bool
	readyMu       sync.RWMutex
	serverWG      sync.WaitGroup
	node          *ChordNode
	metrics       *MetricsCollector
	port          int
	server        *http.Server
	msgsSent      uint64
	bytesSent     uint64
	msgsReceived  uint64
	bytesReceived uint64
}

func NewNetworkTransport(timeout time.Duration, metrics *MetricsCollector) *NetworkTransport {
	return &NetworkTransport{
		client: &http.Client{
			Timeout: timeout,
		},
		metrics: metrics,
	}
}

func (t *NetworkTransport) SetNode(node *ChordNode) {
	t.node = node
}

// StartListener starts the HTTP server listening on the specified port.
func (t *NetworkTransport) StartListener(port int) error {
	t.port = port
	mux := http.NewServeMux()

	if t.swim != nil {
		mux.Handle("/swim/rpc", t.swim)
		mux.HandleFunc("/membership", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(t.swim.table.Snapshot())
		})
		mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(t.telemetry.Events())
		})
	}
	// RPC Handlers
	mux.HandleFunc("/rpc/find_successor", t.handleFindSuccessor)
	mux.HandleFunc("/rpc/predecessor_and_list", t.handlePredecessorAndList)
	mux.HandleFunc("/rpc/notify", t.handleNotify)
	mux.HandleFunc("/rpc/ping", t.handlePing)

	// Orchestrator / Diagnostic Endpoints
	mux.HandleFunc("/lookup", t.handleLookup)
	mux.HandleFunc("/state", t.handleState)
	mux.HandleFunc("/metrics", t.handleMetrics)
	mux.HandleFunc("/health", t.handleHealth)

	t.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
	}

	listener, err := net.Listen("tcp", t.server.Addr)
	if err != nil {
		return err
	}
	t.serverWG.Add(1)
	go func() {
		defer t.serverWG.Done()
		if err := t.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[Transport] Listener failed: %v\n", err)
		}
	}()

	return nil
}

// StopListener gracefully stops the HTTP server.
func (t *NetworkTransport) StopListener() {
	if t.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := t.server.Shutdown(ctx); err != nil {
		if t.telemetry != nil {
			t.telemetry.Emit("error", "", map[string]interface{}{"operation": "http_shutdown", "error": err.Error()})
		}
		_ = t.server.Close()
	}
	t.serverWG.Wait()
}

// SendChordRPC performs a synchronous HTTP POST/GET overlay RPC with telemetry.
func (t *NetworkTransport) SendChordRPC(target string, endpoint string, req interface{}, resp interface{}) (resultErr error) {
	defer func() {
		if resultErr != nil && t.telemetry != nil {
			t.telemetry.Emit("chord_rpc_error", "", map[string]interface{}{"target_addr": target, "endpoint": endpoint, "error": resultErr.Error()})
		}
	}()
	if t.node != nil && t.node.peerBlocked(target) {
		return fmt.Errorf("peer %s blocked by confirmed-dead hint", target)
	}
	var bodyReader io.Reader
	var reqLen int
	if req != nil {
		reqBytes, err := json.Marshal(req)
		if err != nil {
			return err
		}
		reqLen = len(reqBytes)
		bodyReader = bytes.NewReader(reqBytes)
	}

	url := fmt.Sprintf("http://%s%s", target, endpoint)
	method := "POST"
	if req == nil {
		method = "GET"
	}

	httpReq, err := http.NewRequestWithContext(t.requestContext(), method, url, bodyReader)
	if err != nil {
		return err
	}
	if req != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	atomic.AddUint64(&t.msgsSent, 1)
	atomic.AddUint64(&t.bytesSent, uint64(reqLen))

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}

	atomic.AddUint64(&t.msgsReceived, 1)
	atomic.AddUint64(&t.bytesReceived, uint64(len(respBytes)))

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc error: http status %d", httpResp.StatusCode)
	}
	if resp != nil {
		return json.Unmarshal(respBytes, resp)
	}
	return nil
}

// SendSWIMMessage is a stub for Baseline Chord (Case 1 does not use SWIM).
func (t *NetworkTransport) SendSWIMMessage(target string, message interface{}) error {
	// Not applicable for baseline_chord (no SWIM)
	return nil
}

// --- Specific RPC Wrappers ---

type FindSuccessorReq struct {
	ID   string   `json:"id"`
	M    int      `json:"m"`
	Path []string `json:"path,omitempty"`
}

type FindSuccessorResp struct {
	Node *NodeRef `json:"node"`
	Hops int      `json:"hops"`
}

func (t *NetworkTransport) FindSuccessorRPC(target string, id *big.Int) (*NodeRef, int, error) {
	return t.findSuccessorPathRPC(target, id, nil)
}

func (t *NetworkTransport) findSuccessorPathRPC(target string, id *big.Int, path []string) (*NodeRef, int, error) {
	req := FindSuccessorReq{ID: id.String(), M: t.node.M, Path: path}
	var resp FindSuccessorResp
	err := t.SendChordRPC(target, "/rpc/find_successor", req, &resp)
	if err != nil {
		return nil, 0, err
	}
	return resp.Node, resp.Hops, nil
}

type PredAndListResp struct {
	Predecessor   *NodeRef   `json:"predecessor"`
	SuccessorList []*NodeRef `json:"successor_list"`
	M             int        `json:"m"`
}

func (t *NetworkTransport) GetPredecessorAndListRPC(target string) (*NodeRef, []*NodeRef, error) {
	var resp PredAndListResp
	err := t.SendChordRPC(target, "/rpc/predecessor_and_list", nil, &resp)
	if err != nil {
		return nil, nil, err
	}
	if resp.M != t.node.M {
		return nil, nil, fmt.Errorf("incompatible identifier space")
	}
	return resp.Predecessor, resp.SuccessorList, nil
}

type NotifyReq struct {
	Candidate *NodeRef `json:"candidate"`
	M         int      `json:"m"`
}

func (t *NetworkTransport) NotifyRPC(target string, candidate *NodeRef) error {
	req := NotifyReq{Candidate: candidate, M: t.node.M}
	return t.SendChordRPC(target, "/rpc/notify", req, nil)
}

func (t *NetworkTransport) PingRPC(target string) error {
	return t.SendChordRPC(target, "/rpc/ping", nil, nil)
}

// Overlay counters cover JSON/body payloads, excluding HTTP/TCP headers.
type countedBody struct {
	io.ReadCloser
	count *uint64
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	atomic.AddUint64(b.count, uint64(n))
	return n, err
}

type countedResponse struct {
	http.ResponseWriter
	transport *NetworkTransport
	started   bool
}

func (w *countedResponse) WriteHeader(status int) {
	if !w.started {
		w.started = true
		atomic.AddUint64(&w.transport.msgsSent, 1)
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *countedResponse) Write(p []byte) (int, error) {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	atomic.AddUint64(&w.transport.bytesSent, uint64(n))
	return n, err
}
func (t *NetworkTransport) trackRPC(w http.ResponseWriter, r *http.Request) http.ResponseWriter {
	if r.Body != nil {
		r.Body = &countedBody{ReadCloser: r.Body, count: &t.bytesReceived}
	}
	return &countedResponse{ResponseWriter: w, transport: t}
}

// --- HTTP Handlers ---

func (t *NetworkTransport) handleFindSuccessor(w http.ResponseWriter, r *http.Request) {
	w = t.trackRPC(w, r)
	atomic.AddUint64(&t.msgsReceived, 1)
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FindSuccessorReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, ok := new(big.Int).SetString(req.ID, 10)
	if !ok {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	if req.M != t.node.M || !validID(id, t.node.M) {
		http.Error(w, "incompatible space or invalid ID", http.StatusBadRequest)
		return
	}
	succ, hops, err := t.node.findSuccessor(id, req.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := FindSuccessorResp{Node: succ, Hops: hops}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (t *NetworkTransport) handlePredecessorAndList(w http.ResponseWriter, r *http.Request) {
	w = t.trackRPC(w, r)
	atomic.AddUint64(&t.msgsReceived, 1)
	t.node.mu.RLock()
	pred := t.node.Predecessor
	succList := make([]*NodeRef, len(t.node.SuccessorList))
	copy(succList, t.node.SuccessorList)
	t.node.mu.RUnlock()

	resp := PredAndListResp{
		Predecessor:   pred,
		SuccessorList: succList,
		M:             t.node.M,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (t *NetworkTransport) handleNotify(w http.ResponseWriter, r *http.Request) {
	w = t.trackRPC(w, r)
	atomic.AddUint64(&t.msgsReceived, 1)
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req NotifyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.M != t.node.M {
		http.Error(w, "incompatible identifier space", http.StatusBadRequest)
		return
	}
	if err := t.node.Notify(req.Candidate); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (t *NetworkTransport) handlePing(w http.ResponseWriter, r *http.Request) {
	w = t.trackRPC(w, r)
	atomic.AddUint64(&t.msgsReceived, 1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"pong"}`))
}

func (t *NetworkTransport) handleLookup(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := r.URL.Query().Get("key")
	idStr := r.URL.Query().Get("id")

	var keyID *big.Int
	if idStr != "" {
		var ok bool
		keyID, ok = new(big.Int).SetString(idStr, 10)
		if !ok {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
	} else if key != "" {
		keyID = HashKey(key, t.node.M)
	} else {
		http.Error(w, "missing ?key= or ?id= parameter", http.StatusBadRequest)
		return
	}

	if !validID(keyID, t.node.M) {
		http.Error(w, "ID outside identifier space", http.StatusBadRequest)
		return
	}
	succ, hops, err := t.node.FindSuccessor(keyID)
	latMs := float64(time.Since(start).Microseconds()) / 1000.0

	success := (err == nil && succ != nil)
	t.metrics.LogLookup(latMs, success)
	t.metrics.LogHops(hops, success)

	if !success {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    false,
			"error":      fmt.Sprintf("%v", err),
			"latency_ms": latMs,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"key":        key,
		"key_id":     keyID.String(),
		"successor":  succ,
		"hops":       hops,
		"latency_ms": latMs,
	})
}

func (t *NetworkTransport) handleState(w http.ResponseWriter, r *http.Request) {
	state := t.node.GetState()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (t *NetworkTransport) handleMetrics(w http.ResponseWriter, r *http.Request) {
	summary := t.metrics.GetSummary()
	summary["network"] = map[string]interface{}{
		"msgs_sent":      atomic.LoadUint64(&t.msgsSent),
		"msgs_received":  atomic.LoadUint64(&t.msgsReceived),
		"bytes_sent":     atomic.LoadUint64(&t.bytesSent),
		"bytes_received": atomic.LoadUint64(&t.bytesReceived),
	}
	if t.swim != nil {
		summary["variant"] = "chord_standard_swim"
		summary["swim"] = t.swim.Metrics()
		summary["gossipmesh"] = t.coordinator.Metrics()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summary)
}

func (t *NetworkTransport) requestContext() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return context.Background()
}
func (t *NetworkTransport) setReady(ready bool) {
	t.readyMu.Lock()
	t.ready = ready
	t.readyMu.Unlock()
}
func (t *NetworkTransport) handleHealth(w http.ResponseWriter, r *http.Request) {
	if t.swim != nil {
		t.readyMu.RLock()
		ready := t.ready
		t.readyMu.RUnlock()
		if !ready {
			http.Error(w, "joining", http.StatusServiceUnavailable)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}
