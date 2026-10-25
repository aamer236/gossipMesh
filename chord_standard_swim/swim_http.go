package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPSWIMTransport struct {
	client    *http.Client
	telemetry *Telemetry
}

func NewHTTPSWIMTransport(t *Telemetry) *HTTPSWIMTransport {
	return &HTTPSWIMTransport{client: &http.Client{Timeout: 30 * time.Second}, telemetry: t}
}
func (t *HTTPSWIMTransport) Exchange(ctx context.Context, addr string, msg SWIMMessage) (SWIMMessage, error) {
	raw, err := encodeSWIM(msg)
	if err != nil {
		return SWIMMessage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/swim/rpc", bytes.NewReader(raw))
	if err != nil {
		return SWIMMessage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	t.telemetry.Add("swim_messages_sent", 1)
	t.telemetry.Add("swim_payload_bytes_sent", uint64(len(raw)))
	resp, err := t.client.Do(req)
	if err != nil {
		return SWIMMessage{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSWIMBody+1))
	t.telemetry.Add("swim_messages_received", 1)
	t.telemetry.Add("swim_payload_bytes_received", uint64(len(data)))
	if err != nil {
		return SWIMMessage{}, err
	}
	if len(data) > maxSWIMBody {
		return SWIMMessage{}, fmt.Errorf("SWIM response too large")
	}
	if resp.StatusCode != http.StatusOK {
		return SWIMMessage{}, fmt.Errorf("SWIM HTTP %d: %s", resp.StatusCode, string(data))
	}
	reply, _, err := decodeSWIM(data)
	return reply, err
}
func (s *SWIM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	select {
	case s.handlers <- struct{}{}:
		defer func() { <-s.handlers }()
	default:
		http.Error(w, "SWIM handler capacity reached", http.StatusTooManyRequests)
		return
	}
	s.telemetry.Add("swim_messages_received", 1)
	r.Body = http.MaxBytesReader(w, r.Body, maxSWIMBody)
	raw, err := io.ReadAll(r.Body)
	s.telemetry.Add("swim_payload_bytes_received", uint64(len(raw)))
	var msg SWIMMessage
	compact := false
	if err == nil {
		msg, compact, err = decodeSWIM(raw)
	}
	if err != nil {
		s.writeHTTP(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*s.table.cfg.DirectTimeout)
	defer cancel()
	reply, err := s.Receive(ctx, msg)
	if err != nil {
		s.telemetry.Emit("swim_rpc_error", peerID(msg.From.Peer), map[string]interface{}{"error": err.Error(), "probe_id": msg.ProbeID})
		s.writeHTTP(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if compact {
		raw, encodeErr := encodeSWIM(reply)
		if encodeErr != nil {
			s.writeHTTP(w, 500, map[string]string{"error": encodeErr.Error()})
			return
		}
		s.writeHTTP(w, http.StatusOK, json.RawMessage(raw))
	} else {
		s.writeHTTP(w, http.StatusOK, reply)
	}
}
func (s *SWIM) writeHTTP(w http.ResponseWriter, status int, body interface{}) {
	raw, err := json.Marshal(body)
	if err != nil {
		s.telemetry.Emit("error", "", map[string]interface{}{"operation": "encode_swim_response", "error": err.Error()})
		http.Error(w, "encode error", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	n, err := w.Write(raw)
	s.telemetry.Add("swim_messages_sent", 1)
	s.telemetry.Add("swim_payload_bytes_sent", uint64(n))
	if err != nil {
		s.telemetry.Emit("swim_rpc_error", "", map[string]interface{}{"operation": "write_swim_response", "error": err.Error()})
	}
}
