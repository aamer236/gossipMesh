package main

// Experimental owner-local storage. No replication, persistence or key migration.
import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sort"
	"sync"
)

const maxStoredKeys = 4096
const maxValueBytes = 65536

type KeyStore struct {
	mu     sync.RWMutex
	values map[string]string
}
type StorageRequest struct {
	Version   int    `json:"version"`
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}
type StorageReply struct {
	Version int    `json:"version"`
	Key     string `json:"key"`
	Owner   string `json:"owner"`
	Address string `json:"address"`
	Found   bool   `json:"found"`
	Value   string `json:"value"`
	Error   string `json:"error,omitempty"`
}

func storageJSON(w http.ResponseWriter, status int, reply StorageReply) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(reply)
}
func decodeStorage(w http.ResponseWriter, r *http.Request, out interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, 6*maxValueBytes+1024)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}
func (t *NetworkTransport) storageKey(raw string) (*big.Int, error) {
	id, ok := new(big.Int).SetString(raw, 10)
	if !ok || id.Sign() < 0 || id.BitLen() > t.node.M {
		return nil, fmt.Errorf("id must be a decimal integer in [0, 2^M)")
	}
	return id, nil
}

// Called only at the owner; the routing lock makes ownership-check and local
// write atomic with respect to local neighbor changes, with no network I/O.
func (t *NetworkTransport) applyStorage(req StorageRequest) StorageReply {
	reply := StorageReply{Version: 1, Key: req.Key, Owner: t.node.ID.String(), Address: t.node.Addr}
	id, err := t.storageKey(req.Key)
	if err != nil || req.Version != 1 || (req.Operation != "put" && req.Operation != "get") || len(req.Value) > maxValueBytes {
		reply.Error = "invalid storage request"
		return reply
	}
	reply.Key = id.String()
	n := t.node
	n.mu.RLock()
	defer n.mu.RUnlock()
	owns := n.singleton || (n.Predecessor != nil && InInterval(id, n.Predecessor.ID, n.ID, false, true, n.M))
	if !owns {
		reply.Error = "owner changed or ownership not established; retry after convergence"
		return reply
	}
	store := t.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if req.Operation == "put" {
		if _, exists := store.values[reply.Key]; !exists && len(store.values) >= maxStoredKeys {
			reply.Error = "node storage capacity reached"
			return reply
		}
		store.values[reply.Key] = req.Value
	}
	reply.Value, reply.Found = store.values[reply.Key]
	return reply
}
func (t *NetworkTransport) handleStorageRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	var req StorageRequest
	if err := decodeStorage(w, r, &req); err != nil {
		storageJSON(w, 400, StorageReply{Version: 1, Error: err.Error()})
		return
	}
	// Application errors are explicit in the validated RPC reply, including a miss.
	storageJSON(w, 200, t.applyStorage(req))
}
func (t *NetworkTransport) handleStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodGet {
		http.Error(w, "GET or PUT required", 405)
		return
	}
	id, err := t.storageKey(r.URL.Query().Get("id"))
	if err != nil {
		storageJSON(w, 400, StorageReply{Version: 1, Error: err.Error()})
		return
	}
	req := StorageRequest{Version: 1, Operation: "get", Key: id.String()}
	if r.Method == http.MethodPut {
		var body struct {
			Value *string `json:"value"`
		}
		if err := decodeStorage(w, r, &body); err != nil || body.Value == nil {
			storageJSON(w, 400, StorageReply{Version: 1, Error: "JSON string value required"})
			return
		}
		req.Operation = "put"
		req.Value = *body.Value
		if len(req.Value) > maxValueBytes {
			storageJSON(w, 413, StorageReply{Version: 1, Error: "value exceeds 64 KiB"})
			return
		}
	}
	owner, _, err := t.node.FindSuccessor(id)
	if err != nil || owner == nil {
		storageJSON(w, 503, StorageReply{Version: 1, Key: req.Key, Error: "Chord owner lookup failed"})
		return
	}
	var reply StorageReply
	if owner.Addr == t.node.Addr {
		reply = t.applyStorage(req)
	} else {
		if err := t.SendChordRPC(owner.Addr, "/rpc/kv", req, &reply); err != nil {
			storageJSON(w, 503, StorageReply{Version: 1, Key: req.Key, Error: err.Error()})
			return
		}
	}
	if reply.Version != 1 || reply.Key != req.Key || reply.Owner != owner.ID.String() || reply.Address != owner.Addr {
		storageJSON(w, 502, StorageReply{Version: 1, Error: "invalid owner response"})
		return
	}
	status := 200
	if reply.Error != "" {
		status = 409
	} else if !reply.Found {
		status = 404
	}
	storageJSON(w, status, reply)
}

// Read-only diagnostics: list records physically resident in this process, not
// records inferred from the controller's owner oracle. Values are previewed.
func (t *NetworkTransport) handleLocalStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", 405)
		return
	}
	type entry struct {
		Key       string `json:"key"`
		Preview   string `json:"value_preview"`
		Bytes     int    `json:"value_bytes"`
		Truncated bool   `json:"truncated"`
	}
	entries := make([]entry, 0)
	t.store.mu.RLock()
	for key, value := range t.store.values {
		preview := []rune(value)
		truncated := len(preview) > 160
		if truncated {
			preview = preview[:160]
		}
		entries = append(entries, entry{key, string(preview), len(value), truncated})
	}
	t.store.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		a, _ := new(big.Int).SetString(entries[i].Key, 10)
		b, _ := new(big.Int).SetString(entries[j].Key, 10)
		return a.Cmp(b) < 0
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"version": 1, "owner": t.node.ID.String(), "address": t.node.Addr, "count": len(entries), "entries": entries})
}
