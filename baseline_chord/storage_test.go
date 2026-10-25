package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func storageActor(t *testing.T, id int64) (*NetworkTransport, *httptest.Server) {
	t.Helper()
	transport := NewNetworkTransport(time.Second, NewMetricsCollector())
	node := NewChordNode(big.NewInt(id), "", 8, 4, transport, NewMetricsCollector())
	transport.SetNode(node)
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc/ping", transport.handlePing)
	mux.HandleFunc("/rpc/find_successor", transport.handleFindSuccessor)
	mux.HandleFunc("/rpc/predecessor_and_list", transport.handlePredecessorAndList)
	mux.HandleFunc("/kv", transport.handleStorage)
	mux.HandleFunc("/kv/local", transport.handleLocalStorage)
	mux.HandleFunc("/rpc/kv", transport.handleStorageRPC)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	node.Addr = strings.TrimPrefix(server.URL, "http://")
	self := &NodeRef{ID: node.ID, Addr: node.Addr}
	node.Successor = self
	node.SuccessorList = []*NodeRef{self}
	for i := range node.FingerTable {
		node.FingerTable[i] = self
	}
	return transport, server
}
func storageCall(t *testing.T, server *httptest.Server, method, id, body string) (int, StorageReply) {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+"/kv?id="+id, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result StorageReply
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, result
}
func TestStorageRoutedPutGetAndIsolation(t *testing.T) {
	a, sa := storageActor(t, 0)
	b, sb := storageActor(t, 128)
	ar := &NodeRef{ID: a.node.ID, Addr: a.node.Addr}
	br := &NodeRef{ID: b.node.ID, Addr: b.node.Addr}
	a.node.singleton = false
	a.node.Predecessor = br
	a.node.Successor = br
	a.node.SuccessorList = []*NodeRef{br}
	b.node.singleton = false
	b.node.Predecessor = ar
	b.node.Successor = ar
	b.node.SuccessorList = []*NodeRef{ar}
	status, put := storageCall(t, sa, "PUT", "80", `{"value":"hello"}`)
	if status != 200 || !put.Found || put.Owner != "128" || put.Value != "hello" {
		t.Fatalf("put=%d %+v", status, put)
	}
	if _, ok := a.store.values["80"]; ok {
		t.Fatal("value stored on initiating non-owner")
	}
	status, get := storageCall(t, sa, "GET", "80", "")
	if status != 200 || get.Value != "hello" || get.Owner != "128" {
		t.Fatalf("get=%d %+v", status, get)
	}
	_, get = storageCall(t, sb, "GET", "80", "")
	if get.Value != "hello" {
		t.Fatal(get)
	}
	status, _ = storageCall(t, sa, "GET", "81", "")
	if status != 404 {
		t.Fatal("missing value not 404")
	}
	// A different transport (a separate variant/process) cannot see this store.
	_, other := storageActor(t, 0)
	status, _ = storageCall(t, other, "GET", "80", "")
	if status != 404 {
		t.Fatal("stores shared across nodes")
	}
	rejected := a.applyStorage(StorageRequest{Version: 1, Operation: "put", Key: "80", Value: "wrong"})
	if rejected.Error == "" {
		t.Fatal("non-owner accepted direct write")
	}
}
func TestStorageValidationAndCapacity(t *testing.T) {
	tr, s := storageActor(t, 0)
	for _, tc := range []struct {
		method, id, body string
		status           int
	}{
		{"GET", "256", "", 400}, {"GET", "-1", "", 400}, {"PUT", "1", `{}`, 400},
		{"PUT", "1", `{"value":2}`, 400}, {"PUT", "1", `{"value":"a","extra":1}`, 400},
		{"PUT", "1", `{"value":"a"} {}`, 400}, {"PUT", "1", `{"value":""}`, 200},
		{"PUT", "1", `{"value":"` + strings.Repeat("x", maxValueBytes+1) + `"}`, 413},
	} {
		status, _ := storageCall(t, s, tc.method, tc.id, tc.body)
		if status != tc.status {
			t.Errorf("%s %s status %d want %d", tc.method, tc.id, status, tc.status)
		}
	}
	for i := 0; i < maxStoredKeys; i++ {
		tr.store.values[fmt.Sprintf("entry-%d", i)] = "x"
	}
	reply := tr.applyStorage(StorageRequest{Version: 1, Operation: "put", Key: "2"})
	if reply.Error == "" {
		t.Fatal("unbounded store")
	}
	body := bytes.NewBufferString(`{"version":2,"operation":"put","key":"2","value":"x"}`)
	resp, err := s.Client().Post(s.URL+"/rpc/kv", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var invalid StorageReply
	_ = json.NewDecoder(resp.Body).Decode(&invalid)
	if invalid.Error == "" {
		t.Fatal("invalid version accepted")
	}
}
func TestStorageConcurrentAndRestartLoss(t *testing.T) {
	tr, _ := storageActor(t, 0)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := fmt.Sprint(i)
				r := tr.applyStorage(StorageRequest{Version: 1, Operation: "put", Key: key, Value: "v"})
				if r.Error != "" {
					t.Error(r.Error)
				}
				_ = tr.applyStorage(StorageRequest{Version: 1, Operation: "get", Key: key})
			}
		}(i)
	}
	wg.Wait()
	restarted, _ := storageActor(t, 0)
	r := restarted.applyStorage(StorageRequest{Version: 1, Operation: "get", Key: "1"})
	if r.Found {
		t.Fatal("memory storage unexpectedly survived restart")
	}
}

func TestLocalStorageListsActualRecords(t *testing.T) {
	tr, s := storageActor(t, 0)
	tr.applyStorage(StorageRequest{Version: 1, Operation: "put", Key: "80", Value: "hello"})
	tr.applyStorage(StorageRequest{Version: 1, Operation: "put", Key: "3", Value: strings.Repeat("é", 200)})
	response, err := s.Client().Get(s.URL + "/kv/local")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Owner   string `json:"owner"`
		Count   int    `json:"count"`
		Entries []struct {
			Key       string `json:"key"`
			Preview   string `json:"value_preview"`
			Bytes     int    `json:"value_bytes"`
			Truncated bool   `json:"truncated"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 2 || body.Owner != "0" || body.Entries[0].Key != "3" || body.Entries[1].Key != "80" || body.Entries[1].Preview != "hello" {
		t.Fatalf("unexpected actual records: %+v", body)
	}
	if len([]rune(body.Entries[0].Preview)) != 160 || body.Entries[0].Bytes != 400 || !body.Entries[0].Truncated {
		t.Fatal("preview not bounded correctly")
	}
}
