package main

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"
)

func TestCompactWireRoundTripAndSize(t *testing.T) {
	peer := &NodeRef{ID: big.NewInt(42), Addr: "127.0.0.1:12345"}
	u := Update{Peer: peer, State: Alive, Incarnation: math.MaxUint64}
	for _, kind := range []string{"PING", "ACK", "PING_REQ", "JOIN", "JOIN_ACK"} {
		msg := SWIMMessage{Version: wireVersion, M: 8, Type: kind, ProbeID: "42/1/1", Origin: peer, From: u, Target: &u, Witness: &u, Updates: []Update{u}, View: []Update{u}}
		raw, err := encodeSWIM(msg)
		if err != nil {
			t.Fatal(err)
		}
		legacy, _ := json.Marshal(msg)
		if len(raw) >= len(legacy) {
			t.Fatal("compact payload did not shrink")
		}
		got, compact, err := decodeSWIM(raw)
		if err != nil || !compact || got.Type != kind || got.Witness.Incarnation != math.MaxUint64 || !samePeer(got.Target.Peer, peer) || len(got.Updates) != 0 || len(got.View) != 1 {
			t.Fatalf("roundtrip: %+v %v", got, err)
		}
		old, compact, err := decodeSWIM(legacy)
		if err != nil || compact || old.From.Incarnation != math.MaxUint64 {
			t.Fatal("legacy decode failed")
		}
	}
}
func TestCompactWireRejectsMalformed(t *testing.T) {
	for _, raw := range []string{
		`{"v":3}`, `{"v":2}`, `{"v":2,"extra":true}`,
		`{"v":2,"m":8,"p":[{"id":1,"addr":"a"}],"f":{"p":2}}`,
		`{"v":2,"m":8,"p":[{"id":1,"addr":"a"},{"id":1,"addr":"b"}],"f":{"p":0}}`,
		`{"v":2} {}`, `{"version":1,"extra":1}`,
	} {
		if _, _, err := decodeSWIM([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
