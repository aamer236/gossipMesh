package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Version 2 interns identities once per message. Protocol semantics remain v1.
// Legacy requests receive legacy replies; new clients require v2-capable servers.
type wireUpdate struct {
	Peer        int         `json:"p"`
	State       MemberState `json:"s"`
	Incarnation uint64      `json:"i"`
}
type compactMessage struct {
	Version int          `json:"v"`
	M       int          `json:"m"`
	Type    string       `json:"t"`
	ProbeID string       `json:"q"`
	Peers   []*NodeRef   `json:"p"`
	Origin  int          `json:"o"`
	From    *wireUpdate  `json:"f"`
	Target  *wireUpdate  `json:"d,omitempty"`
	Witness *wireUpdate  `json:"w,omitempty"`
	Updates []wireUpdate `json:"u,omitempty"`
	View    []wireUpdate `json:"j,omitempty"`
}

func encodeSWIM(msg SWIMMessage) ([]byte, error) {
	w := compactMessage{Version: 2, M: msg.M, Type: msg.Type, ProbeID: msg.ProbeID}
	intern := func(p *NodeRef) int {
		for i, old := range w.Peers {
			if samePeer(old, p) {
				return i
			}
		}
		w.Peers = append(w.Peers, p)
		return len(w.Peers) - 1
	}
	update := func(u Update) wireUpdate { return wireUpdate{intern(u.Peer), u.State, u.Incarnation} }
	w.Origin = intern(msg.Origin)
	from := update(msg.From)
	w.From = &from
	if msg.Target != nil {
		u := update(*msg.Target)
		w.Target = &u
	}
	if msg.Witness != nil {
		u := update(*msg.Witness)
		w.Witness = &u
	}
	for _, u := range msg.Updates {
		// From is always merged by the recipient; retransmitting its exact duplicate
		// adds no information. Other fields have different merge semantics.
		if samePeer(u.Peer, msg.From.Peer) && u.State == msg.From.State && u.Incarnation == msg.From.Incarnation {
			continue
		}
		w.Updates = append(w.Updates, update(u))
	}
	for _, u := range msg.View {
		w.View = append(w.View, update(u))
	}
	return json.Marshal(w)
}
func strictJSON(raw []byte, out interface{}) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(interface{})) != io.EOF {
		return fmt.Errorf("trailing SWIM payload")
	}
	return nil
}
func decodeSWIM(raw []byte) (SWIMMessage, bool, error) {
	var header struct {
		Version int `json:"v"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return SWIMMessage{}, false, err
	}
	if header.Version == 0 {
		var msg SWIMMessage
		err := strictJSON(raw, &msg)
		return msg, false, err
	}
	fail := func() (SWIMMessage, bool, error) {
		return SWIMMessage{}, true, fmt.Errorf("invalid compact SWIM envelope")
	}
	var w compactMessage
	if err := strictJSON(raw, &w); err != nil {
		return SWIMMessage{}, true, err
	}
	if w.M < 1 || w.M > 160 || w.Version != 2 || w.From == nil || len(w.Peers) == 0 || len(w.Peers) > 16384 || w.Origin < 0 || w.Origin >= len(w.Peers) {
		return fail()
	}
	ids, addrs := map[string]bool{}, map[string]bool{}
	for _, p := range w.Peers {
		if p == nil || !validID(p.ID, w.M) || ids[p.ID.String()] || addrs[p.Addr] {
			return fail()
		}
		ids[p.ID.String()] = true
		addrs[p.Addr] = true
	}
	valid := true
	update := func(u wireUpdate) Update {
		if u.Peer < 0 || u.Peer >= len(w.Peers) {
			valid = false
			return Update{}
		}
		return Update{Peer: w.Peers[u.Peer], State: u.State, Incarnation: u.Incarnation}
	}
	msg := SWIMMessage{Version: wireVersion, M: w.M, Type: w.Type, ProbeID: w.ProbeID, Origin: w.Peers[w.Origin], From: update(*w.From)}
	if w.Target != nil {
		u := update(*w.Target)
		msg.Target = &u
	}
	if w.Witness != nil {
		u := update(*w.Witness)
		msg.Witness = &u
	}
	for _, u := range w.Updates {
		msg.Updates = append(msg.Updates, update(u))
	}
	for _, u := range w.View {
		msg.View = append(msg.View, update(u))
	}
	if !valid {
		return fail()
	}
	return msg, true, nil
}
