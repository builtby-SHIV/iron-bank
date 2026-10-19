package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

type PeerInfo struct {
	ID       string   `json:"id"`
	HTTPAddr string   `json:"http_addr"`
	TCPAddr  string   `json:"tcp_addr"`
	Role     string   `json:"role"`
}

type PeerMap struct {
	mu    sync.RWMutex
	peers map[string] *PeerInfo 
}

func NewPeerMap() *PeerMap {
	return &PeerMap{peers: make(map[string]*PeerInfo)}
}

func (pm *PeerMap) Add(p PeerInfo) {
	pm.mu.Lock()
	pm.peers[p.ID] = &p
	pm.mu.Unlock()
}

func (pm *PeerMap) Remove(id string) {
	pm.mu.Lock()
	delete(pm.peers, id)
	pm.mu.Unlock()
}

func (pm *PeerMap) Get(id string) (PeerInfo, bool) {
	pm.mu.RLock()
	p, ok := pm.peers[id]
	pm.mu.RUnlock()
	if !ok {
		return PeerInfo{}, false
	}
	return *p, true
}

func (pm *PeerMap) Snapshot() []PeerInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	out := make([]PeerInfo, 0, len(pm.peers))
	for _, p := range pm.peers {
		out = append(out, *p)
	}
	return out
}

//returns a human-readable representation of the peer map.
func (pm *PeerMap) String() string {
	peers := pm.Snapshot()
	var b strings.Builder
	fmt.Fprintf(&b, "PeerMap (%d peers):\n", len(peers))
	for _, p := range peers {
		data, _ := json.MarshalIndent(p, "  ", "  ")
		fmt.Fprintf(&b, "  %s\n", data)
	}
	return b.String()
}
