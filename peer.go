package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type MessageType string

const (
	MsgAnnounce    MessageType = "ANNOUNCE"
	MsgAnnounceAck MessageType = "ANNOUNCE_ACK"
)

type ClusterMessage struct {
	Type     MessageType `json:"type"`
	SenderID string      `json:"sender_id"`
	HTTPAddr string      `json:"http_addr"`
	TCPAddr  string      `json:"tcp_addr"`
	Role     string      `json:"role"`
}

type PeerInfo struct {
	ID       string    `json:"id"`
	HTTPAddr string    `json:"http_addr"`
	TCPAddr  string    `json:"tcp_addr"`
	Role     string    `json:"role"`
	LastSeen time.Time `json:"last_seen,omitempty"`
}

type PeerMap struct {
	mu    sync.RWMutex
	peers map[string]*PeerInfo
}

func NewPeerMap() *PeerMap {
	return &PeerMap{peers: make(map[string]*PeerInfo)}
}

func (pm *PeerMap) Add(p PeerInfo) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if p.LastSeen.IsZero() {
		p.LastSeen = time.Now()
	}
	pm.peers[p.ID] = &p
}

func (pm *PeerMap) Remove(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, id)
}

func (pm *PeerMap) Get(id string) (PeerInfo, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, ok := pm.peers[id]
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

func (pm *PeerMap) Followers() []PeerInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	var out []PeerInfo
	for _, p := range pm.peers {
		if p.Role == RoleFollower {
			out = append(out, *p)
		}
	}
	return out
}

func (pm *PeerMap) Leader() (PeerInfo, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, p := range pm.peers {
		if p.Role == RoleLeader {
			return *p, true
		}
	}
	return PeerInfo{}, false
}

func (pm *PeerMap) OtherPeers(selfID string) []PeerInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	var out []PeerInfo
	for _, p := range pm.peers {
		if p.ID != selfID {
			out = append(out, *p)
		}
	}
	return out
}

func (pm *PeerMap) QuorumSize(includeSelf bool) int {
	total := len(pm.peers)
	if includeSelf {
		total++
	}
	return (total / 2) + 1
}

func (pm *PeerMap) HandleTCPConnection(conn net.Conn, self PeerInfo) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	var msg ClusterMessage
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	switch msg.Type {
	case MsgAnnounce:
		if msg.SenderID != "" && msg.SenderID != self.ID {
			pm.Add(PeerInfo{
				ID:       msg.SenderID,
				HTTPAddr: msg.HTTPAddr,
				TCPAddr:  msg.TCPAddr,
				Role:     msg.Role,
				LastSeen: time.Now(),
			})
			log.Printf("[peers] registered peer %s (%s, %s)", msg.SenderID, msg.TCPAddr, msg.Role)
		}

		// Reply with self identity
		ack := ClusterMessage{
			Type:     MsgAnnounceAck,
			SenderID: self.ID,
			HTTPAddr: self.HTTPAddr,
			TCPAddr:  self.TCPAddr,
			Role:     self.Role,
		}
		_ = json.NewEncoder(conn).Encode(ack)
	}
}

func (pm *PeerMap) AnnounceTo(targetTCP string, self PeerInfo, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", targetTCP, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	msg := ClusterMessage{
		Type:     MsgAnnounce,
		SenderID: self.ID,
		HTTPAddr: self.HTTPAddr,
		TCPAddr:  self.TCPAddr,
		Role:     self.Role,
	}
	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return err
	}

	var reply ClusterMessage
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		return err
	}

	if reply.Type == MsgAnnounceAck && reply.SenderID != "" && reply.SenderID != self.ID {
		pm.Add(PeerInfo{
			ID:       reply.SenderID,
			HTTPAddr: reply.HTTPAddr,
			TCPAddr:  reply.TCPAddr,
			Role:     reply.Role,
			LastSeen: time.Now(),
		})
		log.Printf("[peers] announced to peer %s (%s, %s)", reply.SenderID, reply.TCPAddr, reply.Role)
	}
	return nil
}

func (pm *PeerMap) BroadcastAnnouncement(targets []string, self PeerInfo) {
	for _, target := range targets {
		addr := strings.TrimSpace(target)
		if addr == "" || addr == self.TCPAddr {
			continue
		}
		go func(targetAddr string) {
			_ = pm.AnnounceTo(targetAddr, self, 2*time.Second)
		}(addr)
	}
}

// returns a human-readable representation of the peer map.
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
