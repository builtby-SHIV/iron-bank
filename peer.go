package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MessageType string

type ClusterMessage struct {
	Type     MessageType `json:"type"`
	SenderID string      `json:"sender_id"`
	HTTPAddr string      `json:"http_addr"`
	TCPAddr  string      `json:"tcp_addr"`
	Role     string      `json:"role"`
}

type PeerInfo struct {
	ID       string    
	HTTPAddr string    
	TCPAddr  string    
	Role     string    
	LastSeen time.Time 
}

type PeerMap struct {
	mu    sync.RWMutex
	peers map[string]*PeerInfo
}

const (
	MsgAnnounce    MessageType = "ANNOUNCE"
	MsgAnnounceAck MessageType = "ANNOUNCE_ACK"
)

func NewPeerMap() *PeerMap {
	return &PeerMap{peers: make(map[string]*PeerInfo)}
}

func (pm *PeerMap) Len() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.peers)
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

func (pm *PeerMap) AnnounceTo(targetTCP string, self PeerInfo, timeout time.Duration, ctx context.Context) error {
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

	if reply.Role == RoleFollower && self.Role == RoleLeader {
		send := make(chan *AppendEntriesArgs)
		senderId, err := strconv.Atoi(reply.SenderID)
		if err != nil {
			fmt.Printf("Error with the reply's senderId %v\n", err.Error())
			return err
		}
		go establishTCPConnection(targetTCP, send, ctx, time.Duration(1 * senderId ) * (time.Second))
	} else if reply.Role == RoleLeader && self.Role == RoleFollower {
		go listenToLeader(&self)
	} 

	return nil
}

func (pm *PeerMap) BroadcastAnnouncement(targets []string, self PeerInfo, ctx context.Context) {
	for _, target := range targets {
		addr := strings.TrimSpace(target)
		if addr == "" || addr == self.TCPAddr {
			continue
		}
		go func(targetAddr string) {
			_ = pm.AnnounceTo(targetAddr, self, 2*time.Second, ctx)
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

func establishTCPConnection(targetTCP string, send chan *AppendEntriesArgs, ctx context.Context, interval time.Duration) error {
	var dialer net.Dialer
	var conn net.Conn
	var err error

	for {
		conn, err = dialer.DialContext(ctx, "tcp", targetTCP)
		if err == nil {
			break
		}
	}
	defer conn.Close()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case data := <- send:
			payload := MustMarshal(data)

			size := uint64(len(payload))
			header := make([]byte, 8)
			binary.LittleEndian.PutUint64(header, size)

			if _, err := conn.Write(header); err != nil {
				return err
			}

			if _, err := conn.Write(payload); err != nil {
				return err
			}

			_, err := io.ReadFull(conn, header) 
			if err != nil {
				fmt.Println("Connection closed or read error:", err)
				return err
			}
			length := binary.BigEndian.Uint32(header)

			read := make([]byte, length)
			_, err = io.ReadFull(conn, read)
			if err != nil {
				fmt.Println("Failed to read complete payload:", err)
				return err
			}

			reply := &AppendEntriesReply{}
			MustUnmarshal(payload, reply)

			if !reply.Success {
				//access previous entries and check where is it lagging behind
			} else {
				continue
			}
			
		case t := <- ticker.C:
			fmt.Printf("Executed at: %s\n", t.Format("15:04:05"))
		case <- ctx.Done():
			fmt.Println("[Goroutine] Context cancelled cleaning up and exiting...")
			return ctx.Err()
		}
	}
}

func listenToLeader(self *PeerInfo) {
	tcpLn, err := net.Listen("tcp", self.TCPAddr)
	if err != nil {
		fmt.Println("error on starting a TCP listener:", err)
		return
	}
	defer tcpLn.Close()

	conn, err := tcpLn.Accept()
	if err != nil {
		fmt.Println("error on listening for a TCP connection:", err)
		return
	}
	header := make([]byte, 8)
	_, err = io.ReadFull(conn, header) 
	if err != nil {
		fmt.Println("Connection closed or read error:", err)
		return
	}
	length := binary.BigEndian.Uint32(header)

	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		fmt.Println("Failed to read complete payload:", err)
		return
	}

	data := &AppendEntriesArgs{}
	MustUnmarshal(payload, data)

	_ = data
	// if data.Term < ...
}
