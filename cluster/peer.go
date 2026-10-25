package cluster

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"iron-bank/proto"
)

type ClusterMessage struct {
	Type     string 
	SenderID uint64      
	HTTPAddr string      
	TCPAddr  string      
	Role     string      
}

type PeerInfo struct {
	ID       uint64    
	HTTPAddr string    
	TCPAddr  string    
	Role     string    
	LastSeen time.Time 
}

type PeerMap struct {
	mu    sync.RWMutex
	peers map[uint64]*PeerInfo
}

const (
	MsgAnnounce           = "ANNOUNCE"
	MsgAnnounceAck        = "ANNOUNCE_ACK"
	MsgAppendEntries      = 1
	MsgRequestVote        = 2
	MsgAppendEntriesReply = 3
	MsgRequestVoteReply   = 4
)

func NewPeerMap() *PeerMap {
	return &PeerMap{peers: make(map[uint64]*PeerInfo)}
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

func (pm *PeerMap) Remove(id uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, id)
}

func (pm *PeerMap) Get(id uint64) (PeerInfo, bool) {
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

func (pm *PeerMap) OtherPeers(selfID uint64) []PeerInfo {
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
		if msg.SenderID != 0 && msg.SenderID != self.ID {
			pm.Add(PeerInfo{
				ID:       msg.SenderID,
				HTTPAddr: msg.HTTPAddr,
				TCPAddr:  msg.TCPAddr,
				Role:     msg.Role,
				LastSeen: time.Now(),
			})
			log.Printf("[peers] registered peer %d (%s, %s)", msg.SenderID, msg.TCPAddr, msg.Role)
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

func (pm *PeerMap) AnnounceTo(
	targetTCP string, 
	self *NodeState, 
	timeout time.Duration, 
	ctx context.Context) error {
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

	if reply.Type == MsgAnnounceAck && reply.SenderID != 0 && reply.SenderID != self.ID {
		pm.Add(PeerInfo{
			ID:       reply.SenderID,
			HTTPAddr: reply.HTTPAddr,
			TCPAddr:  reply.TCPAddr,
			Role:     reply.Role,
			LastSeen: time.Now(),
		})
		log.Printf("[peers] announced to peer %d (%s, %s)", reply.SenderID, reply.TCPAddr, reply.Role)
	}

	if reply.Role == RoleFollower && self.Role == RoleLeader {
		send := make(chan *proto.AppendEntriesArgs)
		go sendAndListenToFollower(
			targetTCP, 
			send, 
			ctx, 
			time.Duration(1*reply.SenderID)*(time.Second), 
			self)
	} else if reply.Role == RoleLeader && self.Role == RoleFollower {
		go StartAndMonitor(self)
	}

	return nil
}

func (pm *PeerMap) BroadcastAnnouncement(targets []string, self *NodeState, ctx context.Context) {
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

func sendAndListenToFollower(
	targetTCP string, 
	send chan *proto.AppendEntriesArgs, 
	ctx context.Context, 
	interval time.Duration, 
	self *NodeState) error {
	var(
		PrevLogIndex, PrevLogTerm, LeaderCommit uint64
		dialer net.Dialer
		conn net.Conn
		out []byte
		err error
	)

	for {
		conn, err = dialer.DialContext(ctx, "tcp", targetTCP)
		if err == nil {
			break
		}
	}
	writer := bufio.NewWriter(conn)
	defer conn.Close()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case data := <-send:
			PrevLogIndex = data.PrevLogIndex
			PrevLogTerm = data.PrevLogTerm
			LeaderCommit = data.LeaderCommit

			typeBuf := []byte{MsgAppendEntries}
			out = append(out, typeBuf...)
			payload := proto.MustMarshal(data)
			size := uint64(len(payload))
			binary.LittleEndian.PutUint64(out, size)
			out = append(out, payload...)
			if _, err := writer.Write(out); err != nil {
				//handle this later on
			}
			if err := writer.Flush(); err != nil {
				fmt.Println("Failed to flush packet stream to wire:", err)
			}
			
			// keep a timeout for here otherwise this will become a blocked read
			_, err := io.ReadFull(conn, header)
			if err != nil {
				fmt.Println("Connection closed or read error:", err)
				return err
			}
			length := binary.LittleEndian.Uint64(header)

			read := make([]byte, length)
			_, err = io.ReadFull(conn, read)
			if err != nil {
				fmt.Println("Failed to read complete payload:", err)
				return err
			}

			reply := &proto.AppendEntriesReply{}
			proto.MustUnmarshal(payload, reply)

			if !reply.Success {
				//access previous entries and check where is it lagging behind
			} else {
				ticker.Reset(interval)
				continue
			}

		case t := <-ticker.C:
			emptyPayload := &proto.AppendEntriesArgs{
				Term:         self.Term(),
				LeaderID:     self.ID,
				PrevLogIndex: PrevLogIndex,
				PrevLogTerm:  PrevLogTerm,
				LeaderCommit: LeaderCommit,
				Entries:      []*proto.LogEntry{},
			}
			payload := proto.MustMarshal(emptyPayload)
			size := uint64(len(payload))
			header := make([]byte, 8)
			binary.LittleEndian.PutUint64(header, size)

			if _, err := conn.Write([]byte{MsgAppendEntries}); err != nil {
				return err
			}
			if _, err := conn.Write(header); err != nil {
				return err
			}
			if _, err := conn.Write(payload); err != nil {
				return err
			}
			fmt.Printf("Executed at: %s\n", t.Format("15:04:05"))
		case <-ctx.Done():
			fmt.Println("[Goroutine] Context cancelled cleaning up and exiting...")
			return ctx.Err()
		}
	}
}

func StartAndMonitor(self *NodeState) {
	var(
		tcpLn net.Listener
		out []byte
		err error
	)
	
	listenChan := make(chan []byte)
	
	for {
		tcpLn, err = net.Listen("tcp", self.TCPAddr)
		if err != nil {
			fmt.Println("error on starting a TCP listener:", err)
			continue
		}
		break
	}
	go listenAndReply(listenChan, self, tcpLn)

	ticker := time.NewTicker(time.Duration(float64(self.ID) * 2.5) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case stream := <-listenChan:
			if stream[0] == MsgAppendEntries {
				data := &proto.AppendEntriesArgs{}
				proto.MustUnmarshal(stream[10:], data)
				var reply *proto.AppendEntriesReply
				
				if data.Term < self.Term() {
					reply = &proto.AppendEntriesReply{
						Term:          self.Term(),
						ConflictIndex: 0,
						ConflictTerm:  0,
						Success:       false,
					}
				} else if data.Term == self.Term() {
					entry, err := self.WAL.GetLastEntryInLog()
					if err != nil {
						//handle this later on
					}
					
					if data.PrevLogIndex == entry.Index {
						if data.PrevLogTerm == entry.Term {
							if len(data.Entries) > 0 {
								self.mu.RLock()
								for _, entries := range data.Entries {
									self.RaftState.Log = append(self.RaftState.Log, entries)
								}
								fmt.Println("appended entries")
								
								reply = &proto.AppendEntriesReply{
									Term:          self.Term(),
									ConflictIndex: 0,
									ConflictTerm:  0,
									Success:       true,
								}
								self.mu.RUnlock()
							} else  {
								//empty entries meaning it's a liveness signal
							}
						}
					} else if  data.PrevLogIndex > entry.Index {
						reply = &proto.AppendEntriesReply{
							Term:          self.Term(),
							ConflictIndex: entry.Index,
							ConflictTerm:  0,
							Success:       false,
						}
					} else {
						reply = &proto.AppendEntriesReply{
							Term:          self.Term(),
							ConflictIndex: entry.Index,
							ConflictTerm:  0,
							Success:       false,
						}
					}
				} else {
					self.Modify(RoleFollower, &proto.RaftState{ Term: data.Term, VoteFor: -1 })
				}

				typebuf := []byte{MsgAppendEntriesReply}
				out = append(out, typebuf...)
				payload := proto.MustMarshal(reply)
				binary.LittleEndian.AppendUint64(out, uint64(len(payload)))
				out = append(out, payload...)
				listenChan <-out
				out = nil
			}
			// case t := <-ticker.C:
			// start election
		}
	}
}

func listenAndReply(listen chan []byte, self *NodeState, tcpLn net.Listener) {
	var(
		typeMsg [1]byte
		header [8]byte
		ne net.Error
	)

	defer tcpLn.Close()
	for {
		conn, err := tcpLn.Accept()
		if err != nil {
			fmt.Println("error on listening for a TCP connection:", err)
			continue
		}

		writer := bufio.NewWriter(conn)
		wait := time.Duration(self.ID)
		conn.SetReadDeadline(time.Now().Add(wait * time.Second))

		if _, err := io.ReadFull(conn, typeMsg[:]); err != nil {
			if errors.As(err, &ne) && ne.Timeout() {
				//handle this later
			}
		} else {
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))

			if _, err := io.ReadFull(conn, header[:]); err != nil {
				//handle this later
			}

			n := binary.LittleEndian.Uint64(header[:])
			payload := make([]byte, n + 1)
			if _, err := io.ReadFull(conn, payload[:]); err != nil {
				//handle this later
			}
			payload = append(payload, typeMsg[0])
			listen<- payload[:]
			
			reply := <-listen
			if _, err := writer.Write(reply); err != nil {
				//handle this later on
			}
			if err := writer.Flush(); err != nil {
				fmt.Println("Failed to flush packet stream to wire:", err)
			}
		}
	}
}