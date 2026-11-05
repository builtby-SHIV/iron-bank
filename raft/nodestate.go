package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"iron-bank/proto"
	"iron-bank/storage"

	googleproto "google.golang.org/protobuf/proto"
)

type Role string

type RPCEvent struct {
	peer		string
	msgType 	byte
	sendBack	chan googleproto.Message
	msgRecv		googleproto.Message
	msgSent		googleproto.Message
}

type appendEntry struct {
	data	*proto.KVPair
	index	uint64
	term	uint64
}

type clientCmd struct {
	command    proto.Command
	key, value string
}

type LeaderState struct {
	nextIndex	map[string]uint64
	matchIndex	map[string]uint64
	inflight	map[string]bool
	pending		map[uint64]chan result // client waiting on log index N
	mu			sync.RWMutex
}

type snapShotMetaData struct {
	lastIncludedTerm, lastIncludedIndex uint64
}

type NodeState struct {
	ID          uint64
	Role        Role
	CommitIndex uint64
	statePath   string
	tp          TCPTransport
	ls			*LeaderState
	replyCh     chan *RPCEvent // incoming requests from the forwarders
	clientCh    chan clientCmd // client commands
	applyCh     chan []*proto.RaftLog
	stopCh      chan struct{}
	entries		[]*appendEntry
	RaftState   *proto.RaftState
	Log         *storage.WAL
	ssMetaData	*snapShotMetaData
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
}

const (
	RoleLeader 			  Role = "leader"
	RoleFollower          Role = "follower"
	RoleCandidate      	  Role = "candidate"
	RoleUnknown        	  Role = "unknown"
	nodeStateFile      		   = "raft_state.bin"
	WALDirectorySuffix 		   = "_wal"
	MAX_Entries			uint64 = 50
)

func ParseRole(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "leader":
		return RoleLeader, nil
	case "follower":
		return RoleFollower, nil
	case "observer":
		return RoleObserver, nil
	case "unknown", "":
		return RoleUnknown, nil
	default:
		return RoleUnknown, fmt.Errorf("unknown role %q: must be one of leader, follower, observer", s)
	}
}

func NewNodeState(
	id uint64,
	httpAddr,
	tcpAddr,
	dir string,
	role Role,
	ctx context.Context,
	raftState *proto.RaftState) *NodeState {
	wal, err := storage.OpenWAL(dir+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil
	}

	if raftState == nil {
		raftState = &proto.RaftState{
			Term:    0,
			VotedFor: "",
		}
	}

	newCtx, cancel := context.WithCancel(ctx)
	return &NodeState{
		ID:        id,
		Role:      role,
		RaftState: raftState,
		Log:       wal,
		ctx:       newCtx,
		cancel:    cancel,
	}
}

func LoadOrInitNodeState(dataDir string) (*NodeState, error) {
	// _, cancel := context.WithCancel(context.Background())

	wal, err := storage.OpenWAL(dataDir+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil, err
	}

	statePath := filepath.Join(dataDir, nodeStateFile)

	data, err := os.ReadFile(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			ns := &NodeState{
				statePath: statePath,
				RaftState: &proto.RaftState{
					Term:    0,
					VotedFor: "",
				},
				Log: wal,
			}
			if err := ns.Save(); err != nil {
				return nil, fmt.Errorf("creating %s: %w", statePath, err)
			}
			return ns, nil
		}
		return nil, fmt.Errorf("reading %s: %w", statePath, err)
	}

	var rs proto.RaftState
	proto.MustUnmarshal(data, &rs)

	return &NodeState{
		statePath: statePath,
		RaftState: &rs,
	}, nil
}

func (ns *NodeState) Save() error {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	if ns.statePath == "" {
		return nil
	}

	if ns.RaftState == nil {
		ns.RaftState = &proto.RaftState{
			Term:    0,
			VoteFor: -1,
			Log:     []*proto.RaftLog{},
		}
	}

	payload := &proto.RaftState{
		Term:    ns.RaftState.Term,
		VoteFor: ns.RaftState.VoteFor,
		Log:     ns.RaftState.Log,
	}

	data := proto.MustMarshal(payload)

	dir := filepath.Dir(ns.statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	tmp := ns.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing node state: %w", err)
	}
	if err := os.Rename(tmp, ns.statePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("committing node state: %w", err)
	}
	return nil
}

func (ns *NodeState) MergeFlags(id uint64, httpAddr, tcpAddr string, role string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if id != 0 {
		ns.ID = id
	}
	if httpAddr != "" {
		ns.HTTPAddr = httpAddr
	}
	if tcpAddr != "" {
		ns.TCPAddr = tcpAddr
	}
	if role != RoleUnknown {
		ns.Role = role
	}
	if ns.Role == "" {
		ns.Role = RoleUnknown
	}
}

func (ns *NodeState) Modify(role string, newRaftState *proto.RaftState) {
	ns.mu.Lock()
	if role != "" {
		ns.Role = role
	}
	ns.RaftState = newRaftState
	ns.mu.Unlock()

	_ = ns.Save()
}

func (ns *NodeState) UpdateRole(role string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Role = role
	return nil
}

func (ns *NodeState) run() {
	go ns.tp.ListenLoop(ns.tp.TCPAddr, ns.rpcCh)
	for {
		switch ns.Role {
		case RoleLeader:
			ns.Role = ns.runLeader()
		case RoleFollower:
			ns.Role = ns.runFollower()
		case RoleCandidate:
			ns.Role = ns.runCandidate()
		}
	}
}

func (ns *NodeState) runLeader() Role {
	ns.ls = NewLeaderState()

	interval := (time.Duration(float64(ns.ID) * 2.5)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ns.tp.mu.RLock()
			ns.formatAndSend(&proto.KVPair{})
			ns.tp.mu.RUnlock()
		case rp := <-ns.replyCh:
			ns.ls.inflight[rp.peer] = false
			switch rp.msgType {
				case TypeAppendEntriesReply:
				recv := (rp.msgRecv).(*proto.AppendEntriesReply)
				sent := (rp.msgSent).(*proto.AppendEntriesArgs)
				if recv.Term == ns.RaftState.Term {
					if recv.Success {
						ns.ls.matchIndex[rp.peer] = max(
							ns.ls.matchIndex[rp.peer], sent.PrevLogIndex + uint64(len(sent.Entries)))
						ns.ls.nextIndex[rp.peer] = ns.ls.matchIndex[rp.peer] + 1
						if ns.CommitIndex < ns.ls.matchIndex[ns.tp.TCPAddr] {
							var higherIdx, count uint64
							for _, idx := range ns.ls.matchIndex {
								if idx > higherIdx {
									higherIdx = idx
									count = 1
								} else {
									count += 1
								}
							}
							if count > uint64(len(ns.ls.matchIndex) / 2) {
								ns.CommitIndex += 1
							}
						}
					} else {
						_, args, err := ns.handleBackup(recv.ConflictIndex)
						ns.ls.nextIndex[rp.peer] = recv.ConflictIndex
						if err != nil {
							fmt.Printf("%v\n", err)
						} else {
							ns.ls.inflight[rp.peer] = true
							go ns.tp.sendToAppendEntries(rp.peer, TypeAppendEntries, args, ns.replyCh)
						}
					}
				} else if recv.Term > ns.RaftState.Term {
					// stale leader; depose to a follower
					// persist raft state to config file
					ns.tp.cancel()
					ns.Role = RoleFollower
					ns.RaftState.Term = recv.Term
					ns.RaftState.VotedFor = ""
					return RoleFollower
				}
			}
		case cmd := <-ns.clientCh:
			entry := &proto.KVPair{
				Key:     cmd.key,
				Value:   cmd.value,
				Command: cmd.command,
			}
			
			ns.ls.nextIndex[ns.tp.TCPAddr] += 1
			ns.ls.matchIndex[ns.tp.TCPAddr] += 1
			ns.entries = append(ns.entries, &appendEntry{ 
				data: entry, 
				term: ns.RaftState.Term, 
				index: ns.ls.matchIndex[ns.tp.TCPAddr],
			})
			
			ns.tp.mu.RLock()
			ns.formatAndSend(entry)
			ns.tp.mu.RUnlock()
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

func (ns *NodeState) runFollower() Role {
	lastDigit, err := strconv.ParseFloat(ns.tp.TCPAddr[len(ns.tp.TCPAddr) - 1:], 32)
	if err != nil {
		fmt.Printf("Error in parsing last digit %v\n", err)
	}
	interval := (time.Duration(lastDigit * 2.5)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// start election by becoming candidate
			return RoleCandidate
		case rp := <-ns.replyCh:
			// handle incoming messages
			switch rp.msgType {
				case TypeAppendEntries:
				recv := (rp.msgRecv).(*proto.AppendEntriesArgs)
				
				if recv.Term < ns.RaftState.Term {
					reply := &proto.AppendEntriesReply{
						Term: ns.RaftState.Term,
						ConflictIndex: 0,
						ConflictTerm: 0,
						Success: false,
					}
				} else if recv.Term > ns.RaftState.Term {
					// remember to change config file
					ticker.Reset(interval)
					ns.RaftState.Term = recv.Term
					reply, err := ns.handleAppendEntries(recv)
					if err != nil {
						fmt.Printf("Error in handling appendentriesargs %v\n", err)
						break
					}
					rp.sendBack<- reply
				} else {
					ticker.Reset(interval)
					reply, err := ns.handleAppendEntries(recv)
					if err != nil {
						fmt.Printf("Error in handling appendentriesargs %v\n", err)
						break
					}
					rp.sendBack<- reply
				}

			}
		case <-ns.ctx.Done():
			ns.cancel()
			return ""
		}
	}
}

func (ns *NodeState) runCandidate() Role {
}

func (ns *NodeState) handleBackup(idx uint64) (uint64, *proto.AppendEntriesArgs, error) {
	ns.Log.ReadRaftLog(idx, MAX_Entries)
}

func (ns *NodeState) formatAndSend(entry *proto.KVPair) {
	for addr, _ := range ns.tp.Peers {
		if addr != ns.tp.TCPAddr && !ns.ls.inflight[addr]{
			ns.ls.inflight[addr] = true
			
			prevLogIndex := ns.ls.nextIndex[addr] - 1
			var prevLogTerm uint64
			if prevLogIndex >= ns.entries[0].index {
				for _, entry := range ns.entries {
					if entry.index == prevLogIndex {
						prevLogTerm = entry.term
						break
					}
				}
				args := &proto.AppendEntriesArgs{
					Term:         ns.RaftState.Term,
					LeaderID:     ns.ID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: ns.CommitIndex,
					Entries:      []*proto.KVPair{entry},
				}
				go ns.tp.sendToAppendEntries(addr, TypeAppendEntries, args, ns.replyCh)
			} else {
				// get prevLogIndex andprevLogTerm from raft log otheriwse send snapshots
			}
		}
	}
}

func (ns *NodeState) handleAppendEntries(recv *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	var reply *proto.AppendEntriesReply
	if ns.ssMetaData.lastIncludedIndex == recv.PrevLogIndex {
		// add to raft Log
		for idx, entry := range recv.Entries {
			ns.entries = append(ns.entries, &appendEntry{ 
				data: entry, 
				index: recv.PrevLogIndex + uint64(idx) + 1, 
				term: recv.Term, 
			})
		}

		reply = &proto.AppendEntriesReply{
			Term: ns.RaftState.Term,
			ConflictIndex: 0,
			ConflictTerm: 0,
			Success: true,
		}
	} else if ns.ssMetaData.lastIncludedIndex < recv.PrevLogIndex {
		// the leader's next index for this node is too far back so it should've sent a snapshot instead
		reply = &proto.AppendEntriesReply{
			Term: ns.RaftState.Term,
			ConflictIndex: ns.ssMetaData.lastIncludedIndex + 1,
			ConflictTerm: 0,
			Success: false,
		}
	} else {
		sliceIdx := recv.PrevLogIndex - ns.ssMetaData.lastIncludedIndex - 1
		if ns.entries[sliceIdx].index == recv.PrevLogIndex {
			// add to raft Log
			for idx, entry := range recv.Entries {
				ns.entries = append(ns.entries, &appendEntry{ 
					data: entry, 
					index: recv.PrevLogIndex + uint64(idx) + 1, 
					term: recv.Term,
				})
			}
	
			reply = &proto.AppendEntriesReply{
				Term: ns.RaftState.Term,
				ConflictIndex: 0,
				ConflictTerm: 0,
				Success: true,
			}
		} else if ns.entries[sliceIdx].index < recv.PrevLogIndex {
			reply = &proto.AppendEntriesReply{
				Term: ns.RaftState.Term,
				ConflictIndex: ns.entries[sliceIdx].index + 1,
				ConflictTerm: 0,
				Success: false,
			}
		} else {
			if ns.entries[sliceIdx].term != recv.PrevLogTerm {
				ns.Log.TruncateAfterIdx(sliceIdx)
				ns.entries = slices.Delete(ns.entries, int(sliceIdx), len(ns.entries))
				// add to raft Log
				for idx, entry := range recv.Entries {
					ns.entries = append(ns.entries, &appendEntry{ 
						data: entry, 
						index: recv.PrevLogIndex + uint64(idx) + 1, 
						term: recv.Term,
					})
				}
			} else {
				for idx, entry := range recv.Entries {
			        targetIndex := recv.PrevLogIndex + uint64(idx) + 1
			        localSliceIdx := targetIndex - ns.ssMetaData.lastIncludedIndex - 1
			        
			        if localSliceIdx < uint64(len(ns.entries)) {
			            if ns.entries[localSliceIdx].term != recv.Term {
			                ns.Log.TruncateAfterIdx(localSliceIdx)
			                ns.entries = slices.Delete(ns.entries, int(localSliceIdx), len(ns.entries))
			                
			                ns.entries = append(ns.entries, &appendEntry{
			                    data:  entry,
			                    index: targetIndex,
			                    term:  recv.Term,
			                })
			            } else {
			                continue
			            }
			        } else {
			            ns.entries = append(ns.entries, &appendEntry{
			                data:  entry,
			                index: targetIndex,
			                term:  recv.Term,
			            })
			        }
			    }
			}
		}
	}
	
	ns.CommitIndex = min(ns.CommitIndex, recv.LeaderCommit)
	return reply, nil
}