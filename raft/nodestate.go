package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"iron-bank/proto"
	"iron-bank/storage"
)

type Role string

type NodeState struct {
	ID				uint64
	Role			Role
	CommitIndex		uint64
	statePath		string
	Transport		Transport
	RaftState		*proto.RaftState
	WAL				*storage.WAL
	mu				sync.RWMutex
}

const (
	RoleLeader Role   = "leader"
	RoleFollower Role = "follower"
	RoleCandidate Role = "candidate"
	RoleUnknown Role = "unknown"
	nodeStateFile = "raft_state.bin"
	WALDirectorySuffix = "_wal"
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
	role, 
	dir string, 
	raftState *proto.RaftState) *NodeState {
	// ctx, cancel := context.WithCancel(context.Background())

	wal, err := storage.OpenWAL(dir + WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		// cancel()
		return nil
	}
	
	if raftState == nil {
		raftState = &proto.RaftState{
			Term: 0, 
			VoteFor: -1,
		}
	}
	return &NodeState{
		ID:        id,
		HTTPAddr:  httpAddr,
		TCPAddr:   tcpAddr,
		Role:      role,
		RaftState: raftState,
		WAL: 	   wal,
	}
}

func LoadOrInitNodeState(dataDir string) (*NodeState, error) {
	// _, cancel := context.WithCancel(context.Background())

	wal, err := storage.OpenWAL(dataDir + WALDirectorySuffix, true, 128000, 1000)
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
					VoteFor: -1,
					Log: [] *proto.LogEntry{},
				},
				WAL: wal,
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
			Log: [] *proto.LogEntry{},
		}
	}

	payload := &proto.RaftState{
		Term:    ns.RaftState.Term,
		VoteFor: ns.RaftState.VoteFor,
		Log: 	 ns.RaftState.Log,
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
	
}

func (ns *NodeState) runFollower() Role {
	
}

func (ns *NodeState) runCandidate() Role {
	
}
// func (ns *NodeState) Term() uint64 {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	if ns.RaftState != nil {
// 		return ns.RaftState.Term
// 	}
// 	return 0
// }

// func (ns *NodeState) VoteFor() int64 {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	if ns.RaftState != nil {
// 		return ns.RaftState.VoteFor
// 	}
// 	return -1
// }

// func (ns *NodeState) Log() [] *proto.LogEntry {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	if ns.RaftState != nil {
// 		return ns.RaftState.Log
// 	}
// 	return [] *proto.LogEntry{}
// }

// func (ns *NodeState) Snapshot() NodeState {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	var rs *proto.RaftState
// 	if ns.RaftState != nil {
// 		rs = &proto.RaftState{
// 			Term:    ns.RaftState.Term,
// 			VoteFor: ns.RaftState.VoteFor,
// 		}
// 	} else {
// 		rs = &proto.RaftState{
// 			Term:    0,
// 			VoteFor: -1,
// 		}
// 	}
// 	return NodeState{
// 		ID:        ns.ID,
// 		HTTPAddr:  ns.HTTPAddr,
// 		TCPAddr:   ns.TCPAddr,
// 		Role:      ns.Role,
// 		statePath: ns.statePath,
// 		RaftState: rs,
// 	}
// }

// func (ns *NodeState) NodeID() uint64 {
// 	return ns.ID
// }

// func (ns *NodeState) CurrentTerm() uint64 {
// 	return ns.Term()
// }

// func (ns *NodeState) GetRole() string {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	return ns.Role
// }

// func (ns *NodeState) LastLogIndex() uint64 {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	if ns.WAL != nil {
// 		if entry, err := ns.WAL.GetLastEntryInLog(); err == nil && entry != nil {
// 			return entry.Index
// 		}
// 	}
// 	if ns.RaftState != nil && len(ns.RaftState.Log) > 0 {
// 		return ns.RaftState.Log[len(ns.RaftState.Log)-1].Index
// 	}
// 	return 0
// }

// func (ns *NodeState) LastLogTerm() uint64 {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	if ns.WAL != nil {
// 		if entry, err := ns.WAL.GetLastEntryInLog(); err == nil && entry != nil {
// 			return entry.Term
// 		}
// 	}
// 	if ns.RaftState != nil && len(ns.RaftState.Log) > 0 {
// 		return ns.RaftState.Log[len(ns.RaftState.Log)-1].Term
// 	}
// 	return 0
// }

// // CommitIndex returns the leader's known committed log index.
// // func (ns *NodeState) CommitIndex() uint64 {
// // 	return 0
// // }

// func (ns *NodeState) GetLogEntries(nextIndex uint64) (uint64, uint64, []*proto.LogEntry, uint64) {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()

// 	prevIndex := nextIndex - 1
// 	prevTerm := uint64(0)
// 	var entries []*proto.LogEntry

// 	if ns.RaftState != nil {
// 		for _, entry := range ns.RaftState.Log {
// 			if entry.Index == prevIndex {
// 				prevTerm = entry.Term
// 			}
// 			if entry.Index >= nextIndex {
// 				entries = append(entries, entry)
// 			}
// 		}
// 	}

// 	return prevIndex, prevTerm, entries, 0
// }

// func (ns *NodeState) HandleAppendEntries(args *proto.AppendEntriesArgs) *proto.AppendEntriesReply {
// 	ns.mu.Lock()
// 	defer ns.mu.Unlock()

// 	currentTerm := uint64(0)
// 	if ns.RaftState != nil {
// 		currentTerm = ns.RaftState.Term
// 	}

// 	if args.Term < currentTerm {
// 		return &proto.AppendEntriesReply{
// 			Term:    currentTerm,
// 			Success: false,
// 		}
// 	}

// 	if args.Term > currentTerm {
// 		ns.Role = RoleFollower
// 		if ns.RaftState != nil {
// 			ns.RaftState.Term = args.Term
// 			ns.RaftState.VoteFor = -1
// 		}
// 		_ = ns.Save()
// 	}

// 	return &proto.AppendEntriesReply{
// 		Term:    args.Term,
// 		Success: true,
// 	}
// }

// func (ns *NodeState) HandleRequestVote(args *proto.RequestVoteArgs) *proto.RequestVoteReply {
// 	ns.mu.Lock()
// 	defer ns.mu.Unlock()

// 	currentTerm := uint64(0)
// 	if ns.RaftState != nil {
// 		currentTerm = ns.RaftState.Term
// 	}

// 	if args.Term < currentTerm {
// 		return &proto.RequestVoteReply{
// 			Term:        currentTerm,
// 			VoteGranted: false,
// 		}
// 	}

// 	if args.Term > currentTerm {
// 		ns.Role = RoleFollower
// 		if ns.RaftState != nil {
// 			ns.RaftState.Term = args.Term
// 			ns.RaftState.VoteFor = -1
// 		}
// 		_ = ns.Save()
// 	}

// 	voteGranted := false
// 	if ns.RaftState != nil && (ns.RaftState.VoteFor == -1 || ns.RaftState.VoteFor == int64(args.CandidateID)) {
// 		ns.RaftState.VoteFor = int64(args.CandidateID)
// 		voteGranted = true
// 		_ = ns.Save()
// 	}

// 	return &proto.RequestVoteReply{
// 		Term:        args.Term,
// 		VoteGranted: voteGranted,
// 	}
// }

// func (ns *NodeState) BecomeFollower(term uint64) {
// 	ns.mu.Lock()
// 	ns.Role = RoleFollower
// 	if ns.RaftState != nil && term > ns.RaftState.Term {
// 		ns.RaftState.Term = term
// 		ns.RaftState.VoteFor = -1
// 	}
// 	ns.mu.Unlock()
// 	_ = ns.Save()
// }

// func (ns *NodeState) BecomeCandidate() {
// 	ns.mu.Lock()
// 	ns.Role = "candidate"
// 	if ns.RaftState != nil {
// 		ns.RaftState.Term++
// 		ns.RaftState.VoteFor = int64(ns.ID)
// 	}
// 	ns.mu.Unlock()
// 	_ = ns.Save()
// }

// // var _ RaftHooks = (*NodeState)(nil)

// func (ns *NodeState) BecomeLeader() {
// 	ns.mu.Lock()
// 	ns.Role = RoleLeader
// 	ns.mu.Unlock()
// 	_ = ns.Save()
// }