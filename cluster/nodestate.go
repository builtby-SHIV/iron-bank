package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"iron-bank/proto"
)

const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleObserver  = "observer"
	RoleUnknown   = "unknown"
	nodeStateFile = "raft_state.bin"
)

type NodeState struct {
	ID        uint64
	HTTPAddr  string
	TCPAddr   string
	Role      string
	statePath string
	RaftState *proto.RaftState
	mu        sync.RWMutex
}

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

func NewNodeState(id uint64, httpAddr, tcpAddr, role string, raftState *proto.RaftState) *NodeState {
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
	}
}

func LoadOrInitNodeState(dataDir string) (*NodeState, error) {
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

func (ns *NodeState) Term() uint64 {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if ns.RaftState != nil {
		return ns.RaftState.Term
	}
	return 0
}

func (ns *NodeState) VoteFor() int64 {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if ns.RaftState != nil {
		return ns.RaftState.VoteFor
	}
	return -1
}

func (ns *NodeState) Log() [] *proto.LogEntry{} {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	if ns.RaftState != nil {
		return ns.RaftState.Log
	}
	return []
}

func (ns *NodeState) Snapshot() NodeState {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	var rs *proto.RaftState
	if ns.RaftState != nil {
		rs = &proto.RaftState{
			Term:    ns.RaftState.Term,
			VoteFor: ns.RaftState.VoteFor,
		}
	} else {
		rs = &proto.RaftState{
			Term:    0,
			VoteFor: -1,
		}
	}
	return NodeState{
		ID:        ns.ID,
		HTTPAddr:  ns.HTTPAddr,
		TCPAddr:   ns.TCPAddr,
		Role:      ns.Role,
		statePath: ns.statePath,
		RaftState: rs,
	}
}