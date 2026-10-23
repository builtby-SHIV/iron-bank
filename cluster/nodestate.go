package cluster

import (
	"fmt"
	"iron-bank/proto"
	"strings"
	"sync"
)

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
	RoleObserver = "observer"
	RoleUnknown  = "unknown"
)

type NodeState struct {
	ID        uint64
	HTTPAddr  string
	TCPAddr   string
	Role      string
	statePath string
	raftState *proto.RaftState
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
	return &NodeState{
		ID:        id,
		HTTPAddr:  httpAddr,
		TCPAddr:   tcpAddr,
		Role:      role,
		raftState: raftState,
	}
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
	defer ns.mu.Unlock()
	if role != "" {
		ns.Role = role
	}
	ns.raftState = newRaftState

	//persist the data into another separate file and every write should overwrite the previous data
}

func (ns *NodeState) UpdateRole(role string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Role = role
	return nil
}

func (ns *NodeState) Snapshot() NodeState {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return NodeState{
		ID:       ns.ID,
		HTTPAddr: ns.HTTPAddr,
		TCPAddr:  ns.TCPAddr,
		Role:     ns.Role,
		raftState: ns.raftState,
	}
}

func (ns *NodeState) Save() error {
	return nil
}

