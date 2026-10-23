package main

import (
	"fmt"
	"strings"
	"sync"
)

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
	RoleObserver = "observer"
	RoleUnknown  = "unknown"
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

type NodeState struct {
	ID       string
	HTTPAddr string
	TCPAddr  string
	Role     string
	Term     uint64
	VotedFor int64
	mu       sync.RWMutex
}

func NewNodeState(id, httpAddr, tcpAddr, role string, term uint64, votedFor int64) *NodeState {
	return &NodeState{
		ID:       id,
		HTTPAddr: httpAddr,
		TCPAddr:  tcpAddr,
		Role:     role,
		Term:     term,
		VotedFor: votedFor,
	}
}

func (ns *NodeState) MergeFlags(id, httpAddr, tcpAddr string, role string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if id != "" {
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

func (ns *NodeState) ModifyNodeState(term uint64, votedFor int64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Term = term
	ns.VotedFor = votedFor
}

func (ns *NodeState) Modify(role string, term uint64, votedFor int64) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if role != "" {
		ns.Role = role
	}
	ns.Term = term
	ns.VotedFor = votedFor
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
		Term:     ns.Term,
		VotedFor: ns.VotedFor,
	}
}

func (ns *NodeState) Save() error {
	return nil
}

