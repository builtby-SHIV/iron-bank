package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	RoleLeader = "leader"
	RoleFollower = "follower"
	RoleObserver = "observer"
	RoleUnknown = "unknown"

	nodeStateFile = "node.json"
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
	ID        string       `json:"id"`
	HTTPAddr  string       `json:"http_addr"`
	TCPAddr   string       `json:"tcp_addr"`
	Role      string       `json:"role"`
	statePath string       `json:"-"`
	mu        sync.RWMutex `json:"-"`
}

func LoadOrInitNodeState(dataDir string) (*NodeState, error) {
	statePath := filepath.Join(dataDir, nodeStateFile)

	data, err := os.ReadFile(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &NodeState{statePath: statePath}, nil
		}
		return nil, fmt.Errorf("reading %s: %w", statePath, err)
	}

	var ns NodeState
	if err := json.Unmarshal(data, &ns); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", statePath, err)
	}
	ns.statePath = statePath
	return &ns, nil
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

func (ns *NodeState) UpdateRole(role string) error {
	ns.mu.Lock()
	ns.Role = role
	ns.mu.Unlock()
	return ns.Save()
}

func (ns *NodeState) Snapshot() NodeState {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return NodeState{
		ID:       ns.ID,
		HTTPAddr: ns.HTTPAddr,
		TCPAddr:  ns.TCPAddr,
		Role:     ns.Role,
	}
}

func (ns *NodeState) Save() error {
	ns.mu.RLock()
	payload := struct {
		ID       string   `json:"id"`
		HTTPAddr string   `json:"http_addr"`
		TCPAddr  string   `json:"tcp_addr"`
		Role     string   `json:"role"`
	}{ID: ns.ID, HTTPAddr: ns.HTTPAddr, TCPAddr: ns.TCPAddr, Role: ns.Role}
	ns.mu.RUnlock()

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(ns.statePath), 0755); err != nil {
		return fmt.Errorf("creating directory for node state: %w", err)
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
