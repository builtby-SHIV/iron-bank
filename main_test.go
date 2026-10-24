package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"iron-bank/cluster"
	"iron-bank/proto"
	"iron-bank/storage"
)

// ── normalizeAddr / extractPort ───────────────────────────────────────────────

func TestExtractPort(t *testing.T) {
	cases := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{":8001", "8001", false},
		{"127.0.0.1:8002", "8002", false},
		{"localhost:8003", "8003", false},
		{"0.0.0.0:8004", "8004", false},
		{"[::1]:8005", "8005", false},
		{"8006", "8006", false},
		{"  :8007  ", "8007", false},
		{"", "", true},
		{"invalid", "", true},
		{":99999", "", true},
		{":0", "", true},
		{":-1", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := extractPort(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("extractPort(%q) want error, got %q", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractPort(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("extractPort(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ── resolveConfig ─────────────────────────────────────────────────────────────

func TestResolveConfig(t *testing.T) {
	t.Run("all flags provided", func(t *testing.T) {
		cfg, err := resolveConfig("1", ":8001", ":7001", "leader", filepath.Join("data", "1"), ":7002,:7003")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.NodeID != 1 {
			t.Errorf("NodeID: got %d, want 1", cfg.NodeID)
		}
		if cfg.HTTPAddr != ":8001" {
			t.Errorf("HTTPAddr: got %q, want :8001", cfg.HTTPAddr)
		}
		if cfg.TCPAddr != ":7001" {
			t.Errorf("TCPAddr: got %q, want :7001", cfg.TCPAddr)
		}
		if cfg.Role != cluster.RoleLeader {
			t.Errorf("Role: got %q, want leader", cfg.Role)
		}
		if cfg.DataDir != filepath.Join("data", "1") {
			t.Errorf("DataDir: got %q, want %q", cfg.DataDir, filepath.Join("data", "1"))
		}
		if cfg.Peers != ":7002,:7003" {
			t.Errorf("Peers: got %q, want :7002,:7003", cfg.Peers)
		}
	})

	t.Run("id missing returns error", func(t *testing.T) {
		_, err := resolveConfig("", ":8001", ":7001", "leader", "./data", ":7002")
		if err == nil || !strings.Contains(err.Error(), "--id") {
			t.Fatalf("expected error mentioning --id, got %v", err)
		}
	})

	t.Run("id invalid format or zero returns error", func(t *testing.T) {
		for _, bad := range []string{"abc", "0", "-1"} {
			_, err := resolveConfig(bad, ":8001", ":7001", "leader", "./data", ":7002")
			if err == nil {
				t.Fatalf("expected error for --id=%q", bad)
			}
		}
	})

	t.Run("http missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", "", ":7001", "leader", "./data", ":7002")
		if err == nil || !strings.Contains(err.Error(), "--http") {
			t.Fatalf("expected error mentioning --http, got %v", err)
		}
	})

	t.Run("tcp missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", "", "leader", "./data", ":7002")
		if err == nil || !strings.Contains(err.Error(), "--tcp") {
			t.Fatalf("expected error mentioning --tcp, got %v", err)
		}
	})

	t.Run("role missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", ":7001", "", "./data", ":7002")
		if err == nil || !strings.Contains(err.Error(), "--role") {
			t.Fatalf("expected error mentioning --role, got %v", err)
		}
	})

	t.Run("data missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", ":7001", "leader", "", ":7002")
		if err == nil || !strings.Contains(err.Error(), "--data") {
			t.Fatalf("expected error mentioning --data, got %v", err)
		}
	})

	t.Run("peers missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", ":7001", "leader", "./data", "")
		if err == nil || !strings.Contains(err.Error(), "--peers") {
			t.Fatalf("expected error mentioning --peers, got %v", err)
		}
	})

	t.Run("invalid role returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", ":7001", "emperor", "./data", ":7002")
		if err == nil {
			t.Fatal("expected error for unknown role")
		}
	})

	t.Run("invalid tcp returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", "not-a-port", "leader", "./data", ":7002")
		if err == nil {
			t.Fatal("expected error for invalid tcp")
		}
	})

	t.Run("invalid http returns error", func(t *testing.T) {
		_, err := resolveConfig("1", "not-a-port", ":7001", "leader", "./data", ":7002")
		if err == nil {
			t.Fatal("expected error for invalid http")
		}
	})
}

// ── NodeState ─────────────────────────────────────────────────────────────────

func TestNodeStateModify(t *testing.T) {
	ns := cluster.NewNodeState(1, ":8001", ":7001", cluster.RoleFollower, &proto.RaftState{Term: 0, VoteFor: -1})
	snap := ns.Snapshot()
	if snap.ID != 1 || snap.Role != cluster.RoleFollower || snap.RaftState.Term != 0 || snap.RaftState.VoteFor != -1 {
		t.Fatalf("unexpected initial snapshot: %+v", &snap)
	}

	ns.Modify(cluster.RoleFollower, &proto.RaftState{Term: 3, VoteFor: 2})
	snap = ns.Snapshot()
	if snap.RaftState.Term != 3 || snap.RaftState.VoteFor != 2 {
		t.Errorf("expected term=3, votedFor=2, got %+v", &snap)
	}
}

func TestNodeStateBinaryPersistence(t *testing.T) {
	dir := t.TempDir()

	ns, err := cluster.LoadOrInitNodeState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if snap := ns.Snapshot(); snap.RaftState.Term != 0 || snap.RaftState.VoteFor != -1 {
		t.Fatalf("expected initial term=0, voteFor=-1, got %+v", &snap)
	}

	// Update and atomically persist
	ns.Modify(cluster.RoleLeader, &proto.RaftState{Term: 7, VoteFor: 3})

	// Verify file was written
	binPath := filepath.Join(dir, "raft_state.bin")
	if fi, err := os.Stat(binPath); err != nil || fi.Size() == 0 {
		t.Fatalf("raft_state.bin not created or empty: %v", err)
	}

	// Reopen on node restart
	ns2, err := cluster.LoadOrInitNodeState(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap2 := ns2.Snapshot()
	if snap2.RaftState.Term != 7 || snap2.RaftState.VoteFor != 3 {
		t.Errorf("recovered state mismatch: expected term=7, votedFor=3, got term=%d, votedFor=%d", snap2.RaftState.Term, snap2.RaftState.VoteFor)
	}
}

func TestNodeStateMergeFlags(t *testing.T) {
	ns := &cluster.NodeState{}
	ns.MergeFlags(1, ":8001", ":7001", cluster.RoleFollower)
	ns.MergeFlags(0, "", "", cluster.RoleLeader)
	snap := ns.Snapshot()

	if snap.Role != cluster.RoleLeader {
		t.Errorf("expected role=%q, got %q", cluster.RoleLeader, snap.Role)
	}
	if snap.ID != 1 || snap.HTTPAddr != ":8001" || snap.TCPAddr != ":7001" {
		t.Errorf("fields should be preserved: %+v", &snap)
	}
}

func TestNodeStateUpdateRole(t *testing.T) {
	ns := &cluster.NodeState{ID: 1, Role: cluster.RoleFollower}
	if err := ns.UpdateRole(cluster.RoleLeader); err != nil {
		t.Fatal(err)
	}
	if ns.Snapshot().Role != cluster.RoleLeader {
		t.Error("role not updated after UpdateRole")
	}
}

// ── PeerMap ───────────────────────────────────────────────────────────────────

func TestPeerMapOperations(t *testing.T) {
	pm := cluster.NewPeerMap()

	if pm.Len() != 0 {
		t.Fatal("expected empty map")
	}

	p1 := cluster.PeerInfo{ID: 1, HTTPAddr: ":8001", TCPAddr: ":7001", Role: cluster.RoleLeader}
	p2 := cluster.PeerInfo{ID: 2, HTTPAddr: ":8002", TCPAddr: ":7002", Role: cluster.RoleFollower}

	pm.Add(p1)
	pm.Add(p2)

	if pm.Len() != 2 {
		t.Fatalf("expected 2 peers, got %d", pm.Len())
	}

	got, ok := pm.Get(1)
	if !ok || got.Role != cluster.RoleLeader {
		t.Errorf("Get(1): %+v ok=%v", got, ok)
	}

	// Re-add should overwrite.
	pm.Add(cluster.PeerInfo{ID: 1, HTTPAddr: ":8001", TCPAddr: ":7001", Role: cluster.RoleFollower})
	got, _ = pm.Get(1)
	if got.Role != cluster.RoleFollower {
		t.Errorf("Add should overwrite: want follower, got %q", got.Role)
	}

	pm.Remove(2)
	if pm.Len() != 1 {
		t.Fatalf("expected 1 peer after Remove, got %d", pm.Len())
	}
	if _, ok := pm.Get(2); ok {
		t.Error("peer 2 should have been removed")
	}
}

func TestPeerMapLeaderAndFollowerHelpers(t *testing.T) {
	// From node 1's perspective: peers are 2 and 3
	pmLeader := cluster.NewPeerMap()
	pmLeader.Add(cluster.PeerInfo{ID: 2, TCPAddr: ":7002", Role: cluster.RoleFollower})
	pmLeader.Add(cluster.PeerInfo{ID: 3, TCPAddr: ":7003", Role: cluster.RoleFollower})

	// Leader helper: leader wants all followers to send messages to
	followers := pmLeader.Followers()
	if len(followers) != 2 {
		t.Fatalf("expected 2 followers, got %d", len(followers))
	}

	// Quorum size for 3-node cluster (2 peers + self): majority is 2
	if q := pmLeader.QuorumSize(true); q != 2 {
		t.Errorf("QuorumSize(true) for 3-node cluster: got %d, want 2", q)
	}

	// From node 2's perspective (follower): peers are 1 (leader) and 3 (follower)
	pmFollower := cluster.NewPeerMap()
	pmFollower.Add(cluster.PeerInfo{ID: 1, TCPAddr: ":7001", Role: cluster.RoleLeader})
	pmFollower.Add(cluster.PeerInfo{ID: 3, TCPAddr: ":7003", Role: cluster.RoleFollower})

	// Follower helper: check who the leader is
	leader, found := pmFollower.Leader()
	if !found || leader.ID != 1 {
		t.Fatalf("expected leader with ID 1, got %+v (found=%v)", leader, found)
	}

	// Follower helper: start election - get other peers to send vote requests to
	others := pmFollower.OtherPeers(2)
	if len(others) != 2 {
		t.Fatalf("expected 2 other peers for node 2, got %d", len(others))
	}
	for _, o := range others {
		if o.ID == 2 {
			t.Errorf("other peers should not include self (ID 2)")
		}
	}
}

func TestPeerMapNetworkAnnouncement(t *testing.T) {
	// Start node 1 TCP listener
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()
	addr1 := ln1.Addr().String()

	// Start node 2 TCP listener
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	addr2 := ln2.Addr().String()

	pm1 := cluster.NewPeerMap()
	self1 := cluster.PeerInfo{ID: 1, HTTPAddr: ":8001", TCPAddr: addr1, Role: cluster.RoleLeader}

	pm2 := cluster.NewPeerMap()
	self2 := &cluster.NodeState{ID: 2, HTTPAddr: ":8002", TCPAddr: addr2, Role: cluster.RoleFollower}

	// Run accept loops for both nodes
	go func() {
		for {
			conn, err := ln1.Accept()
			if err != nil {
				return
			}
			go pm1.HandleTCPConnection(conn, self1)
		}
	}()

	go func() {
		for {
			conn, err := ln2.Accept()
			if err != nil {
				return
			}
			go pm2.HandleTCPConnection(conn, cluster.PeerInfo{
				ID:       self2.ID,
				HTTPAddr: self2.HTTPAddr,
				TCPAddr:  self2.TCPAddr,
				Role:     self2.Role,
			})
		}
	}()

	// Node 2 announces itself to Node 1
	if err := pm2.AnnounceTo(addr1, self2, 1*time.Second, context.Background()); err != nil {
		t.Fatalf("node 2 failed to announce to node 1: %v", err)
	}

	// Verify node 1 received node 2's announcement
	time.Sleep(50 * time.Millisecond)
	p2, ok := pm1.Get(2)
	if !ok {
		t.Fatal("node 1 did not register node 2 in peer map")
	}
	if p2.Role != cluster.RoleFollower || p2.TCPAddr != addr2 {
		t.Errorf("node 1 recorded incorrect info for node 2: %+v", p2)
	}

	// Verify node 2 received node 1's ack and added node 1
	p1, ok := pm2.Get(1)
	if !ok {
		t.Fatal("node 2 did not register node 1 from ack")
	}
	if p1.Role != cluster.RoleLeader || p1.TCPAddr != addr1 {
		t.Errorf("node 2 recorded incorrect info for node 1: %+v", p1)
	}

	// Verify followers list on leader
	followers := pm1.Followers()
	if len(followers) != 1 || followers[0].ID != 2 {
		t.Errorf("pm1 Followers mismatch: %+v", followers)
	}
}

func TestParsePeersList(t *testing.T) {
	// When empty, should default to 7001, 7002, 7003 excluding self
	defaults := parsePeersList("", ":7001")
	if len(defaults) != 2 {
		t.Fatalf("expected 2 default peers, got %v", defaults)
	}
	for _, p := range defaults {
		if p == ":7001" {
			t.Errorf("default list should not include self :7001")
		}
	}

	// Custom list
	custom := parsePeersList(":7002, :7003, :7004", ":7002")
	if len(custom) != 2 {
		t.Fatalf("expected 2 custom peers, got %v", custom)
	}
	if custom[0] != ":7003" || custom[1] != ":7004" {
		t.Errorf("custom peers mismatch: %v", custom)
	}
}

// ── Storage isolation ─────────────────────────────────────────────────────────

func TestMultipleProcessesStorageIsolation(t *testing.T) {
	base := t.TempDir()
	dir1 := filepath.Join(base, "node1")
	dir2 := filepath.Join(base, "node2")

	lsm1, err := storage.Open(dir1, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm1.Close()
	lsm2, err := storage.Open(dir2, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm2.Close()

	for i := 0; i < 20; i++ {
		_ = lsm1.PUT(fmt.Sprintf("n1_%d", i), []byte(fmt.Sprintf("v1_%d", i)))
		_ = lsm2.PUT(fmt.Sprintf("n2_%d", i), []byte(fmt.Sprintf("v2_%d", i)))
	}

	// Node 1 must not see node 2's keys.
	for i := 0; i < 20; i++ {
		v, err := lsm1.GET(fmt.Sprintf("n1_%d", i))
		if err != nil || v != fmt.Sprintf("v1_%d", i) {
			t.Fatalf("lsm1 GET n1_%d: err=%v val=%q", i, err, v)
		}
		if _, err := lsm1.GET(fmt.Sprintf("n2_%d", i)); err == nil {
			t.Fatalf("lsm1 should not see n2_%d", i)
		}
	}

	_ = lsm1.Close()
	_ = lsm2.Close()

	// Reopen and verify WAL recovery restores data.
	r1, _ := storage.Open(dir1, 100, true)
	defer r1.Close()
	r2, _ := storage.Open(dir2, 100, true)
	defer r2.Close()

	for i := 0; i < 20; i++ {
		if _, err := r1.GET(fmt.Sprintf("n1_%d", i)); err != nil {
			t.Fatalf("r1 recovery GET n1_%d: %v", i, err)
		}
		if _, err := r2.GET(fmt.Sprintf("n2_%d", i)); err != nil {
			t.Fatalf("r2 recovery GET n2_%d: %v", i, err)
		}
	}
}

// ── HTTP server: /status and /peers endpoints ─────────────────────────────────

func TestHTTPStatusAndPeersEndpoints(t *testing.T) {
	base := t.TempDir()

	ns := &cluster.NodeState{
		ID:       1,
		HTTPAddr: ":28001",
		TCPAddr:  ":27001",
		Role:     cluster.RoleLeader,
	}

	lsm, err := storage.Open(filepath.Join(base, "lsm"), 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	pm := cluster.NewPeerMap()
	pm.Add(cluster.PeerInfo{ID: 2, TCPAddr: ":27002", Role: cluster.RoleFollower})

	srv := &Server{lsm: lsm}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)
	mux.HandleFunc("DELETE /del", srv.delete)
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		s := ns.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":    s.ID,
			"role":  s.Role,
			"http":  s.HTTPAddr,
			"tcp":   s.TCPAddr,
			"peers": pm.Snapshot(),
		})
	})
	mux.HandleFunc("GET /peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pm.Snapshot())
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(ln) }()
	defer httpSrv.Close()

	base2 := "http://" + ln.Addr().String()

	// Give the server a moment to be ready
	time.Sleep(20 * time.Millisecond)

	// /status
	resp, err := http.Get(base2 + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/status: want 200, got %d", resp.StatusCode)
	}
	var status map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&status)
	if fmt.Sprintf("%v", status["id"]) != "1" {
		t.Errorf("/status id: got %v", status["id"])
	}
	if status["role"] != string(cluster.RoleLeader) {
		t.Errorf("/status role: got %v", status["role"])
	}

	// /peers
	resp2, err := http.Get(base2 + "/peers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var peers []cluster.PeerInfo
	_ = json.NewDecoder(resp2.Body).Decode(&peers)
	if len(peers) != 1 || peers[0].ID != 2 {
		t.Errorf("/peers: got %+v", peers)
	}

	// KV round-trip through HTTP
	// (uses the existing lsm_test HTTP helpers indirectly)
}

func TestHTTPServerAPI(t *testing.T) {
	lsm, err := storage.Open(t.TempDir(), 1024, true)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer lsm.Close()

	srv := &Server{lsm: lsm}

	putReq := httptest.NewRequest("POST", "/put", strings.NewReader(`{"key":"hello","val":"world"}`))
	putRec := httptest.NewRecorder()
	srv.add(putRec, putReq)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", putRec.Code)
	}

	getReq := httptest.NewRequest("GET", "/get", strings.NewReader(`{"key":"hello"}`))
	getRec := httptest.NewRecorder()
	srv.get(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}
	if !bytes.Contains(getRec.Body.Bytes(), []byte("world")) {
		t.Fatalf("expected body containing world, got %s", getRec.Body.String())
	}

	getReq2 := httptest.NewRequest("GET", "/get?key=hello", nil)
	getRec2 := httptest.NewRecorder()
	srv.get(getRec2, getReq2)
	if getRec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec2.Code)
	}

	getReq3 := httptest.NewRequest("GET", "/get?key=unknown", nil)
	getRec3 := httptest.NewRecorder()
	srv.get(getRec3, getReq3)
	if getRec3.Code != http.StatusNotFound {
		t.Fatalf("expected 404 NotFound, got %d", getRec3.Code)
	}

	delReq := httptest.NewRequest("DELETE", "/del?key=hello", nil)
	delRec := httptest.NewRecorder()
	srv.delete(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", delRec.Code)
	}

	getReq4 := httptest.NewRequest("GET", "/get?key=hello", nil)
	getRec4 := httptest.NewRecorder()
	srv.get(getRec4, getReq4)
	if getRec4.Code != http.StatusNotFound {
		t.Fatalf("expected 404 NotFound after delete, got %d", getRec4.Code)
	}
}
