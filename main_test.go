package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		cfg, err := resolveConfig("1", ":8001", ":7001", "leader", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.NodeID != "1" {
			t.Errorf("NodeID: got %q, want 1", cfg.NodeID)
		}
		if cfg.HTTPAddr != ":8001" {
			t.Errorf("HTTPAddr: got %q, want :8001", cfg.HTTPAddr)
		}
		if cfg.TCPAddr != ":7001" {
			t.Errorf("TCPAddr: got %q, want :7001", cfg.TCPAddr)
		}
		if cfg.Role != RoleLeader {
			t.Errorf("Role: got %q, want leader", cfg.Role)
		}
		if cfg.DataDir != filepath.Join("data", "1") {
			t.Errorf("DataDir: got %q, want %q", cfg.DataDir, filepath.Join("data", "1"))
		}
	})

	t.Run("http optional — omitted", func(t *testing.T) {
		cfg, err := resolveConfig("2", "", ":7002", "follower", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.HTTPAddr != "" {
			t.Errorf("HTTPAddr should be empty, got %q", cfg.HTTPAddr)
		}
		if cfg.Role != RoleFollower {
			t.Errorf("Role: got %q, want follower", cfg.Role)
		}
	})

	t.Run("id defaults to tcp port when http omitted", func(t *testing.T) {
		cfg, err := resolveConfig("", "", ":7003", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.NodeID != "7003" {
			t.Errorf("NodeID: got %q, want 7003", cfg.NodeID)
		}
	})

	t.Run("id defaults to http port when http provided", func(t *testing.T) {
		cfg, err := resolveConfig("", ":8004", ":7004", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.NodeID != "8004" {
			t.Errorf("NodeID: got %q, want 8004", cfg.NodeID)
		}
	})

	t.Run("explicit data dir overrides default", func(t *testing.T) {
		cfg, err := resolveConfig("1", "", ":7001", "", "./custom")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.DataDir != "./custom" {
			t.Errorf("DataDir: got %q, want ./custom", cfg.DataDir)
		}
	})

	t.Run("tcp missing returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", "", "leader", "")
		if err == nil {
			t.Fatal("expected error when --tcp omitted")
		}
		if !strings.Contains(err.Error(), "--tcp") {
			t.Errorf("error should mention --tcp, got: %v", err)
		}
	})

	t.Run("invalid role returns error", func(t *testing.T) {
		_, err := resolveConfig("1", ":8001", ":7001", "emperor", "")
		if err == nil {
			t.Fatal("expected error for unknown role")
		}
	})

	t.Run("invalid tcp returns error", func(t *testing.T) {
		_, err := resolveConfig("1", "", "not-a-port", "", "")
		if err == nil {
			t.Fatal("expected error for invalid tcp")
		}
	})

	t.Run("invalid http returns error", func(t *testing.T) {
		_, err := resolveConfig("1", "not-a-port", ":7001", "", "")
		if err == nil {
			t.Fatal("expected error for invalid http")
		}
	})
}

// ── NodeState ─────────────────────────────────────────────────────────────────

func TestNodeStatePersistence(t *testing.T) {
	dir := t.TempDir()

	// First run: no file yet.
	ns, err := LoadOrInitNodeState(dir)
	if err != nil {
		t.Fatal(err)
	}
	ns.MergeFlags("1", ":8001", ":7001", RoleLeader)
	if err := ns.Save(); err != nil {
		t.Fatal(err)
	}

	// Second run: file exists, values should be restored.
	ns2, err := LoadOrInitNodeState(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap := ns2.Snapshot()
	if snap.ID != "1" || snap.HTTPAddr != ":8001" || snap.TCPAddr != ":7001" || snap.Role != RoleLeader {
		t.Errorf("persisted state mismatch: %+v", snap)
	}
}

func TestNodeStateMergeFlags_CLIWins(t *testing.T) {
	dir := t.TempDir()

	ns, _ := LoadOrInitNodeState(dir)
	ns.MergeFlags("1", ":8001", ":7001", RoleFollower)
	_ = ns.Save()

	// Reload and override only role via CLI.
	ns2, _ := LoadOrInitNodeState(dir)
	ns2.MergeFlags("", "", "", RoleLeader)
	snap := ns2.Snapshot()

	if snap.Role != RoleLeader {
		t.Errorf("CLI role should win: got %q", snap.Role)
	}
	// Other fields should survive from disk.
	if snap.ID != "1" || snap.HTTPAddr != ":8001" {
		t.Errorf("disk fields should be preserved: %+v", snap)
	}
}

func TestNodeStateUpdateRole(t *testing.T) {
	dir := t.TempDir()
	ns, _ := LoadOrInitNodeState(dir)
	ns.MergeFlags("1", "", ":7001", RoleFollower)
	_ = ns.Save()

	if err := ns.UpdateRole(RoleLeader); err != nil {
		t.Fatal(err)
	}

	// Reload and confirm role was persisted.
	ns2, _ := LoadOrInitNodeState(dir)
	if ns2.Snapshot().Role != RoleLeader {
		t.Error("role not persisted after UpdateRole")
	}
}

// ── PeerMap ───────────────────────────────────────────────────────────────────

func TestPeerMapOperations(t *testing.T) {
	pm := NewPeerMap()

	if pm.Len() != 0 {
		t.Fatal("expected empty map")
	}

	p1 := PeerInfo{ID: "1", HTTPAddr: ":8001", TCPAddr: ":7001", Role: RoleLeader}
	p2 := PeerInfo{ID: "2", HTTPAddr: ":8002", TCPAddr: ":7002", Role: RoleFollower}

	pm.Add(p1)
	pm.Add(p2)

	if pm.Len() != 2 {
		t.Fatalf("expected 2 peers, got %d", pm.Len())
	}

	got, ok := pm.Get("1")
	if !ok || got.Role != RoleLeader {
		t.Errorf("Get(1): %+v ok=%v", got, ok)
	}

	// Re-add should overwrite.
	pm.Add(PeerInfo{ID: "1", HTTPAddr: ":8001", TCPAddr: ":7001", Role: RoleFollower})
	got, _ = pm.Get("1")
	if got.Role != RoleFollower {
		t.Errorf("Add should overwrite: want follower, got %q", got.Role)
	}

	pm.Remove("2")
	if pm.Len() != 1 {
		t.Fatalf("expected 1 peer after Remove, got %d", pm.Len())
	}
	if _, ok := pm.Get("2"); ok {
		t.Error("peer 2 should have been removed")
	}
}

// ── Storage isolation ─────────────────────────────────────────────────────────

func TestMultipleProcessesStorageIsolation(t *testing.T) {
	base := t.TempDir()
	dir1 := filepath.Join(base, "node1")
	dir2 := filepath.Join(base, "node2")

	lsm1, err := Open(dir1, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm1.Close()
	lsm2, err := Open(dir2, 100, true)
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
	r1, _ := Open(dir1, 100, true)
	defer r1.Close()
	r2, _ := Open(dir2, 100, true)
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

	ns, _ := LoadOrInitNodeState(base)
	ns.MergeFlags("1", ":28001", ":27001", RoleLeader)
	_ = ns.Save()

	lsm, err := Open(filepath.Join(base, "lsm"), 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	pm := NewPeerMap()
	pm.Add(PeerInfo{ID: "2", TCPAddr: ":27002", Role: RoleFollower})

	srv := &Server{lsm: lsm}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put",   srv.add)
	mux.HandleFunc("GET /get",    srv.get)
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
	if status["id"] != "1" {
		t.Errorf("/status id: got %v", status["id"])
	}
	if status["role"] != string(RoleLeader) {
		t.Errorf("/status role: got %v", status["role"])
	}

	// /peers
	resp2, err := http.Get(base2 + "/peers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var peers []PeerInfo
	_ = json.NewDecoder(resp2.Body).Decode(&peers)
	if len(peers) != 1 || peers[0].ID != "2" {
		t.Errorf("/peers: got %+v", peers)
	}

	// KV round-trip through HTTP
	// (uses the existing lsm_test HTTP helpers indirectly)
}
