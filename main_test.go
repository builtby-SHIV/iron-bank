package main

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractPort(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		hasErr   bool
	}{
		{input: ":8001", expected: "8001", hasErr: false},
		{input: "127.0.0.1:8002", expected: "8002", hasErr: false},
		{input: "localhost:8003", expected: "8003", hasErr: false},
		{input: "0.0.0.0:8004", expected: "8004", hasErr: false},
		{input: "[::1]:8005", expected: "8005", hasErr: false},
		{input: "8006", expected: "8006", hasErr: false},
		{input: "  :8007  ", expected: "8007", hasErr: false},
		{input: "", expected: "", hasErr: true},
		{input: "invalid", expected: "", hasErr: true},
		{input: ":99999", expected: "", hasErr: true},
		{input: ":0", expected: "", hasErr: true},
		{input: ":-1", expected: "", hasErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := extractPort(tt.input)
			if tt.hasErr {
				if err == nil {
					t.Fatalf("extractPort(%q) expected error, got %q", tt.input, got)
				}
			} else {
				if err != nil {
					t.Fatalf("extractPort(%q) unexpected error: %v", tt.input, err)
				}
				if got != tt.expected {
					t.Fatalf("extractPort(%q) = %q, expected %q", tt.input, got, tt.expected)
				}
			}
		})
	}
}

func TestResolveConfig(t *testing.T) {
	t.Run("Valid config with id, http, and tcp", func(t *testing.T) {
		cfg, err := resolveConfig("8001", ":8001", ":9001", "", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.NodeID != "8001" {
			t.Errorf("expected NodeID 8001, got %s", cfg.NodeID)
		}
		if cfg.HTTPAddr != ":8001" {
			t.Errorf("expected HTTPAddr :8001, got %s", cfg.HTTPAddr)
		}
		if cfg.TCPAddr != ":9001" {
			t.Errorf("expected TCPAddr :9001, got %s", cfg.TCPAddr)
		}
		expectedDir := filepath.Join("data", "8001")
		if cfg.DataDir != expectedDir {
			t.Errorf("expected DataDir %s, got %s", expectedDir, cfg.DataDir)
		}
	})

	t.Run("HTTP flag is optional (omitted)", func(t *testing.T) {
		cfg, err := resolveConfig("8001", "", ":9001", "", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.NodeID != "8001" {
			t.Errorf("expected NodeID 8001, got %s", cfg.NodeID)
		}
		if cfg.HTTPAddr != "" {
			t.Errorf("expected empty HTTPAddr, got %s", cfg.HTTPAddr)
		}
		if cfg.TCPAddr != ":9001" {
			t.Errorf("expected TCPAddr :9001, got %s", cfg.TCPAddr)
		}
		expectedDir := filepath.Join("data", "8001")
		if cfg.DataDir != expectedDir {
			t.Errorf("expected DataDir %s, got %s", expectedDir, cfg.DataDir)
		}
	})

	t.Run("Missing required tcp flag returns error", func(t *testing.T) {
		_, err := resolveConfig("8001", ":8001", "", "", "", 0)
		if err == nil {
			t.Fatal("expected error when -tcp flag is missing, got nil")
		}
		if !strings.Contains(err.Error(), "-tcp flag is required") {
			t.Fatalf("expected '-tcp flag is required' in error, got %v", err)
		}
	})

	t.Run("ID omitted defaults to http port or tcp port", func(t *testing.T) {
		cfg1, err := resolveConfig("", ":8002", ":9002", "", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg1.NodeID != "8002" {
			t.Errorf("expected NodeID 8002 from http, got %s", cfg1.NodeID)
		}

		cfg2, err := resolveConfig("", "", ":9003", "", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg2.NodeID != "9003" {
			t.Errorf("expected NodeID 9003 from tcp, got %s", cfg2.NodeID)
		}
	})

	t.Run("Explicit custom data directory overrides default", func(t *testing.T) {
		cfg, err := resolveConfig("8001", ":8001", ":9001", "./custom_dir", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DataDir != "./custom_dir" {
			t.Errorf("expected DataDir ./custom_dir, got %s", cfg.DataDir)
		}
	})

	t.Run("Invalid TCP address returns error", func(t *testing.T) {
		_, err := resolveConfig("8001", ":8001", "invalid_tcp", "", "", 0)
		if err == nil {
			t.Fatal("expected error for invalid TCP address, got nil")
		}
	})

	t.Run("Invalid HTTP address returns error", func(t *testing.T) {
		_, err := resolveConfig("8001", "invalid_http", ":9001", "", "", 0)
		if err == nil {
			t.Fatal("expected error for invalid HTTP address, got nil")
		}
	})

	t.Run("Backward compatibility with -addr and -port aliases", func(t *testing.T) {
		cfg, err := resolveConfig("", "", ":9001", "", ":8001", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.HTTPAddr != ":8001" {
			t.Errorf("expected HTTPAddr :8001 from -addr, got %s", cfg.HTTPAddr)
		}

		cfg2, err := resolveConfig("", "", ":9001", "", "", 8005)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg2.HTTPAddr != ":8005" {
			t.Errorf("expected HTTPAddr :8005 from -port, got %s", cfg2.HTTPAddr)
		}
	})
}

func TestMultipleProcessesStorageIsolation(t *testing.T) {
	tempBase := t.TempDir()

	// Node 1 config (simulating process on port 8001)
	port1 := "8001"
	dir1 := filepath.Join(tempBase, port1)

	// Node 2 config (simulating process on port 8002)
	port2 := "8002"
	dir2 := filepath.Join(tempBase, port2)

	// Open both trees simultaneously
	lsm1, err := Open(dir1, 100, true)
	if err != nil {
		t.Fatalf("Failed to open node 1: %v", err)
	}
	defer lsm1.Close()

	lsm2, err := Open(dir2, 100, true)
	if err != nil {
		t.Fatalf("Failed to open node 2: %v", err)
	}
	defer lsm2.Close()

	// Write keys to node 1
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("node1_key_%d", i)
		val := fmt.Sprintf("node1_val_%d", i)
		if err := lsm1.PUT(key, []byte(val)); err != nil {
			t.Fatalf("Node 1 PUT failed: %v", err)
		}
	}

	// Write keys to node 2
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("node2_key_%d", i)
		val := fmt.Sprintf("node2_val_%d", i)
		if err := lsm2.PUT(key, []byte(val)); err != nil {
			t.Fatalf("Node 2 PUT failed: %v", err)
		}
	}

	// Verify node 1 only has its own keys
	for i := 0; i < 20; i++ {
		key1 := fmt.Sprintf("node1_key_%d", i)
		v, err := lsm1.GET(key1)
		if err != nil || v != fmt.Sprintf("node1_val_%d", i) {
			t.Fatalf("Node 1 GET %s expected value, got %s (err: %v)", key1, v, err)
		}

		key2 := fmt.Sprintf("node2_key_%d", i)
		_, err = lsm1.GET(key2)
		if err == nil {
			t.Fatalf("Node 1 should NOT see key %s from node 2", key2)
		}
	}

	// Verify node 2 only has its own keys
	for i := 0; i < 20; i++ {
		key2 := fmt.Sprintf("node2_key_%d", i)
		v, err := lsm2.GET(key2)
		if err != nil || v != fmt.Sprintf("node2_val_%d", i) {
			t.Fatalf("Node 2 GET %s expected value, got %s (err: %v)", key2, v, err)
		}

		key1 := fmt.Sprintf("node1_key_%d", i)
		_, err = lsm2.GET(key1)
		if err == nil {
			t.Fatalf("Node 2 should NOT see key %s from node 1", key1)
		}
	}

	// Close both nodes
	if err := lsm1.Close(); err != nil {
		t.Fatalf("Failed to close node 1: %v", err)
	}
	if err := lsm2.Close(); err != nil {
		t.Fatalf("Failed to close node 2: %v", err)
	}

	// Reopen node 1 and node 2, ensuring independent crash recovery
	reopened1, err := Open(dir1, 100, true)
	if err != nil {
		t.Fatalf("Failed to reopen node 1: %v", err)
	}
	defer reopened1.Close()

	reopened2, err := Open(dir2, 100, true)
	if err != nil {
		t.Fatalf("Failed to reopen node 2: %v", err)
	}
	defer reopened2.Close()

	// Verify recovery for node 1
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("node1_key_%d", i)
		v, err := reopened1.GET(key)
		if err != nil || v != fmt.Sprintf("node1_val_%d", i) {
			t.Fatalf("Reopened Node 1 GET %s failed: %v", key, err)
		}
	}

	// Verify recovery for node 2
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("node2_key_%d", i)
		v, err := reopened2.GET(key)
		if err != nil || v != fmt.Sprintf("node2_val_%d", i) {
			t.Fatalf("Reopened Node 2 GET %s failed: %v", key, err)
		}
	}
}

func TestMultipleServersHTTP(t *testing.T) {
	tempBase := t.TempDir()

	startServer := func(port int) (*http.Server, string, *LSMTree, func()) {
		dataDir := filepath.Join(tempBase, fmt.Sprintf("%d", port))
		lsm, err := Open(dataDir, 1024, true)
		if err != nil {
			t.Fatalf("failed to open lsm for port %d: %v", port, err)
		}

		srv := &Server{lsm: lsm}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /put", srv.add)
		mux.HandleFunc("GET /get", srv.get)
		mux.HandleFunc("DELETE /del", srv.delete)

		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			lsm.Close()
			t.Fatalf("failed to listen on port %d: %v", port, err)
		}

		server := &http.Server{Handler: mux}
		go func() {
			_ = server.Serve(listener)
		}()

		cleanup := func() {
			_ = server.Close()
			_ = lsm.Close()
		}

		return server, listener.Addr().String(), lsm, cleanup
	}

	// Pick two test ports
	p1 := 18001
	p2 := 18002

	_, addr1, _, cleanup1 := startServer(p1)
	defer cleanup1()

	_, addr2, _, cleanup2 := startServer(p2)
	defer cleanup2()

	// PUT to server 1
	body1 := `{"key": "alpha", "val": "first"}`
	resp1, err := http.Post(fmt.Sprintf("http://%s/put", addr1), "application/json", strings.NewReader(body1))
	if err != nil {
		t.Fatalf("POST to server 1 failed: %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from server 1, got %d", resp1.StatusCode)
	}

	// PUT to server 2
	body2 := `{"key": "beta", "val": "second"}`
	resp2, err := http.Post(fmt.Sprintf("http://%s/put", addr2), "application/json", strings.NewReader(body2))
	if err != nil {
		t.Fatalf("POST to server 2 failed: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from server 2, got %d", resp2.StatusCode)
	}

	// GET from server 1 for "alpha" -> should succeed
	getResp1, err := http.Get(fmt.Sprintf("http://%s/get?key=alpha", addr1))
	if err != nil {
		t.Fatalf("GET from server 1 failed: %v", err)
	}
	defer getResp1.Body.Close()
	if getResp1.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from server 1, got %d", getResp1.StatusCode)
	}

	// GET from server 2 for "alpha" -> should be 404 NotFound
	getResp2NotFound, err := http.Get(fmt.Sprintf("http://%s/get?key=alpha", addr2))
	if err != nil {
		t.Fatalf("GET from server 2 failed: %v", err)
	}
	defer getResp2NotFound.Body.Close()
	if getResp2NotFound.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 NotFound from server 2 for alpha, got %d", getResp2NotFound.StatusCode)
	}

	// GET from server 2 for "beta" -> should succeed
	getResp2, err := http.Get(fmt.Sprintf("http://%s/get?key=beta", addr2))
	if err != nil {
		t.Fatalf("GET from server 2 failed: %v", err)
	}
	defer getResp2.Body.Close()
	if getResp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from server 2, got %d", getResp2.StatusCode)
	}
}

func TestTCPAndOptionalHTTPServer(t *testing.T) {
	tempBase := t.TempDir()

	// 1. Process 1: Starts both TCP and HTTP
	cfg1, err := resolveConfig("8001", ":28001", ":29001", filepath.Join(tempBase, "8001"), "", 0)
	if err != nil {
		t.Fatalf("resolveConfig failed: %v", err)
	}

	lsm1, err := Open(cfg1.DataDir, 1024, true)
	if err != nil {
		t.Fatalf("failed to open lsm1: %v", err)
	}
	defer lsm1.Close()

	tcpListener1, err := net.Listen("tcp", cfg1.TCPAddr)
	if err != nil {
		t.Fatalf("failed to listen on TCP 1: %v", err)
	}
	defer tcpListener1.Close()

	go func() {
		for {
			conn, err := tcpListener1.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	srv1 := &Server{lsm: lsm1}
	mux1 := http.NewServeMux()
	mux1.HandleFunc("POST /put", srv1.add)
	mux1.HandleFunc("GET /get", srv1.get)
	httpServer1 := &http.Server{Addr: cfg1.HTTPAddr, Handler: mux1}
	go func() {
		_ = httpServer1.ListenAndServe()
	}()
	defer httpServer1.Close()

	// Verify TCP connection succeeds on process 1
	tcpConn1, err := net.Dial("tcp", "127.0.0.1:29001")
	if err != nil {
		t.Fatalf("failed to dial TCP on process 1: %v", err)
	}
	_ = tcpConn1.Close()

	// Verify HTTP succeeds on process 1
	putResp, err := http.Post("http://127.0.0.1:28001/put", "application/json", strings.NewReader(`{"key":"k1","val":"v1"}`))
	if err != nil {
		t.Fatalf("HTTP post to process 1 failed: %v", err)
	}
	_ = putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from HTTP server 1, got %d", putResp.StatusCode)
	}

	// 2. Process 2: Starts with ONLY TCP (HTTP is optional and omitted)
	cfg2, err := resolveConfig("8002", "", ":29002", filepath.Join(tempBase, "8002"), "", 0)
	if err != nil {
		t.Fatalf("resolveConfig failed: %v", err)
	}
	if cfg2.HTTPAddr != "" {
		t.Fatalf("expected empty HTTPAddr for process 2, got %s", cfg2.HTTPAddr)
	}

	lsm2, err := Open(cfg2.DataDir, 1024, true)
	if err != nil {
		t.Fatalf("failed to open lsm2: %v", err)
	}
	defer lsm2.Close()

	tcpListener2, err := net.Listen("tcp", cfg2.TCPAddr)
	if err != nil {
		t.Fatalf("failed to listen on TCP 2: %v", err)
	}
	defer tcpListener2.Close()

	go func() {
		for {
			conn, err := tcpListener2.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	// Verify TCP connection succeeds on process 2
	tcpConn2, err := net.Dial("tcp", "127.0.0.1:29002")
	if err != nil {
		t.Fatalf("failed to dial TCP on process 2: %v", err)
	}
	_ = tcpConn2.Close()

	// Verify process 1 and 2 storage directories are isolated
	if cfg1.DataDir == cfg2.DataDir {
		t.Fatalf("expected separate data directories, got identical %s", cfg1.DataDir)
	}
}

