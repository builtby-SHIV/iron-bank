package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Response struct {
	Message string `json:"message"`
}

type AddRequest struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type NonAddRequest struct {
	Key string `json:"key"`
}

type Server struct {
	lsm          *LSMTree
	term 		 uint64
	votedFor     int64
	prevLogIndex uint64
}

type ServerConfig struct {
	NodeID   uint64
	HTTPAddr string 
	TCPAddr  string 
	DataDir  string
	Role     string
	Peers    string
}

func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.lsm.PUT(req.Key, []byte(req.Val)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(Response{Message: "Key-Val pair created"})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	var nf *NotFoundError
	if key := r.URL.Query().Get("key"); key != "" {
		req.Key = key
	} else if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	v, err := s.lsm.GET(req.Key)
	if err != nil {
		if errors.As(err, &nf) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{req.Key: v})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	if key := r.URL.Query().Get("key"); key != "" {
		req.Key = key
	} else if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if err := s.lsm.DELETE(req.Key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"msg": "Key successfully deleted"})
}

func normalizeAddr(addr string) (string, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", errors.New("empty address")
	}
	if p, err := strconv.Atoi(addr); err == nil && p > 0 && p <= 65535 {
		return ":" + addr, addr, nil
	}
	if strings.Contains(addr, ":") {
		host, port, err := net.SplitHostPort(addr)
		if err == nil && port != "" {
			if p, err := strconv.Atoi(port); err == nil && p > 0 && p <= 65535 {
				if host == "" {
					return ":" + port, port, nil
				}
				return addr, port, nil
			}
		}
		last := strings.LastIndex(addr, ":")
		if last != -1 && last < len(addr)-1 {
			candidate := addr[last+1:]
			if p, err := strconv.Atoi(candidate); err == nil && p > 0 && p <= 65535 {
				return addr, candidate, nil
			}
		}
	}
	return "", "", fmt.Errorf("invalid address or port %q", addr)
}

func extractPort(addr string) (string, error) {
	_, port, err := normalizeAddr(addr)
	return port, err
}

func resolveConfig(idFlag, httpFlag, tcpFlag, roleFlag, dataDirFlag, peersFlag string) (*ServerConfig, error) {
	if strings.TrimSpace(idFlag) == "" {
		return nil, errors.New("--id is required")
	}
	nodeID, err := strconv.ParseUint(strings.TrimSpace(idFlag), 10, 64)
	if err != nil || nodeID == 0 {
		return nil, fmt.Errorf("invalid --id=%q: must be a positive integer", idFlag)
	}

	if strings.TrimSpace(httpFlag) == "" {
		return nil, errors.New("--http is required")
	}
	normHTTP, _, err := normalizeAddr(httpFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --http=%q: %w", httpFlag, err)
	}

	if strings.TrimSpace(tcpFlag) == "" {
		return nil, errors.New("--tcp is required")
	}
	normTCP, _, err := normalizeAddr(tcpFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --tcp=%q: %w", tcpFlag, err)
	}

	if strings.TrimSpace(roleFlag) == "" {
		return nil, errors.New("--role is required")
	}
	role, err := ParseRole(roleFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --role=%q: %w", roleFlag, err)
	}
	if role == RoleUnknown {
		return nil, fmt.Errorf("invalid --role=%q: must be one of leader, follower, observer", roleFlag)
	}

	dataDir := strings.TrimSpace(dataDirFlag)
	if dataDir == "" {
		return nil, errors.New("--data is required")
	}

	peers := strings.TrimSpace(peersFlag)
	if peers == "" {
		return nil, errors.New("--peers is required")
	}

	return &ServerConfig{
		NodeID:   nodeID,
		HTTPAddr: normHTTP,
		TCPAddr:  normTCP,
		DataDir:  dataDir,
		Role:     role,
		Peers:    peers,
	}, nil
}


// parsePeersList returns a list of target peer TCP addresses, excluding selfTCP.
func parsePeersList(raw string, selfTCP string) []string {
	var candidates []string
	if strings.TrimSpace(raw) != "" {
		for _, p := range strings.Split(raw, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				norm, _, err := normalizeAddr(p)
				if err == nil {
					candidates = append(candidates, norm)
				} else {
					candidates = append(candidates, p)
				}
			}
		}
	}

	normSelf, _, _ := normalizeAddr(selfTCP)
	var targets []string
	for _, c := range candidates {
		if c != selfTCP && c != normSelf {
			targets = append(targets, c)
		}
	}
	return targets
}

func main() {
	idFlag      := flag.String("id",    "", "Node identifier (required, uint64)")
	httpFlag    := flag.String("http",  "", "HTTP listen address (required)")
	tcpFlag     := flag.String("tcp",   "", "TCP listen address (required)")
	roleFlag    := flag.String("role",  "", "Node role: leader | follower | observer (required)")
	dataDirFlag := flag.String("data",  "", "Storage directory (required)")
	peersFlag   := flag.String("peers", "", "List of peer TCP addresses (required)")
	flag.Parse()

	cfg, err := resolveConfig(*idFlag, *httpFlag, *tcpFlag, *roleFlag, *dataDirFlag, *peersFlag)
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("[startup] create data dir %s: %v", cfg.DataDir, err)
	}

	ns := &NodeState{
		ID:       cfg.NodeID,
		HTTPAddr: cfg.HTTPAddr,
		TCPAddr:  cfg.TCPAddr,
		Role:     cfg.Role,
	}

	snap := ns.Snapshot()
	log.Printf("[node] id=%d  role=%s  http=%s  tcp=%s  data=%s",
		snap.ID, snap.Role, snap.HTTPAddr, snap.TCPAddr, cfg.DataDir)

	lsm, err := Open(cfg.DataDir, 4096, true)
	if err != nil {
		log.Fatalf("[startup] open LSM at %s: %v", cfg.DataDir, err)
	}
	defer lsm.Close()

	tcpLn, err := net.Listen("tcp", snap.TCPAddr)
	if err != nil {
		log.Fatalf("[startup] listen TCP %s: %v", snap.TCPAddr, err)
	}
	defer tcpLn.Close()
	log.Printf("[tcp] listening on %s", snap.TCPAddr)

	entry, err := lsm.wal.getLastEntryInLog()
	if err != nil {
		//handle this later on
	}
	if entry != nil {
		ns.ModifyNodeState(entry.Term, entry.VotedFor)
		snap = ns.Snapshot()
	}

	peerMap := NewPeerMap()
	selfInfo := PeerInfo{
		ID:       snap.ID,
		HTTPAddr: snap.HTTPAddr,
		TCPAddr:  snap.TCPAddr,
		Role:     snap.Role,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── TCP Accept Loop: receive incoming peer connections & announcements ──
	go func() {
		for {
			conn, err := tcpLn.Accept()
			if err != nil {
				return // listener closed during shutdown
			}
			go peerMap.HandleTCPConnection(conn, selfInfo)
		}
	}()

	targets := parsePeersList(*peersFlag, snap.TCPAddr)
	peerMap.BroadcastAnnouncement(targets, ns, ctx)

	var httpServer *http.Server

	if snap.HTTPAddr != "" {
		srv := &Server{ 
			lsm:          lsm, 
			term:         snap.Term,
			votedFor:     snap.VotedFor,
		}
		if entry != nil {
			srv.prevLogIndex = entry.Index
		}
		mux := http.NewServeMux()

		mux.HandleFunc("POST /put",   srv.add)
		mux.HandleFunc("GET /get",    srv.get)
		mux.HandleFunc("DELETE /del", srv.delete)

		// Node introspection
		mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
			s := ns.Snapshot()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    s.ID,
				"role":  s.Role,
				"http":  s.HTTPAddr,
				"tcp":   s.TCPAddr,
				"peers": peerMap.Snapshot(),
			})
		})

		mux.HandleFunc("GET /peers", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(peerMap.Snapshot())
		})

		httpServer = &http.Server{Addr: snap.HTTPAddr, Handler: mux}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[http] %v", err)
			}
		}()
		log.Printf("[http] listening on %s", snap.HTTPAddr)
	}

	<- ctx.Done()

	log.Println("[node] shutting down…")
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
}