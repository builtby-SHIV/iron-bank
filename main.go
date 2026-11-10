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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"iron-bank/proto"
	"iron-bank/raft"
	"iron-bank/storage"
)

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
	RoleObserver = "observer"
	RoleUnknown  = "unknown"
)

type Response struct {
	Message string
}

type AddRequest struct {
	Key string
	Val string
}

type NonAddRequest struct {
	Key string
}

type Server struct {
	lsm *storage.LSMTree
	ns  *raft.NodeState
}

type ServerConfig struct {
	NodeID   uint64
	HTTPAddr string
	TCPAddr  string
	DataDir  string
	Role     string
	Peers    string
}

func parseRole(s string) (string, error) {
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

func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.ns.Log.WriteEntry(proto.Command_PUT, req.Key, []byte(req.Val), s.ns.RaftState.Term); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.ns.ClientCh<- &raft.ClientCmd{ Command: proto.Command_PUT, Key: req.Key, Value: req.Val }
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(Response{Message: "Key-Val pair created"})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	var nf *storage.NotFoundError
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

func parsePort(addr string) (int, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return 0, nil
	}
	portStr, err := extractPort(addr)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func resolveConfig(
	idFlag,
	httpFlag,
	tcpFlag,
	roleFlag,
	dataDirFlag,
	peersFlag string) (*ServerConfig, error) {
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
	role, err := parseRole(roleFlag)
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
	} else {
		candidates = []string{":7001", ":7002", ":7003", ":7004", ":7005"}
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
	idFlag := flag.String("id", "", "Node identifier")
	httpFlag := flag.String("http", "", "HTTP listen address")
	tcpFlag := flag.String("tcp", "", "TCP listen address")
	roleFlag := flag.String("role", "", "Node role: leader | follower | observer")
	dataDirFlag := flag.String("data", "", "Storage directory")
	peersFlag := flag.String("peers", "", "List of peer TCP addresses")
	flag.Parse()

	cfg, err := resolveConfig(*idFlag, *httpFlag, *tcpFlag, *roleFlag, *dataDirFlag, *peersFlag)
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("[startup] create data dir %s: %v", cfg.DataDir, err)
	}

	procCfg, err := storage.LoadOrInitProcessConfig(cfg.DataDir, *httpFlag, *tcpFlag)
	if err != nil {
		log.Fatalf("[startup] load or init process config: %v", err)
	}

	log.Printf("[node] id=%d  role=%s  http=%s  tcp=%s  term=%d  votedFor=%d  data=%s",
		cfg.NodeID, cfg.Role, cfg.HTTPAddr, cfg.TCPAddr, procCfg.CurrentTerm, procCfg.VotedFor, cfg.DataDir)

	walDir := filepath.Join(cfg.DataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("[startup] create wal dir %s: %v", walDir, err)
	}
	wal, err := storage.OpenWAL(walDir, true, 128000, 1000)
	if err != nil {
		log.Fatalf("[startup] open WAL at %s: %v", walDir, err)
	}
	defer wal.Close()

	sstableDir := filepath.Join(cfg.DataDir, "sstable")
	if err := os.MkdirAll(sstableDir, 0755); err != nil {
		log.Fatalf("[startup] create sstable dir %s: %v", sstableDir, err)
	}
	lsm, err := storage.Open(sstableDir, 4096)
	if err != nil {
		log.Fatalf("[startup] open LSM at %s: %v", sstableDir, err)
	}
	defer lsm.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_ = parsePeersList(*peersFlag, cfg.TCPAddr)

	var httpServer *http.Server

	if cfg.HTTPAddr != "" {
		srv := &Server{
			lsm: lsm,
		}
		mux := http.NewServeMux()

		mux.HandleFunc("POST /put", srv.add)
		mux.HandleFunc("GET /get", srv.get)
		mux.HandleFunc("DELETE /del", srv.delete)

		httpServer = &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[http] %v", err)
			}
		}()
		log.Printf("[http] listening on %s", cfg.HTTPAddr)
	}

	<-ctx.Done()

	log.Println("[node] shutting down…")
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
}
