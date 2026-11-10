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

	"iron-bank/storage"
)

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
	RoleObserver = "observer"
	RoleUnknown  = "unknown"
)

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
}

type ServerConfig struct {
	NodeID   uint64
	HTTPAddr string
	TCPAddr  string
	DataDir  string
	Role     string
	Peers    string
}

type ProcessConfig struct {
	Role        string 
	HTTPPort    int    
	TCPPort     int    
	CurrentTerm uint64 
	VotedFor    int64  
}

func (p *ProcessConfig) UnmarshalJSON(data []byte) error {
	type Alias struct {
		Role        any 
		HTTPPort    any 
		TCPPort     any 
		CurrentTerm any 
		VotedFor    any 
	}
	var aux Alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	toInt := func(val any) (int, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return int(v), nil
		case string:
			s := strings.TrimSpace(v)
			if s == "" {
				return 0, nil
			}
			return parsePort(s)
		default:
			return 0, fmt.Errorf("unexpected type for port: %T", val)
		}
	}

	toUint64 := func(val any) (uint64, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return uint64(v), nil
		case string:
			return strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		default:
			return 0, fmt.Errorf("unexpected type for term: %T", val)
		}
	}

	toInt64 := func(val any) (int64, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		default:
			return 0, fmt.Errorf("unexpected type for voted_for: %T", val)
		}
	}

	httpP, err := toInt(aux.HTTPPort)
	if err != nil {
		return err
	}
	tcpP, err := toInt(aux.TCPPort)
	if err != nil {
		return err
	}
	term, err := toUint64(aux.CurrentTerm)
	if err != nil {
		return err
	}
	vf, err := toInt64(aux.VotedFor)
	if err != nil {
		return err
	}

	role := ""
	if aux.Role != nil {
		if s, ok := aux.Role.(string); ok {
			role = strings.TrimSpace(s)
		} else {
			role = fmt.Sprintf("%v", aux.Role)
		}
	}

	p.Role = role
	p.HTTPPort = httpP
	p.TCPPort = tcpP
	p.CurrentTerm = term
	p.VotedFor = vf
	return nil
}

func GetConfigPath(baseDir string) string {
	if strings.HasSuffix(baseDir, ".json") {
		return baseDir
	}
	target := filepath.Join(baseDir, "config", "config.json")
	if _, err := os.Stat(target); err == nil {
		return target
	}
	fallback := filepath.Join(baseDir, "config.json")
	if _, err := os.Stat(fallback); err == nil {
		return fallback
	}
	return target
}

func GetProcessConfig(baseDir string) (*ProcessConfig, error) {
	configPath := GetConfigPath(baseDir)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg ProcessConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file %s: %w", configPath, err)
	}
	return &cfg, nil
}

func OverwriteProcessConfig(baseDir string, cfg *ProcessConfig) error {
	if cfg == nil {
		return errors.New("cannot overwrite config with nil")
	}
	configPath := GetConfigPath(baseDir)
	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("creating config dir %s: %w", configDir, err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing config file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, configPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("committing config file %s: %w", configPath, err)
	}
	return nil
}

func LoadOrInitProcessConfig(baseDir, httpFlag, tcpFlag string, roleFlag ...string) (*ProcessConfig, error) {
	configPath := GetConfigPath(baseDir)
	if _, err := os.Stat(configPath); err == nil {
		return GetProcessConfig(baseDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	httpPort, err := parsePort(httpFlag)
	if err != nil {
		return nil, fmt.Errorf("parsing http port from flag %q: %w", httpFlag, err)
	}

	tcpPort, err := parsePort(tcpFlag)
	if err != nil {
		return nil, fmt.Errorf("parsing tcp port from flag %q: %w", tcpFlag, err)
	}

	role := ""
	if len(roleFlag) > 0 {
		role = strings.TrimSpace(roleFlag[0])
	}

	cfg := &ProcessConfig{
		Role:        role,
		HTTPPort:    httpPort,
		TCPPort:     tcpPort,
		CurrentTerm: 0,
		VotedFor:    -1,
	}

	if err := OverwriteProcessConfig(baseDir, cfg); err != nil {
		return nil, fmt.Errorf("creating config file: %w", err)
	}
	return cfg, nil
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

	procCfg, err := LoadOrInitProcessConfig(cfg.DataDir, *httpFlag, *tcpFlag, cfg.Role)
	if err != nil {
		log.Fatalf("[startup] load or init process config: %v", err)
	}

	log.Printf("[node] id=%d  role=%s  http=%s  tcp=%s  term=%d  votedFor=%d  data=%s",
		cfg.NodeID, procCfg.Role, cfg.HTTPAddr, cfg.TCPAddr, procCfg.CurrentTerm, procCfg.VotedFor, cfg.DataDir)

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