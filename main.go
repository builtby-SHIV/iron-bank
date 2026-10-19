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
	lsm *LSMTree
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

type ServerConfig struct {
	NodeID   string
	HTTPAddr string 
	TCPAddr  string 
	DataDir  string
	Role     string
}

func resolveConfig(idFlag, httpFlag, tcpFlag, roleFlag, dataDirFlag string) (*ServerConfig, error) {
	if strings.TrimSpace(tcpFlag) == "" {
		return nil, errors.New("--tcp is required (e.g. --tcp=:7001)")
	}
	normTCP, tcpPort, err := normalizeAddr(tcpFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --tcp=%q: %w", tcpFlag, err)
	}

	var normHTTP, httpPort string
	if h := strings.TrimSpace(httpFlag); h != "" {
		normHTTP, httpPort, err = normalizeAddr(h)
		if err != nil {
			return nil, fmt.Errorf("invalid --http=%q: %w", h, err)
		}
	}

	nodeID := strings.TrimSpace(idFlag)
	if nodeID == "" {
		if httpPort != "" {
			nodeID = httpPort
		} else {
			nodeID = tcpPort
		}
	}

	dataDir := strings.TrimSpace(dataDirFlag)
	if dataDir == "" {
		dataDir = filepath.Join("data", nodeID)
	}

	role := RoleUnknown
	if r := strings.TrimSpace(roleFlag); r != "" {
		role, err = ParseRole(r)
		if err != nil {
			return nil, fmt.Errorf("invalid --role=%q: %w", r, err)
		}
	}

	return &ServerConfig{
		NodeID:   nodeID,
		HTTPAddr: normHTTP,
		TCPAddr:  normTCP,
		DataDir:  dataDir,
		Role:     role,
	}, nil
}


func main() {
	idFlag      := flag.String("id",   "",  "Node identifier")
	httpFlag    := flag.String("http", "",  "HTTP listen address (optional)")
	tcpFlag     := flag.String("tcp",  "",  "TCP listen address (required)")
	roleFlag    := flag.String("role", "",  "Node role: leader | follower | observer")
	dataDirFlag := flag.String("data", "",  "Storage directory")
	flag.Parse()

	cfg, err := resolveConfig(*idFlag, *httpFlag, *tcpFlag, *roleFlag, *dataDirFlag)
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("[startup] create data dir %s: %v", cfg.DataDir, err)
	}

	ns, err := LoadOrInitNodeState(cfg.DataDir)
	if err != nil {
		log.Fatalf("[startup] load node state: %v", err)
	}
	ns.MergeFlags(cfg.NodeID, cfg.HTTPAddr, cfg.TCPAddr, cfg.Role)
	if err := ns.Save(); err != nil {
		log.Fatalf("[startup] save node state: %v", err)
	}

	snap := ns.Snapshot()
	log.Printf("[node] id=%s  role=%s  http=%s  tcp=%s  data=%s",
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

	peerMap := NewPeerMap()

	var httpServer *http.Server
	if snap.HTTPAddr != "" {
		srv := &Server{lsm: lsm}
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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("[node] shutting down…")
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
}