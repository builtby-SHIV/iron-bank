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
	_ = json.NewEncoder(w).Encode(Response{
		Message: "Key-Val pair created",
	})
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

	// Case 1: Pure number like "8001"
	if p, err := strconv.Atoi(addr); err == nil && p > 0 && p <= 65535 {
		return ":" + addr, addr, nil
	}

	// Case 2: Contains colon (e.g. ":8001", "127.0.0.1:8001", "[::1]:8001")
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

		// Fallback for cases like ":8001" if SplitHostPort had issues
		lastColon := strings.LastIndex(addr, ":")
		if lastColon != -1 && lastColon < len(addr)-1 {
			candidate := addr[lastColon+1:]
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
}

func resolveConfig(idFlag, httpFlag, tcpFlag, dataDirFlag, addrFlag string, portFlag int) (*ServerConfig, error) {
	// 1. Check TCP flag (required)
	tcpStr := strings.TrimSpace(tcpFlag)
	if tcpStr == "" {
		return nil, errors.New("-tcp flag is required (e.g. -tcp :9001)")
	}
	normTCP, tcpPort, err := normalizeAddr(tcpStr)
	if err != nil {
		return nil, fmt.Errorf("invalid -tcp address %q: %w", tcpStr, err)
	}

	// 2. Check HTTP flag (optional)
	httpStr := strings.TrimSpace(httpFlag)
	// Fallback to -addr or -port if -http was not explicitly set
	if httpStr == "" && strings.TrimSpace(addrFlag) != "" {
		httpStr = strings.TrimSpace(addrFlag)
	} else if httpStr == "" && portFlag > 0 && portFlag <= 65535 {
		httpStr = fmt.Sprintf(":%d", portFlag)
	}

	var normHTTP, httpPort string
	if httpStr != "" {
		normHTTP, httpPort, err = normalizeAddr(httpStr)
		if err != nil {
			return nil, fmt.Errorf("invalid -http address %q: %w", httpStr, err)
		}
	}

	// 3. Resolve Node ID
	nodeID := strings.TrimSpace(idFlag)
	if nodeID == "" {
		if httpPort != "" {
			nodeID = httpPort
		} else {
			nodeID = tcpPort
		}
	}

	// 4. Resolve Data Directory
	dirStr := strings.TrimSpace(dataDirFlag)
	if dirStr == "" {
		dirStr = filepath.Join("data", nodeID)
	}

	return &ServerConfig{
		NodeID:   nodeID,
		HTTPAddr: normHTTP,
		TCPAddr:  normTCP,
		DataDir:  dirStr,
	}, nil
}

func main() {
	idFlag := flag.String("id", "", "Node identifier (e.g. 8001)")
	httpFlag := flag.String("http", "", "HTTP listen address (optional, e.g. :8001)")
	tcpFlag := flag.String("tcp", "", "TCP listen address (required, e.g. :9001)")
	dataDirFlag := flag.String("data", "", "Storage directory for WAL and SSTables (default: ./data/<id>)")
	addrFlag := flag.String("addr", "", "HTTP listen address alias (optional)")
	portFlag := flag.Int("port", 0, "HTTP listen port alias (optional)")
	flag.Parse()

	cfg, err := resolveConfig(*idFlag, *httpFlag, *tcpFlag, *dataDirFlag, *addrFlag, *portFlag)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	lsm, err := Open(cfg.DataDir, 4096, true)
	if err != nil {
		log.Fatalf("Failed to open LSM tree at %s: %v", cfg.DataDir, err)
	}
	defer lsm.Close()

	tcpListener, err := net.Listen("tcp", cfg.TCPAddr)
	if err != nil {
		log.Fatalf("Failed to listen on TCP %s: %v", cfg.TCPAddr, err)
	}
	defer tcpListener.Close()

	go func() {
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				return // listener closed during shutdown
			}
			go func(c net.Conn) {
				defer c.Close()
			}(conn)
		}
	}()

	var httpServer *http.Server
	if cfg.HTTPAddr != "" {
		srv := &Server{lsm: lsm}

		mux := http.NewServeMux()
		mux.HandleFunc("POST /put", srv.add)
		mux.HandleFunc("GET /get", srv.get)
		mux.HandleFunc("DELETE /del", srv.delete)

		httpServer = &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: mux,
		}

		go func() {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("HTTP server error: %v", err)
			}
		}()
		log.Printf("Starting node %s on TCP %s, HTTP %s storing data in %s", cfg.NodeID, cfg.TCPAddr, cfg.HTTPAddr, cfg.DataDir)
	} else {
		log.Printf("Starting node %s on TCP %s (HTTP disabled) storing data in %s", cfg.NodeID, cfg.TCPAddr, cfg.DataDir)
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan
	log.Println("Shutting down server...")

	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
}