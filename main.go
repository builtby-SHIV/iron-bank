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

func extractPort(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", errors.New("empty address")
	}

	// Case 1: Pure number like "8001"
	if p, err := strconv.Atoi(addr); err == nil && p > 0 && p <= 65535 {
		return addr, nil
	}

	// Case 2: Host:Port or :Port (e.g., ":8001", "127.0.0.1:8001", "[::1]:8001")
	if strings.Contains(addr, ":") {
		_, port, err := net.SplitHostPort(addr)
		if err == nil && port != "" {
			if p, err := strconv.Atoi(port); err == nil && p > 0 && p <= 65535 {
				return port, nil
			}
		}

		// Fallback for cases like ":8001" if SplitHostPort had issues
		lastColon := strings.LastIndex(addr, ":")
		if lastColon != -1 && lastColon < len(addr)-1 {
			candidate := addr[lastColon+1:]
			if p, err := strconv.Atoi(candidate); err == nil && p > 0 && p <= 65535 {
				return candidate, nil
			}
		}
	}

	return "", fmt.Errorf("invalid port in address %q", addr)
}

func resolveConfig(portFlag int, addrFlag, dataDirFlag, nodeIDFlag string) (string, string, string, string, error) {
	var portStr string
	if portFlag > 0 && portFlag <= 65535 {
		portStr = strconv.Itoa(portFlag)
	}

	addrStr := strings.TrimSpace(addrFlag)
	if addrStr != "" {
		extractedPort, err := extractPort(addrStr)
		if err != nil && portStr == "" {
			return "", "", "", "", fmt.Errorf("failed to determine port from addr %q: %w", addrStr, err)
		}
		if portStr == "" {
			portStr = extractedPort
		}
		if !strings.Contains(addrStr, ":") {
			addrStr = ":" + addrStr
		}
	}

	if portStr == "" {
		portStr = "8001"
	}

	if addrStr == "" {
		addrStr = ":" + portStr
	}

	dirStr := strings.TrimSpace(dataDirFlag)
	if dirStr == "" {
		dirStr = filepath.Join("data", portStr)
	}

	idStr := strings.TrimSpace(nodeIDFlag)
	if idStr == "" {
		idStr = fmt.Sprintf("node-%s", portStr)
	}

	return addrStr, dirStr, idStr, portStr, nil
}

func main() {
	portFlag := flag.Int("port", 0, "HTTP listen port (e.g. 8001)")
	addrFlag := flag.String("addr", "", "HTTP listen address (e.g. :8001, 127.0.0.1:8001)")
	dataDirFlag := flag.String("data", "", "Storage directory for WAL and SSTables (default: ./data/<port>)")
	nodeIDFlag := flag.String("id", "", "Node identifier (default: node-<port>)")
	flag.Parse()

	addr, dataDir, nodeID, _, err := resolveConfig(*portFlag, *addrFlag, *dataDirFlag, *nodeIDFlag)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	lsm, err := Open(dataDir, 4096, true)
	if err != nil {
		log.Fatalf("Failed to open LSM tree at %s: %v", dataDir, err)
	}
	defer lsm.Close()

	srv := &Server{lsm: lsm}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)
	mux.HandleFunc("DELETE /del", srv.delete)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stopChan
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	log.Printf("Starting node %s on %s storing data in %s", nodeID, addr, dataDir)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}