package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"iron-bank/kvstore"
	"net/http"
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
	kv *kvstore.KVStore
}

func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	var req AddRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.kv.Set(req.Key, req.Val)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(Response {
		Message:"Key-Val pair created",
	})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	var nf *kvstore.NotFoundError

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	v, err := s.kv.Get(req.Key)

	if errors.As(err, &nf) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{ req.Key: v})
}

func ( s *Server) delete(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.kv.Del(req.Key)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{ "msg": "Key successfully deleted" })
}

func main() {
	mux := http.NewServeMux()

	kv := kvstore.NewKVStore()
	srv := &Server{ kv:kv }

	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)
	mux.HandleFunc("DELETE /del", srv.delete)

	fmt.Println("Server starting on http://localhost:8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		panic(err)
	}
}