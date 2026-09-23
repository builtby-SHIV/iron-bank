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

type GetRequest struct {
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
	var req GetRequest
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
	w.WriteHeader(http.StatusFound)
	json.NewEncoder(w).Encode(map[string]string{ v: v})
}

func main() {
	mux := http.NewServeMux()

	kv := kvstore.NewKVStore()
	srv := &Server{ kv:kv }

	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)

	fmt.Println("Server starting on http://localhost:8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		panic(err)
	}
}