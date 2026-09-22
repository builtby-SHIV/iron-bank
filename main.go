package main

import (
	"encoding/json"
	"fmt"
	"iron-bank/kvstore"
	"net/http"
)

type Response struct {
	Message string `json:"message"`
}

type Request struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

func add(w http.ResponseWriter, r *http.Request) {
	var req Request

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kv.Set(req.Key, req.Val)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(Response {
		Message:"Key-Val pair created",
	})
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /put", add)
	mux.HandleFunc("GET /get", get)

	fmt.Println("Server starting on http://localhost:8080")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		panic(err)
	}
}