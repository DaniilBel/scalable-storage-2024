package main

import (
	"log/slog"
	"net/http"
)

type Router struct {
	mux   *http.ServeMux
	nodes [][]string
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	r := &Router{mux: mux, nodes: nodes}
	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))

	mux.HandleFunc("/insert", r.handleRedirect)
	mux.HandleFunc("/replace", r.handleRedirect)
	mux.HandleFunc("/delete", r.handleRedirect)
	mux.HandleFunc("/select", r.handleRedirect)
	mux.HandleFunc("/checkpoint", r.handleRedirect)
	mux.HandleFunc("/replication", r.handleRedirect)

	return r
}

func (r *Router) Run() {
	slog.Info("Router is running")
}

func (r *Router) Stop() {
	slog.Info("Router is stopping")
}

func (r *Router) handleRedirect(w http.ResponseWriter, req *http.Request) {
	node := req.URL.Query().Get("node")
	if node == "" {
		// Default to the first node
		node = r.nodes[0][0]
	}

	// Check if the node exists in the nodes list
	found := false
	for _, nodeList := range r.nodes {
		for _, n := range nodeList {
			if n == node {
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		http.Error(w, "Invalid node specified", http.StatusBadRequest)
		return
	}

	target := "/" + node + req.URL.Path
	http.Redirect(w, req, target, http.StatusTemporaryRedirect)
}
