package storage

import (
	"context"
	"github.com/gorilla/websocket"
	"log/slog"
	"net/http"
	"practice3/engine"
	"practice3/util"
	"sync"
	"time"
)

type Storage struct {
	mux          *http.ServeMux
	name         string
	DataFile     string
	Engine       *engine.Engine
	mu           sync.Mutex
	Replicas     []string
	leader       bool
	requestCount int
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx := context.Background()
	eng := engine.NewEngine(ctx, "transaction_"+name+".log", name, leader)
	s := &Storage{
		mux:  mux,
		name: name,
		//dataFile:  "geo.db.json",
		DataFile: "test.geo.data.json",
		Engine:   eng,
		Replicas: replicas,
		leader:   leader,
	}

	mux.HandleFunc("/"+name+"/replication", s.handleReplication)
	mux.HandleFunc("/"+name+"/checkpoint", s.handleCheckpoint)
	mux.HandleFunc("/"+name+"/select", s.handleSelect)
	mux.HandleFunc("/"+name+"/insert", s.handleInsert)
	mux.HandleFunc("/"+name+"/replace", s.handleReplace)
	mux.HandleFunc("/"+name+"/delete", s.handleDelete)

	go s.ConnectToReplicas()

	return s
}

func (s *Storage) Run() {
	slog.Info("Storage is running", "name", s.name)
}

func (s *Storage) Stop() {
	s.Engine.Stop()
	slog.Info("Storage is stopping", "name", s.name)
}

func (s *Storage) handleReplication(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
	if err != nil {
		http.Error(w, "Failed to upgrade to WebSocket", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	slog.Info("WebSocket connection established", "remote", r.RemoteAddr)

	s.Engine.Mu.Lock()
	s.Engine.Replicas[r.RemoteAddr] = conn
	s.Engine.Mu.Unlock()

	// Handle incoming messages
	for {
		var tx util.Transaction
		if err := conn.ReadJSON(&tx); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// slog.Error("WebSocket connection closed unexpectedly", "error", err)
			} else {
				slog.Info("WebSocket connection closed", "error", err)
			}
			break
		}

		slog.Info("Received transaction", "action", tx.Action, "name", tx.Name, "lsn", tx.LSN)

		s.Engine.CommandCh <- util.Command{Action: "replicate", Transaction: tx}
	}

	s.Engine.Mu.Lock()
	delete(s.Engine.Replicas, r.RemoteAddr)
	s.Engine.Mu.Unlock()
}

func (s *Storage) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	responseChan := make(chan any)
	s.Engine.CommandCh <- util.Command{Action: "checkpoint", Response: responseChan}
	<-responseChan

	w.WriteHeader(http.StatusOK)
}

// ConnectToReplicas connects to all Replicas in the Replicas list.
func (s *Storage) ConnectToReplicas() {
	for _, replica := range s.Replicas {
		go func(addr string) {
			for {
				wsURL := "ws://" + addr + "/replication"
				slog.Info("Connecting to replica", "url", wsURL)

				conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					slog.Error("Failed to connect to replica", "replica", addr, "error", err)
					time.Sleep(5 * time.Second) // Retry after 5 seconds
					continue
				}
				defer conn.Close()

				slog.Info("WebSocket connection established", "replica", addr)

				s.Engine.Mu.Lock()
				s.Engine.Replicas[addr] = conn
				s.Engine.Mu.Unlock()

				// Handle incoming messages
				for {
					var tx util.Transaction
					if err := conn.ReadJSON(&tx); err != nil {
						if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
							slog.Error("WebSocket connection closed unexpectedly", "replica", addr, "error", err)
						} else {
							slog.Info("WebSocket connection closed", "replica", addr, "error", err)
						}
						break
					}
					slog.Info("Received transaction", "replica", addr, "action", tx.Action, "name", tx.Name, "lsn", tx.LSN)

					s.Engine.CommandCh <- util.Command{Action: "replicate", Transaction: tx}
				}

				s.Engine.Mu.Lock()
				delete(s.Engine.Replicas, addr)
				s.Engine.Mu.Unlock()
			}
		}(replica)
	}
}
