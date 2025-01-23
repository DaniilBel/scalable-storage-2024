package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"log/slog"
	"os"
	"practice3/util"
	"sync"
)

type Engine struct {
	Mu         sync.Mutex
	Data       map[string]*geojson.Feature    // Primary index by ID
	rtreeIndex rtree.RTreeG[*geojson.Feature] // Spatial index
	lsn        uint64
	TransLog   *os.File
	ChkFile    string
	ctx        context.Context
	cancel     context.CancelFunc
	CommandCh  chan util.Command
	Replicas   map[string]*websocket.Conn
	vclock     map[string]uint64 // Vector clock: node -> LSN
	name       string
	leader     bool
}

func NewEngine(ctx context.Context, transactionLogFile string, name string, leader bool) *Engine {

	engine := &Engine{
		Data:       make(map[string]*geojson.Feature),
		rtreeIndex: rtree.RTreeG[*geojson.Feature]{},
		ChkFile:    "checkpoint-*.json",
		CommandCh:  make(chan util.Command, 10),
		Replicas:   make(map[string]*websocket.Conn),
		vclock:     make(map[string]uint64),
		name:       name,
		leader:     leader,
	}

	if err := engine.loadCheckpoint(); err != nil {
		slog.Error("load checkpoint failed", "err", err)
		return nil
	}

	if err := engine.loadTransactionLog(transactionLogFile); err != nil {
		slog.Error("load transaction log failed", "err", err)
		return nil
	}

	engine.ctx, engine.cancel = context.WithCancel(ctx)
	go engine.run()

	return engine
}

func (e *Engine) run() {
	slog.Info("Engine goroutine started")
	defer slog.Info("Engine goroutine stopped")

	for {
		select {
		case cmd := <-e.CommandCh:
			//slog.Info("Received command", "action", cmd.Action)
			e.Mu.Lock()
			switch cmd.Action {
			case "insert":
				//slog.Info("Processing insert command")
				e.handleInsert(cmd.Feature)
			case "replace":
				//slog.Info("Processing replace command")
				e.handleReplace(cmd.Feature)
			case "delete":
				//slog.Info("Processing delete command")
				e.handleDelete(cmd.Feature)
			case "checkpoint":
				//slog.Info("Processing checkpoint command")
				e.handleCheckpoint()
				cmd.Response <- struct{}{}
			case "select":
				//slog.Info("Processing select command")
				cmd.Response <- e.handleSelect(cmd.Rect)
			case "replicate":
				//slog.Info("Processing replicate command")
				e.handleReplicate(cmd.Transaction)
			}
			e.Mu.Unlock()
		case <-e.ctx.Done():
			slog.Info("Engine stopped")
			return
		}
	}
}

func (e *Engine) Stop() {
	slog.Info("Engine is stopping")
	e.cancel()
}

// broadcastTransaction sends a transaction to all connected Replicas.
func (e *Engine) broadcastTransaction(tx util.Transaction) {
	for _, conn := range e.Replicas {
		if err := conn.WriteJSON(tx); err != nil {
			slog.Error("Failed to broadcast transaction", "error", err)
		}
	}
}

func (e *Engine) clearTransactionLog() error {
	if err := e.TransLog.Truncate(0); err != nil {
		return err
	}
	if _, err := e.TransLog.Seek(0, 0); err != nil {
		return err
	}
	return nil
}

func (e *Engine) writeTransactionLog(action string, feature *geojson.Feature) {
	transaction := util.Transaction{
		Action:  action,
		Name:    e.name,
		LSN:     e.vclock[e.name],
		Feature: feature,
	}

	data, err := json.Marshal(transaction)
	if err != nil {
		slog.Error("Failed to marshal transaction", "error", err)
		return
	}

	e.TransLog.Write(append(data, '\n'))
}

func (e *Engine) loadCheckpoint() error {
	file, err := os.Open(e.ChkFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tx util.Transaction
		if err := json.Unmarshal(scanner.Bytes(), &tx); err != nil {
			return err
		}
		e.handleReplicate(tx)
	}

	return scanner.Err()
}

func (e *Engine) loadTransactionLog(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("Failed to open transaction log", "error", err)
		return err
	}
	e.TransLog = file

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tx util.Transaction
		if err := json.Unmarshal(scanner.Bytes(), &tx); err != nil {
			return err
		}
		e.handleReplicate(tx)
	}

	return scanner.Err()
}
