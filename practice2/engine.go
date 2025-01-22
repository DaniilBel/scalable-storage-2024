package main

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"io"
	"log/slog"
	"os"
	"practice2/util"
	"sync"
)

type Engine struct {
	mu         sync.Mutex
	data       map[string]*geojson.Feature    // Primary index by ID
	rtreeIndex rtree.RTreeG[*geojson.Feature] // Spatial index
	lsn        uint64
	transLog   *os.File
	chkFile    string
	ctx        context.Context
	cancel     context.CancelFunc
	commandCh  chan util.Transaction // channel for commands
}

func NewEngine(ctx context.Context, transactionLogFile string) *Engine {

	engine := &Engine{
		data:       make(map[string]*geojson.Feature),
		rtreeIndex: rtree.RTreeG[*geojson.Feature]{},
		chkFile:    "checkpoint-*.json",
		commandCh:  make(chan util.Transaction, 10),
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
		case cmd := <-e.commandCh:
			slog.Info("Received command", "action", cmd.Action)
			e.mu.Lock()
			switch cmd.Action {
			case "insert":
				slog.Info("Processing insert command")
				e.handleInsert(cmd.Feature)
			case "replace":
				slog.Info("Processing replace command")
				e.handleReplace(cmd.Feature)
			case "delete":
				slog.Info("Processing delete command")
				e.handleDelete(cmd.Feature)
			case "checkpoint":
				slog.Info("Processing checkpoint command")
				e.handleCheckpoint()
				cmd.Response <- struct{}{}
			case "select":
				slog.Info("Processing select command")
				cmd.Response <- e.handleSelect(cmd.Rect)
			}
			e.mu.Unlock()
		//case <-time.After(1 * time.Second):
		//	slog.Info("Unknown tasks")
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

func (e *Engine) handleInsert(feature *geojson.Feature) {
	e.lsn++
	e.data[feature.ID.(string)] = feature
	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Insert(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("insert", feature)
}

func (e *Engine) handleReplace(feature *geojson.Feature) {
	e.lsn++
	e.data[feature.ID.(string)] = feature
	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Insert(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("replace", feature)
}

func (e *Engine) handleDelete(feature *geojson.Feature) {
	e.lsn++
	delete(e.data, feature.ID.(string))
	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Delete(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("delete", feature)
}

func (e *Engine) handleCheckpoint() {
	tmpFile, err := os.CreateTemp("", e.chkFile)
	if err != nil {
		slog.Error("Failed to create checkpoint file", "error", err)
		return
	}
	defer os.Remove(tmpFile.Name())

	for _, feature := range e.data {
		transaction := struct {
			Action  string      `json:"action"`
			LSN     uint64      `json:"lsn"`
			Feature interface{} `json:"feature"`
		}{
			Action:  "insert",
			LSN:     e.lsn,
			Feature: feature,
		}

		data, err := json.Marshal(transaction)
		if err != nil {
			slog.Error("Failed to marshal transaction", "error", err)
			return
		}

		data = append(data, '\n')

		if _, err := tmpFile.Write(data); err != nil {
			slog.Error("Failed to write to temporary checkpoint file", "error", err)
			return
		}
	}

	if err := tmpFile.Close(); err != nil {
		slog.Error("Failed to close temporary checkpoint file", "error", err)
		return
	}

	if err := os.Rename(tmpFile.Name(), e.chkFile); err != nil {
		slog.Error("Failed to replace checkpoint file", "error", err)
		return
	}

	if err := e.clearTransactionLog(); err != nil {
		slog.Error("Failed to clear transaction log", "error", err)
		return
	}

	slog.Info("Checkpoint created successfully")
}

func (e *Engine) clearTransactionLog() error {
	if err := e.transLog.Truncate(0); err != nil {
		return err
	}
	if _, err := e.transLog.Seek(0, 0); err != nil {
		return err
	}
	return nil
}

func (e *Engine) handleSelect(rect [2][2]float64) []*geojson.Feature {
	var results []*geojson.Feature
	e.rtreeIndex.Search(rect[0], rect[1], func(min, max [2]float64, feature *geojson.Feature) bool {
		results = append(results, feature)
		return true
	})
	return results
}

func (e *Engine) writeTransactionLog(action string, feature *geojson.Feature) {
	transaction := struct {
		Action  string      `json:"action"`
		LSN     uint64      `json:"lsn"`
		Feature interface{} `json:"feature"`
	}{
		Action:  action,
		LSN:     e.lsn,
		Feature: feature,
	}

	data, err := json.Marshal(transaction)
	if err != nil {
		slog.Error("Failed to marshal transaction", "error", err)
		return
	}

	data = append(data, '\n')

	if _, err := e.transLog.Write(data); err != nil {
		slog.Error("Failed to write transaction log", "error", err)
	}
}

func (e *Engine) loadCheckpoint() error {
	file, err := os.OpenFile(e.chkFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		var transaction struct {
			Action  string      `json:"action"`
			LSN     uint64      `json:"lsn"`
			Feature interface{} `json:"feature"`
		}
		if err := json.Unmarshal(line, &transaction); err != nil {
			slog.Error("Failed to decode transaction", "error", err)
			continue
		}

		featureJSON, err := json.Marshal(transaction.Feature)
		if err != nil {
			slog.Error("Failed to marshal feature", "error", err)
			continue
		}

		feature, err := geojson.UnmarshalFeature(featureJSON)
		if err != nil {
			slog.Error("Failed to unmarshal feature", "error", err)
			continue
		}

		e.data[feature.ID.(string)] = feature
		e.rtreeIndex.Insert(feature.Geometry.Bound().Min, feature.Geometry.Bound().Max, feature)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) loadTransactionLog(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("Failed to open transaction log", "error", err)
		return err
	}
	e.transLog = file

	fileInfo, err := file.Stat()
	if err != nil {
		slog.Error("Failed to get file info", "error", err)
		return err
	}
	if fileInfo.Size() == 0 {
		slog.Info("Transaction log is empty, skipping load")
		return nil
	}

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var transaction struct {
			Action  string `json:"action"`
			LSN     uint64 `json:"lsn"`
			Feature any    `json:"feature"`
		}
		if err := decoder.Decode(&transaction); err != nil {
			if err == io.EOF {
				slog.Info("Reached end of transaction log")
				break
			}
			slog.Error("Failed to decode transaction", "error", err)
			return err
		}

		featureJSON, err := json.Marshal(transaction.Feature)
		if err != nil {
			slog.Error("Failed to marshal feature", "error", err)
			return err
		}

		feature, err := geojson.UnmarshalFeature(featureJSON)
		if err != nil {
			slog.Error("Failed to unmarshal feature", "error", err)
			return err
		}
		switch transaction.Action {
		case "insert":
			e.handleInsert(feature)
		case "replace":
			e.handleReplace(feature)
		case "delete":
			e.handleDelete(feature)
		}
	}

	return nil
}
