package main

import (
	"context"
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"io"
	"log"
	"log/slog"
	"os"
	"practice2/util"
	"sync"
)

type Engine struct {
	mu             sync.Mutex
	data           map[string]*geojson.Feature // Primary index
	spatialIndex   *rtree.RTree
	logFile        string
	checkpointFile string
	lsn            uint64
	transactions   []util.Transaction
	commands       chan util.Transaction // Channel for transactions
	done           chan struct{}         // Channel to trigger checkpoints
}

//type Engine struct {
//	ctx           context.Context
//	cancel        context.CancelFunc
//	data          map[string]*geojson.Feature // Primary index
//	spatialIndex  *rtree.RTree                // Spatial index
//	transactions  []Transaction               // Transaction log
//	lsn           uint64                      // Log Sequence Number
//	transactionCh chan Transaction            // Channel for transactions
//	checkpointCh  chan struct{}               // Channel to trigger checkpoints
//}

func NewEngine(logFile, checkpointFile string) *Engine {
	return &Engine{
		data:           make(map[string]*geojson.Feature),
		spatialIndex:   &rtree.RTree{},
		logFile:        logFile,
		checkpointFile: checkpointFile,
		transactions:   []util.Transaction{},
		commands:       make(chan util.Transaction),
		done:           make(chan struct{}),
	}
}

func (e *Engine) Run(ctx context.Context) {
	go func() {
		e.loadCheckpoint()
		e.replayLog()
		for {
			select {
			case t := <-e.commands:
				e.handleTransaction(t)
			case <-ctx.Done():
				e.saveCheckpoint()
				close(e.done)
				return
			}
		}
	}()
}

func (e *Engine) Stop() {
	close(e.commands)
	<-e.done
}

func (e *Engine) loadCheckpoint() {
	data, err := os.ReadFile(e.checkpointFile)
	if err == nil {
		var features []*geojson.Feature
		err := json.Unmarshal(data, &features)
		if err != nil {
			slog.Error("Error loading checkpoint")
			return
		}
		for _, f := range features {
			e.data[f.ID.(string)] = f
			e.insertIntoSpatialIndex(f)
		}
	}
}

func (e *Engine) replayLog() {
	file, err := os.Open(e.logFile)
	if err != nil {
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var t util.Transaction
		if err := decoder.Decode(&t); err == io.EOF {
			break
		} else if err != nil {
			continue
		}
		e.handleTransaction(t)
	}
}

func (e *Engine) handleTransaction(t util.Transaction) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch t.Action {
	case "insert":
		e.data[t.Feature.ID.(string)] = t.Feature
		e.insertIntoSpatialIndex(t.Feature)
	case "replace":
		e.deleteFromSpatialIndex(e.data[t.Feature.ID.(string)])
		e.data[t.Feature.ID.(string)] = t.Feature
		e.insertIntoSpatialIndex(t.Feature)
	case "delete":
		e.deleteFromSpatialIndex(e.data[t.Feature.ID.(string)])
		delete(e.data, t.Feature.ID.(string))
	}

	e.lsn++
	e.logTransaction(t)
}

func (e *Engine) logTransaction(t util.Transaction) {
	file, err := os.OpenFile(e.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Error opening log file")
		return
	}
	defer file.Close()

	err = json.NewEncoder(file).Encode(t)
	if err != nil {
		return
	}
}

func (e *Engine) saveCheckpoint() {
	e.mu.Lock()
	defer e.mu.Unlock()

	file, err := os.Create(e.checkpointFile)
	if err != nil {
		return
	}
	defer file.Close()

	var features []*geojson.Feature
	for _, f := range e.data {
		features = append(features, f)
	}

	err = json.NewEncoder(file).Encode(features)
	if err != nil {
		return
	}
	err = os.Remove(e.logFile)
	if err != nil {
		return
	}
}

func (e *Engine) checkpoint() {
	e.mu.Lock()
	defer e.mu.Unlock()

	data := make(map[string]*geojson.Feature)
	for id, feature := range e.data {
		data[id] = feature
	}

	// Сериализация текущего состояния в файл checkpoint
	serializedData, err := json.Marshal(data)
	if err != nil {
		log.Println("Error serializing checkpoint data:", err)
		return
	}

	err = os.WriteFile(e.checkpointFile, serializedData, 0644)
	if err != nil {
		log.Println("Error writing checkpoint file:", err)
		return
	}

	e.transactions = []util.Transaction{}
	log.Println("Checkpoint completed")
}

func (e *Engine) insertIntoSpatialIndex(feature *geojson.Feature) {
	bounds := feature.Geometry.Bound()
	minF := [2]float64{bounds.Min.X(), bounds.Min.Y()}
	maxF := [2]float64{bounds.Max.X(), bounds.Max.Y()}
	e.spatialIndex.Insert(minF, maxF, feature)
}

func (e *Engine) deleteFromSpatialIndex(feature *geojson.Feature) {
	bounds := feature.Geometry.Bound()
	minF := [2]float64{bounds.Min.X(), bounds.Min.Y()}
	maxF := [2]float64{bounds.Max.X(), bounds.Max.Y()}
	e.spatialIndex.Delete(minF, maxF, feature)
}
