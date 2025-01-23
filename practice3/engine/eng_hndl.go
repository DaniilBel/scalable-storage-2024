package engine

import (
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"log/slog"
	"os"
	"practice3/util"
)

func (e *Engine) handleSelect(rect [2][2]float64) []*geojson.Feature {
	var results []*geojson.Feature
	e.rtreeIndex.Search(rect[0], rect[1], func(min, max [2]float64, feature *geojson.Feature) bool {
		results = append(results, feature)
		return true
	})
	return results
}

func (e *Engine) handleInsert(feature *geojson.Feature) {
	//slog.Info("Inserting feature", "id", feature.ID)
	e.vclock[e.name]++            // Increment local LSN
	feature.ID = e.vclock[e.name] // Assign LSN as ID

	id := feature.ID.(uint64)
	e.Data[string(id)] = feature

	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Insert(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("insert", feature)
	e.broadcastTransaction(util.Transaction{
		Action:  "insert",
		Name:    e.name,
		LSN:     e.vclock[e.name],
		Feature: feature,
	})
}

func (e *Engine) handleReplace(feature *geojson.Feature) {
	e.vclock[e.name]++
	feature.ID = e.vclock[e.name]

	id := feature.ID.(uint64)
	e.Data[string(id)] = feature

	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Insert(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("replace", feature)
	e.broadcastTransaction(util.Transaction{
		Action:  "replace",
		Name:    e.name,
		LSN:     e.vclock[e.name],
		Feature: feature,
	})
}

func (e *Engine) handleDelete(feature *geojson.Feature) {
	e.vclock[e.name]++

	id := feature.ID.(uint64)
	delete(e.Data, string(id))

	bounds := feature.Geometry.Bound()
	e.rtreeIndex.Delete(bounds.Min, bounds.Max, feature)
	e.writeTransactionLog("delete", feature)
	e.broadcastTransaction(util.Transaction{
		Action:  "delete",
		Name:    e.name,
		LSN:     e.vclock[e.name],
		Feature: feature,
	})
}

func (e *Engine) handleCheckpoint() {
	tmpFile, err := os.CreateTemp("", e.ChkFile)
	if err != nil {
		slog.Error("Failed to create checkpoint file", "error", err)
		return
	}
	defer os.Remove(tmpFile.Name())

	for _, feature := range e.Data {
		transaction := util.Transaction{
			Action:  "insert",
			Name:    e.name,
			LSN:     e.vclock[e.name],
			Feature: feature,
		}

		data, err := json.Marshal(transaction)
		if err != nil {
			slog.Error("Failed to marshal transaction", "error", err)
			return
		}

		tmpFile.Write(append(data, '\n'))
	}

	if err := os.Rename(tmpFile.Name(), e.ChkFile); err != nil {
		slog.Error("Failed to replace checkpoint file", "error", err)
		return
	}

	if err := e.clearTransactionLog(); err != nil {
		slog.Error("Failed to clear transaction log", "error", err)
		return
	}

	slog.Info("Checkpoint created successfully")
}

func (e *Engine) handleReplicate(tx util.Transaction) {
	// Skip if the transaction is already applied
	if tx.LSN <= e.vclock[tx.Name] {
		return
	}

	// Apply the transaction
	featureJSON, err := json.Marshal(tx.Feature)
	if err != nil {
		slog.Error("Failed to marshal feature", "error", err)
		return
	}

	feature, err := geojson.UnmarshalFeature(featureJSON)
	if err != nil {
		slog.Error("Failed to unmarshal feature", "error", err)
		return
	}

	switch tx.Action {
	case "insert":
		e.handleInsert(feature)
	case "replace":
		e.handleReplace(feature)
	case "delete":
		e.handleDelete(feature)
	}

	e.vclock[tx.Name] = tx.LSN
}
