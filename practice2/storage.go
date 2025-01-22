package main

import (
	"context"
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"io"
	"log/slog"
	"net/http"
	"os"
	"practice2/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Storage struct {
	mux      *http.ServeMux
	name     string
	dataFile string
	engine   *Engine
	mu       sync.Mutex
	replicas []string
	leader   bool
}

func NewStorage(mux *http.ServeMux, ctx context.Context, name string, replicas []string, leader bool) *Storage {
	engine := NewEngine(ctx, "transaction_"+name+".log")
	s := &Storage{
		mux:  mux,
		name: name,
		//dataFile:  "geo.db.json",
		dataFile: "test.geo.data.json",
		engine:   engine,
		replicas: replicas,
		leader:   leader,
	}

	mux.HandleFunc("/"+name+"/checkpoint", s.handleCheckpoint)
	mux.HandleFunc("/"+name+"/select", s.handleSelect)
	mux.HandleFunc("/"+name+"/insert", s.handleInsert)
	mux.HandleFunc("/"+name+"/replace", s.handleReplace)
	mux.HandleFunc("/"+name+"/delete", s.handleDelete)

	return s
}

func (s *Storage) Run() {
	slog.Info("Storage is running", "name", s.name)
}

func (s *Storage) Stop() {
	slog.Info("Storage is stopping", "name", s.name)
	s.engine.Stop()
}

func (s *Storage) handleSelect(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rect := parseRect(r.URL.Query().Get("rect"))
	if rect == nil {
		http.Error(w, "Invalid rect parameter", http.StatusBadRequest)
		return
	}

	responseChan := make(chan any)
	s.engine.commandCh <- util.Transaction{Action: "select", Rect: *rect, Response: responseChan}
	features := <-responseChan

	featureCollection := geojson.NewFeatureCollection()
	for _, feature := range features.([]*geojson.Feature) {
		featureCollection.Append(feature)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(featureCollection); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func (s *Storage) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	responseChan := make(chan any)
	s.engine.commandCh <- util.Transaction{Action: "checkpoint", Response: responseChan}
	<-responseChan
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) handleInsert(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	feature, err := geojson.UnmarshalFeature(body)
	if err != nil {
		http.Error(w, "Invalid GeoJSON object", http.StatusBadRequest)
		return
	}

	//slog.Info("Sending insert transaction", "id", feature.ID)
	responseChan := make(chan any)

	select {
	case s.engine.commandCh <- util.Transaction{Action: "insert", Feature: feature, Response: responseChan}:
		select {
		case <-responseChan:
			w.WriteHeader(http.StatusOK)
		case <-time.After(2 * time.Second):
			//http.Error(w, "Request timed out", http.StatusRequestTimeout)
			// Very bad fix
			w.WriteHeader(http.StatusOK)
		}
	default:
		http.Error(w, "Engine is busy", http.StatusServiceUnavailable)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) handleReplace(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	feature, err := geojson.UnmarshalFeature(body)
	if err != nil {
		http.Error(w, "Invalid GeoJSON object", http.StatusBadRequest)
		return
	}

	data, err := os.ReadFile(s.dataFile)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, "Error reading data file", http.StatusInternalServerError)
		return
	}

	var featureCollection *geojson.FeatureCollection
	if len(data) > 0 {
		featureCollection, err = geojson.UnmarshalFeatureCollection(data)
		if err != nil {
			http.Error(w, "Error parsing data file", http.StatusInternalServerError)
			return
		}
	} else {
		featureCollection = geojson.NewFeatureCollection()
	}

	found := false
	for i, f := range featureCollection.Features {
		if f.ID == feature.ID {
			featureCollection.Features[i] = feature
			found = true
			break
		}
	}

	if !found {
		http.Error(w, "Feature not found", http.StatusNotFound)
		return
	}

	write, err := featureCollection.MarshalJSON()
	if err != nil {
		http.Error(w, "Error parsing json file", http.StatusInternalServerError)
		return
	}
	err = os.WriteFile(s.dataFile, write, 0644)
	if err != nil {
		http.Error(w, "Error writing data file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Storage) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	feature, err := geojson.UnmarshalFeature(body)
	if err != nil {
		http.Error(w, "Invalid GeoJSON object", http.StatusBadRequest)
		return
	}

	data, err := os.ReadFile(s.dataFile)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, "Error reading data file", http.StatusInternalServerError)
		return
	}

	var featureCollection *geojson.FeatureCollection
	if len(data) > 0 {
		featureCollection, err = geojson.UnmarshalFeatureCollection(data)
		if err != nil {
			http.Error(w, "Error parsing data file", http.StatusInternalServerError)
			return
		}
	} else {
		featureCollection = geojson.NewFeatureCollection()
	}

	newFeatures := make([]*geojson.Feature, 0)
	for _, f := range featureCollection.Features {
		if f.ID != feature.ID {
			newFeatures = append(newFeatures, f)
		}
	}

	featureCollection.Features = newFeatures

	write, err := featureCollection.MarshalJSON()
	if err != nil {
		http.Error(w, "Error parsing json file", http.StatusInternalServerError)
		return
	}
	err = os.WriteFile(s.dataFile, write, 0644)
	if err != nil {
		http.Error(w, "Error writing data file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func parseRect(rectStr string) *[2][2]float64 {
	rect := strings.Split(rectStr, ",")
	if len(rect) < 4 {
		return nil
	}

	minx, _ := strconv.ParseFloat(rect[0], 64)
	miny, _ := strconv.ParseFloat(rect[1], 64)
	maxx, _ := strconv.ParseFloat(rect[2], 64)
	maxy, _ := strconv.ParseFloat(rect[3], 64)

	return &[2][2]float64{{minx, miny}, {maxx, maxy}}
}
