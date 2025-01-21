package main

import (
	"github.com/paulmach/orb/geojson"
	"io"
	"log/slog"
	"net/http"
	"os"
	"practice2/util"
	"strconv"
	"strings"
	"sync"
)

type Storage struct {
	mux       *http.ServeMux
	name      string
	dataFile  string
	tDataFile string
	engine    *Engine
	mu        sync.Mutex
	replicas  []string
	leader    bool
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	engine := NewEngine(name, "checkpoint_"+name+".json")
	s := &Storage{
		mux:       mux,
		name:      name,
		dataFile:  "geo.db.json",
		tDataFile: "test.geo.data.json",
		engine:    engine,
		replicas:  replicas,
		leader:    leader,
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
}

func (s *Storage) handleSelect(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	get := r.URL.Query().Get("rect")
	rect := strings.Split(get, ",")
	if len(rect) < 4 {
		http.Error(w, "Invalid rect parameter", http.StatusBadRequest)
		return
	}

	minx, err := strconv.ParseFloat(rect[0], 64)
	if err != nil {
		http.Error(w, "Invalid minx", http.StatusBadRequest)
		return
	}
	miny, err := strconv.ParseFloat(rect[1], 64)
	if err != nil {
		http.Error(w, "Invalid miny", http.StatusBadRequest)
		return
	}
	maxx, err := strconv.ParseFloat(rect[2], 64)
	if err != nil {
		http.Error(w, "Invalid maxx", http.StatusBadRequest)
		return
	}
	maxy, err := strconv.ParseFloat(rect[3], 64)
	if err != nil {
		http.Error(w, "Invalid maxy", http.StatusBadRequest)
		return
	}

	minF := [2]float64{minx, miny}
	maxF := [2]float64{maxx, maxy}

	var results []*geojson.Feature

	s.engine.spatialIndex.Search(minF, maxF, func(min [2]float64, max [2]float64, data interface{}) bool {
		sf, ok := data.(*util.SpatialFeature)
		if ok {
			results = append(results, sf.Feature)
		}
		return true
	})

	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Features = results

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	jsonData, _ := featureCollection.MarshalJSON()
	w.Write(jsonData)
}

func (s *Storage) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	//s.mu.Lock()
	//defer s.mu.Unlock()

	go s.engine.checkpoint()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Checkpoint initiated"))
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

	featureCollection.Append(feature)

	bbox := feature.Geometry.Bound()
	minF := [2]float64{bbox.Min[0], bbox.Min[1]}
	maxF := [2]float64{bbox.Max[0], bbox.Max[1]}
	s.engine.spatialIndex.Insert(minF, maxF, &util.SpatialFeature{Feature: feature})

	s.engine.data[feature.ID.(string)] = feature

	// Добавление транзакции в журнал
	//s.engine.transactions = append(s.engine.transactions, util.Transaction{
	//	Action:  "insert",
	//	Name:    s.engine.logFile,
	//	LSN:     s.engine.lsn,
	//	Feature: feature,
	//})

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

	data, err := os.ReadFile(s.tDataFile)
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
	err = os.WriteFile(s.tDataFile, write, 0644)
	if err != nil {
		http.Error(w, "Error writing data file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
