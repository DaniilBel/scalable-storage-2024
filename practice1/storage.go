package main

import (
	"github.com/paulmach/orb/geojson"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
)

type Storage struct {
	mux       *http.ServeMux
	name      string
	dataFile  string
	tDataFile string
	mu        sync.Mutex
	replicas  []string
	leader    bool
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	s := &Storage{
		mux:       mux,
		name:      name,
		dataFile:  "geo.db.json",
		tDataFile: "test.geo.data.json",
		replicas:  replicas,
		leader:    leader,
	}

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

	data, err := os.ReadFile(s.dataFile)
	if err != nil {
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	write, err := featureCollection.MarshalJSON()
	if err != nil {
		http.Error(w, "Error parsing json file", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(write)
	if err != nil {
		return
	}

	_, err = w.Write([]byte(`{"type": "FeatureCollection", "features": []}`))
	if err != nil {
		return
	}

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
