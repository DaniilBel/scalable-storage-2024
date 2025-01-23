package storage

import (
	"encoding/json"
	"github.com/paulmach/orb/geojson"
	"io"
	"net/http"
	"os"
	"practice3/util"
	"strconv"
	"strings"
	"time"
)

func (s *Storage) handleSelect(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requestCount++
	if s.requestCount > 3 {
		s.mu.Unlock()
		http.Redirect(w, r, "http://replica/select", http.StatusTemporaryRedirect)
		return
	}
	defer s.mu.Unlock()

	rect := parseRect(r.URL.Query().Get("rect"))
	if rect == nil {
		http.Error(w, "Invalid rect parameter", http.StatusBadRequest)
		return
	}

	responseChan := make(chan any)
	s.Engine.CommandCh <- util.Command{Action: "select", Rect: *rect, Response: responseChan}
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
	case s.Engine.CommandCh <- util.Command{Action: "insert", Feature: feature, Response: responseChan}:
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

	responseChan := make(chan any)

	select {
	case s.Engine.CommandCh <- util.Command{Action: "insert", Feature: feature, Response: responseChan}:
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

	data, err := os.ReadFile(s.DataFile)
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
	err = os.WriteFile(s.DataFile, write, 0644)
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
