package main

import (
	"bytes"
	"encoding/json"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var test_name = "test"

func setup() (*Router, *Storage, *http.ServeMux) {
	mux := http.NewServeMux()
	s := NewStorage(mux, test_name, []string{}, true)
	r := NewRouter(mux, [][]string{{test_name}})
	return r, s, mux
}

func TestHandleSelect(t *testing.T) {
	r, s, mux := setup()

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"
	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Append(feature)

	write, _ := featureCollection.MarshalJSON()
	err := os.WriteFile(s.dataFile, write, 0644)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	defer os.Remove(s.dataFile)

	req := httptest.NewRequest(http.MethodGet, "/select", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		location := rr.Header().Get("Location")
		req, _ = http.NewRequest(http.MethodGet, location, nil)
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		var result geojson.FeatureCollection
		err = json.NewDecoder(rr.Body).Decode(&result)
		if err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(result.Features) != 1 || result.Features[0].ID != "1" {
			t.Errorf("Unexpected result: got %+v", result)
		}
	}
}

func TestHandleInsert(t *testing.T) {
	r, s, mux := setup()

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", "/insert", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func TestHandleReplace(t *testing.T) {
	r, s, mux := setup()

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	// Hard insert Data
	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"
	b, _ := feature.MarshalJSON()
	rq, _ := http.NewRequest("POST", "/"+test_name+"/insert", bytes.NewReader(b))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, rq)

	replaceFeature := geojson.NewFeature(orb.Point{1.0, 2.0})
	replaceFeature.ID = "1"
	body, _ := json.Marshal(replaceFeature)

	req := httptest.NewRequest(http.MethodPost, "/replace", bytes.NewReader(body))
	rr = httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		location := rr.Header().Get("Location")
		req, _ = http.NewRequest(http.MethodPost, location, bytes.NewReader(body))
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func TestHandleDelete(t *testing.T) {
	r, s, mux := setup()

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"
	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Append(feature)

	write, _ := featureCollection.MarshalJSON()
	err := os.WriteFile(s.tDataFile, write, 0644)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	defer os.Remove(s.tDataFile)

	// Delete point
	deleteFeature := geojson.NewFeature(orb.Point{})
	deleteFeature.ID = "1"
	body, _ := json.Marshal(deleteFeature)

	req := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		location := rr.Header().Get("Location")
		req, _ = http.NewRequest(http.MethodPost, location, bytes.NewReader(body))
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		data, err := os.ReadFile(s.tDataFile)
		if err != nil {
			t.Fatalf("Failed to read storage file: %v", err)
		}

		var result geojson.FeatureCollection
		err = json.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("Failed to decode storage file: %v", err)
		}

		if len(result.Features) != 0 {
			t.Errorf("Feature was not deleted, got %+v", result.Features)
		}

	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}
