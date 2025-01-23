package main

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"practice3/storage"
	"practice3/util"
	"strings"
	"testing"
	"time"
)

var testName = "test"

func setup() (*Router, *storage.Storage, *http.ServeMux) {
	mux := http.NewServeMux()
	s := storage.NewStorage(mux, testName, []string{}, true)
	r := NewRouter(mux, [][]string{{testName}})
	return r, s, mux
}

func TestHandleReplication(t *testing.T) {
	r, s, mux := setup()

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	server := httptest.NewServer(mux)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/" + testName + "/replication"

	// Connect to the WebSocket endpoint
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	defer conn.Close()

	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"

	// Create a replication transaction
	tx := util.Transaction{
		Action:  "insert",
		Name:    "node2",
		LSN:     1,
		Feature: feature,
	}

	// Send the transaction to the WebSocket connection
	if err := conn.WriteJSON(tx); err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}

	responseChan := make(chan any)
	s.Engine.CommandCh <- util.Command{Action: "select", Rect: [2][2]float64{{0, 0}, {3, 3}}, Response: responseChan}
	features := <-responseChan

	found := false
	for _, f := range features.([]*geojson.Feature) {
		if f.ID.(uint64) == 1 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Feature was not replicated: got %+v", features)
	}
}

func TestHandleSelect(t *testing.T) {
	r, s, mux := setup()

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature1 := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature1.ID = "1"
	feature2 := geojson.NewFeature(orb.Point{3.0, 4.0})
	feature2.ID = "2"

	// Hard insert
	body1, _ := json.Marshal(feature1)
	body2, _ := json.Marshal(feature2)

	req1 := httptest.NewRequest(http.MethodPost, "/"+testName+"/insert", bytes.NewReader(body1))
	req2 := httptest.NewRequest(http.MethodPost, "/"+testName+"/insert", bytes.NewReader(body2))
	rr1 := httptest.NewRecorder()
	rr2 := httptest.NewRecorder()

	mux.ServeHTTP(rr1, req1)
	mux.ServeHTTP(rr2, req2)

	// Query with rect parameter
	rect := "0,0,2,3" // Should include point 1 and not include point 2
	req := httptest.NewRequest(http.MethodGet, "/select?rect="+rect, nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest(http.MethodGet, rr.Header().Get("Location")+"?rect="+rect, nil)
		if err != nil {
			t.Fatal(err)
		}
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("Failed to insert feature: got status %v", rr.Code)
		}

		var result geojson.FeatureCollection
		err = json.NewDecoder(rr.Body).Decode(&result)
		if err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(result.Features) != 1 || result.Features[0].ID.(float64) != 1 {
			t.Errorf("Unexpected result: got %+v", result)
		}
	} else {
		t.Fatalf("Unexpected response: got %v", rr.Body.String())
	}
}

func TestHandleInsert(t *testing.T) {
	r, s, mux := setup()

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "1"
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

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	// Hard insert Data
	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"

	b, _ := feature.MarshalJSON()
	rq, _ := http.NewRequest("POST", "/"+testName+"/insert", bytes.NewReader(b))
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

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"
	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Append(feature)

	write, _ := featureCollection.MarshalJSON()
	err := os.WriteFile(s.DataFile, write, 0644)
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	defer os.Remove(s.DataFile)

	deleteFeature := geojson.NewFeature(orb.Point{})
	deleteFeature.ID = "1"
	body, _ := json.Marshal(deleteFeature)

	req := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, _ = http.NewRequest(http.MethodPost, rr.Header().Get("Location"), bytes.NewReader(body))
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		data, err := os.ReadFile(s.DataFile)
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

func TestHandleCheckpoint(t *testing.T) {
	r, s, mux := setup()

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
		if err := os.Remove(s.Engine.ChkFile); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete checkpoint-*.json: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	feature1 := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature1.ID = "1"
	feature2 := geojson.NewFeature(orb.Point{3.0, 4.0})
	feature2.ID = "2"

	body1, _ := json.Marshal(feature1)
	body2, _ := json.Marshal(feature2)

	// Hard insert
	req1 := httptest.NewRequest(http.MethodPost, "/"+testName+"/insert", bytes.NewReader(body1))
	req2 := httptest.NewRequest(http.MethodPost, "/"+testName+"/insert", bytes.NewReader(body2))
	rr1 := httptest.NewRecorder()
	rr2 := httptest.NewRecorder()

	mux.ServeHTTP(rr1, req1)
	mux.ServeHTTP(rr2, req2)

	req := httptest.NewRequest(http.MethodPost, "/checkpoint", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, _ = http.NewRequest(http.MethodPost, rr.Header().Get("Location"), nil)
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		// Verify checkpoint file
		checkpointData, err := os.ReadFile(s.Engine.ChkFile)
		if err != nil {
			t.Fatalf("Failed to read checkpoint file: %v", err)
		}

		var checkpointFeatures []geojson.Feature
		lines := strings.Split(string(checkpointData), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			var transaction struct {
				Action  string      `json:"action"`
				LSN     uint64      `json:"lsn"`
				Feature interface{} `json:"feature"`
			}
			if err := json.Unmarshal([]byte(line), &transaction); err != nil {
				t.Fatalf("Failed to unmarshal checkpoint transaction: %v", err)
			}

			featureJSON, err := json.Marshal(transaction.Feature)
			if err != nil {
				t.Fatalf("Failed to marshal feature: %v", err)
			}

			feature, err := geojson.UnmarshalFeature(featureJSON)
			if err != nil {
				t.Fatalf("Failed to unmarshal feature: %v", err)
			}

			checkpointFeatures = append(checkpointFeatures, *feature)
		}

		if len(checkpointFeatures) != 2 {
			t.Errorf("Checkpoint file contains incorrect number of features: got %v want %v", len(checkpointFeatures), 2)
		}

		// Verify transaction log is cleared
		logData, err := os.ReadFile(s.Engine.TransLog.Name())
		if err != nil {
			t.Fatalf("Failed to read transaction log: %v", err)
		}

		if len(logData) != 0 {
			t.Errorf("Transaction log was not cleared after checkpoint")
		}
	}
}

func TestConnectToReplicas(t *testing.T) {
	r, s, _ := setup()

	t.Cleanup(func() {
		if err := os.Remove("transaction_" + testName + ".log"); err != nil && !os.IsNotExist(err) {
			t.Errorf("Failed to delete transaction.log: %v", err)
		}
	})

	go func() { s.Run() }()
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	// Create a test HTTP server for the replica
	replicaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatalf("Failed to upgrade to WebSocket: %v", err)
		}
		defer conn.Close()

		feature := geojson.NewFeature(orb.Point{1.0, 2.0})
		feature.ID = "1"
		tx := util.Transaction{
			Action:  "insert",
			Name:    "replica",
			LSN:     1,
			Feature: feature,
		}
		if err := conn.WriteJSON(tx); err != nil {
			t.Fatalf("Failed to send transaction: %v", err)
		}
	}))
	defer replicaServer.Close()

	// Update the replicas list
	s.Replicas = []string{strings.TrimPrefix(replicaServer.URL, "http://")}
	go s.ConnectToReplicas()

	time.Sleep(100 * time.Millisecond)

	responseChan := make(chan any)
	s.Engine.CommandCh <- util.Command{Action: "select", Rect: [2][2]float64{{0, 0}, {3, 3}}, Response: responseChan}
	features := <-responseChan

	// Check if the feature was added
	found := false
	for _, f := range features.([]*geojson.Feature) {
		if f.ID.(uint64) == 1 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Feature was not replicated: got %+v", features)
	}
}
