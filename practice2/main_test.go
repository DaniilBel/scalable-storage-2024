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

	// Добавление тестового объекта в хранилище через handleInsert
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "/insert", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err = http.NewRequest(http.MethodPost, rr.Header().Get("Location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		// Проверка успешного добавления
		if rr.Code != http.StatusOK {
			t.Fatalf("Failed to insert feature: got status %v", rr.Code)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/select?rect=0,0,2,3", nil)
	rr = httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err = http.NewRequest(http.MethodPost, rr.Header().Get("Location")+"?rect=0,0,2,3", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
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

func TestHandleCheckpoint(t *testing.T) {
	r, s, mux := setup()

	// Запуск горутин Storage и Router
	go func() { s.Run() }()
	go func() { r.Run() }()

	// Остановка горутин после завершения теста
	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	// Создание тестового объекта GeoJSON с ID "1"
	feature := geojson.NewFeature(orb.Point{1.0, 2.0})
	feature.ID = "1"

	// Добавление тестового объекта в хранилище через handleInsert
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "/insert", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err = http.NewRequest(http.MethodPost, rr.Header().Get("Location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("Failed to insert feature: got status %v", rr.Code)
		}
	}

	// Выполнение запроса для создания чекпоинта
	req = httptest.NewRequest(http.MethodPost, "/checkpoint", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		location := rr.Header().Get("Location")
		req, _ = http.NewRequest(http.MethodPost, location, bytes.NewReader(body))
		rr = httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		// Проверка статуса ответа
		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		// Проверка, что файл чекпоинта был создан
		checkpointFile := s.engine.checkpointFile
		if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
			t.Fatalf("Checkpoint file was not created")
		}

		// Проверка содержимого файла чекпоинта
		data, err := os.ReadFile(checkpointFile)
		if err != nil {
			t.Fatalf("Failed to read checkpoint file: %v", err)
		}

		var result geojson.FeatureCollection
		err = json.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("Failed to decode checkpoint file: %v", err)
		}

		// Проверка корректности данных в файле чекпоинта
		if len(result.Features) != 1 || result.Features[0].ID != "1" {
			t.Errorf("Unexpected checkpoint data: got %+v", result)
		}
	}
}
