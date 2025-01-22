package util

import "github.com/paulmach/orb/geojson"

type Transaction struct {
	Action   string `json:"action"` // insert, replace, delete
	Name     string `json:"name"`
	LSN      uint64 `json:"lsn"`
	Rect     [2][2]float64
	Feature  *geojson.Feature `json:"feature"`
	Response chan<- any
}
