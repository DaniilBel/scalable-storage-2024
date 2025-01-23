package util

import "github.com/paulmach/orb/geojson"

type Command struct {
	Action      string `json:"action"`
	Rect        [2][2]float64
	Feature     *geojson.Feature `json:"feature"`
	Response    chan<- any
	Transaction Transaction
}
