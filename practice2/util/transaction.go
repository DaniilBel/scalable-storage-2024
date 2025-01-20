package util

import "github.com/paulmach/orb/geojson"

type Transaction struct {
	Action  string           `json:"action"` // insert, replace, delete
	Name    string           `json:"name"`
	LSN     uint64           `json:"lsn"`
	Feature *geojson.Feature `json:"feature"`
}
