package util

import "github.com/paulmach/orb/geojson"

// SpatialFeature оборачивает *geojson.Feature
type SpatialFeature struct {
	Feature *geojson.Feature
}

// Bounds возвращает минимальные и максимальные координаты (bounding box)
func (sf *SpatialFeature) Bounds() (min, max [2]float64) {
	bbox := sf.Feature.Geometry.Bound()
	min = [2]float64{bbox.Min[0], bbox.Min[1]}
	max = [2]float64{bbox.Max[0], bbox.Max[1]}
	return min, max
}
