package util

type Transaction struct {
	Action  string      `json:"action"`
	Name    string      `json:"name"`
	LSN     uint64      `json:"lsn"`
	Feature interface{} `json:"feature"`
}
