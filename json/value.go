package json

import "encoding/json"

func ToBool(value interface{}) (bool, bool) {
	v, ok := value.(bool)
	return v, ok
}

func ToInt64(value interface{}) (int64, bool) {
	jsonv, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	v, err := jsonv.Int64()
	if err != nil {
		return 0, false
	}
	return v, true
}

func ToFloat64(value interface{}) (float64, bool) {
	jsonv, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	f, err := jsonv.Float64()
	if err != nil {
		return 0, false
	}
	return f, true
}
