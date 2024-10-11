package json

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

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

func ToString(value interface{}) (string, bool) {
	v, ok := value.(string)
	return v, ok
}

func IsBase64Encoded(data string) bool {
	_, err := base64.StdEncoding.DecodeString(data)
	return err == nil
}

func ToRFC3339ToTimestampNano(value interface{}) (int64, bool) {
	s, ok := ToString(value)
	if !ok {
		return 0, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, false
	}
	return t.UnixNano(), true
}

func IsRFC3339(data string) bool {
	_, err := time.Parse(time.RFC3339, data)
	return err == nil
}

func ToByteString(value interface{}) ([]byte, bool) {
	s, ok := ToString(value)
	if !ok {
		return nil, false
	}
	if !IsBase64Encoded(s) {
		return nil, false
	}
	return []byte(s), true
}
