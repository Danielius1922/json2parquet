package json

import (
	"bufio"
	"context"
	"io"
	"os"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	tfLog "github.com/thermofisher/json2parquet/log"
)

type Reader struct {
	scanner *bufio.Scanner

	skipNestedObjects bool
}

type (
	NDJsonRecord = map[string]interface{}
	onRead       = func(NDJsonRecord)
)

func New(r io.Reader) (*Reader, error) {
	scanner := bufio.NewScanner(r)
	return &Reader{
		scanner: scanner,
	}, nil
}

func NewFromFile(file string) (*Reader, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return New(f)
}

func (r *Reader) SetSkipNestedObjects(skip bool) {
	r.skipNestedObjects = skip
}

func isNested(value interface{}) bool {
	if value == nil {
		return false
	}
	if reflect.TypeOf(value).Kind() == reflect.Map {
		return true
	}
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		array := value.([]interface{})
		if len(array) > 0 {
			if reflect.TypeOf(array[0]).Kind() == reflect.Slice ||
				reflect.TypeOf(array[0]).Kind() == reflect.Map {
				return true
			}
		}
	}
	return false
}

func (r *Reader) Read(ctx context.Context, onRead onRead) error {
	iter := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !r.scanner.Scan() {
			break
		}
		row := r.scanner.Bytes()

		var jsonRecord NDJsonRecord
		err := iter.Unmarshal(row, &jsonRecord)
		if err != nil {
			tfLog.Logger().Errorf("error parsing data: %v", err)
			continue
		}
		if r.skipNestedObjects {
			for k, v := range jsonRecord {
				if v == nil || isNested(v) {
					delete(jsonRecord, k)
					continue
				}
			}
			if len(jsonRecord) == 0 {
				continue
			}
		}
		onRead(jsonRecord)
	}
	return nil
}
