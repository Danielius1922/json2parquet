package json

import (
	"bufio"
	"io"
	"os"

	jsoniter "github.com/json-iterator/go"
	tfLog "github.com/thermofisher/log"
)

type Reader struct {
	scanner *bufio.Scanner
}

type NDJsonRecord = map[string]interface{}
type onRead = func(NDJsonRecord)

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

func (r *Reader) Read(onRead onRead) {
	iter := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()

	for r.scanner.Scan() {
		row := r.scanner.Bytes()

		var json NDJsonRecord
		err := iter.Unmarshal(row, &json)
		if err != nil {
			tfLog.Logger().Errorf("error parsing data: %v", err)
			continue
		}
		onRead(json)
	}
}
