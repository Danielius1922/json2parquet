package parquet

import (
	"errors"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	tfJson "github.com/thermofisher/json"
	"github.com/thermofisher/log"
)

type Writer struct {
	writer *file.Writer

	batch     []map[string]interface{}
	batchSize int
}

func NewWriter(path string, batchSize int, sc *schema.Schema) (*Writer, error) {
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{
		writer:    file.NewParquetWriter(out, sc.Root()),
		batchSize: batchSize,
	}, nil
}

func (w *Writer) Close() {
	if len(w.batch) > 0 {
		if err := w.WriteBatch(); err != nil {
			log.Logger().Errorf("failed to write last batch datar: %v", err)
		}
	}
	if err := w.writer.FlushWithFooter(); err != nil {
		log.Logger().Errorf("failed to flush parquet writer: %v", err)
	}
	if err := w.writer.Close(); err != nil {
		log.Logger().Errorf("failed to close parquet writer: %v", err)
	}
}

func (w *Writer) writeBoolColumn(cw *file.BooleanColumnChunkWriter) (int64, error) {
	var values []bool
	defLevels := make([]int16, len(w.batch))
	var repLevels []int16
	for i, record := range w.batch {
		name := cw.Descr().SchemaNode().Name()
		if value, ok := record[name]; ok {
			v, ok := tfJson.ToBool(value)
			if !ok {
				return 0, fmt.Errorf("cannot convert(%T) to bool", value)
			}
			defLevels[i] = 1
			values = append(values, v)
		}
	}
	return cw.WriteBatch(values, defLevels, repLevels)
}

func (w *Writer) writeInt64Column(cw *file.Int64ColumnChunkWriter) (int64, error) {
	var values []int64
	defLevels := make([]int16, len(w.batch))
	var repLevels []int16
	for i, record := range w.batch {
		name := cw.Descr().SchemaNode().Name()
		if value, ok := record[name]; ok {
			v, ok := tfJson.ToInt64(value)
			if !ok {
				return 0, fmt.Errorf("cannot convert(%T) to int64", value)
			}
			defLevels[i] = 1
			values = append(values, v)
		}
	}
	return cw.WriteBatch(values, defLevels, repLevels)
}

func (w *Writer) writeFloat64Column(cw *file.Float64ColumnChunkWriter) (int64, error) {
	var values []float64
	defLevels := make([]int16, len(w.batch))
	var repLevels []int16
	for i, record := range w.batch {
		name := cw.Descr().SchemaNode().Name()
		if value, ok := record[name]; ok {
			v, ok := tfJson.ToFloat64(value)
			if !ok {
				return 0, fmt.Errorf("cannot convert(%T) to float64", value)
			}
			defLevels[i] = 1
			values = append(values, v)
		}
	}
	return cw.WriteBatch(values, defLevels, repLevels)
}

func (w *Writer) writeColumn(cw file.ColumnChunkWriter) (int64, error) {
	if bwr, ok := cw.(*file.BooleanColumnChunkWriter); ok {
		return w.writeBoolColumn(bwr)
	}
	if int64wr, ok := cw.(*file.Int64ColumnChunkWriter); ok {
		return w.writeInt64Column(int64wr)
	}
	if f64wr, ok := cw.(*file.Float64ColumnChunkWriter); ok {
		return w.writeFloat64Column(f64wr)
	}
	return 0, errors.New("invalid write")
}

func (w *Writer) Write(data map[string]interface{}) error {
	if len(w.batch) >= w.batchSize {
		if err := w.WriteBatch(); err != nil {
			return err
		}
	}
	w.batch = append(w.batch, data)
	return nil
}

func (w *Writer) WriteBatch() error {
	rgw := w.writer.AppendRowGroup()
	for range rgw.NumColumns() {
		cw, err := rgw.NextColumn()
		if err != nil {
			rgw.Close()
			return err
		}
		_, err = w.writeColumn(cw)
		if err != nil {
			cw.Close()
			rgw.Close()
			return err
		}
		if err := cw.Close(); err != nil {
			rgw.Close()
			return err
		}
	}
	w.batch = nil
	return rgw.Close()
}
