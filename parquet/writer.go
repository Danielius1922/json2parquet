package parquet

import (
	"errors"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	tfJson "github.com/thermofisher/json2parquet/json"
	"github.com/thermofisher/json2parquet/log"
)

type Writer struct {
	writer *file.Writer

	batch     []map[string]interface{}
	batchSize uint
}

func NewWriter(path string, batchSize uint, sc *schema.Schema) (*Writer, error) {
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
			log.Logger().Errorf("failed to write last batch data: %v", err)
		}
	}
	if err := w.writer.FlushWithFooter(); err != nil {
		log.Logger().Errorf("failed to flush parquet writer: %v", err)
	}
	if err := w.writer.Close(); err != nil {
		log.Logger().Errorf("failed to close parquet writer: %v", err)
	}
}

// Define a type for conversion functions
type ValueConverter[T any] func(interface{}) (T, bool)

func writeSingleColumn[T any](w *Writer, cw file.ColumnChunkWriter, convert ValueConverter[T]) ([]T, []int16, []int16, error) {
	var values []T
	defLevels := make([]int16, len(w.batch))
	var repLevels []int16
	name := cw.Descr().SchemaNode().Name()
	for i, row := range w.batch {
		if value, ok := row[name]; ok {
			v, ok := convert(value)
			if !ok {
				return nil, nil, nil, fmt.Errorf("cannot convert(%T) to %T", value, *new(T))
			}
			defLevels[i] = cw.LevelInfo().DefLevel
			values = append(values, v)
		}
	}
	return values, defLevels, repLevels, nil
}

func (w *Writer) writeBoolColumn(cw *file.BooleanColumnChunkWriter) error {
	values, defLevels, repLevels, err := writeSingleColumn[bool](w, cw, tfJson.ToBool)
	if err != nil {
		return err
	}
	_, err = cw.WriteBatch(values, defLevels, repLevels)
	return err
}

func writeArrayColumn[T any](row map[string]interface{}, cw file.ColumnChunkWriter, convert ValueConverter[T]) ([]T, []int16, []int16, error) {
	parent := cw.Descr().SchemaNode().Parent()
	if parent.RepetitionType() == parquet.Repetitions.Repeated {
		return nil, nil, nil, errors.New("nested elements are not supported")
	}
	rep := cw.Descr().SchemaNode().RepetitionType()
	repLevel := cw.LevelInfo().RepLevel
	name := parent.Name()
	value, ok := row[name]
	if !ok {
		if rep == parquet.Repetitions.Required {
			return nil, nil, nil, fmt.Errorf("missing required column(%v)", name)
		}
		return nil, []int16{repLevel - 1}, []int16{0}, nil
	}
	values, ok := value.([]interface{})
	if !ok {
		return nil, nil, nil, fmt.Errorf("unexpected type(%T)", value)
	}
	if len(values) == 0 {
		// missing at current level
		curReplevel := repLevel
		if rep == parquet.Repetitions.Required {
			curReplevel = repLevel - 1
		}
		return nil, []int16{curReplevel}, []int16{0}, nil
	}

	array := make([]T, len(values))
	defLevels := make([]int16, len(values))
	repLevels := make([]int16, len(values))
	for i, v := range values {
		array[i], ok = convert(v)
		if !ok {
			return nil, nil, nil, fmt.Errorf("cannot convert(%T) to %T", v, array[0])
		}
		// non-NULL so take the max defLevel
		defLevels[i] = cw.LevelInfo().DefLevel
		// same array, so take the max repLevel
		repLevels[i] = repLevel
		if i == 0 {
			// new entry start
			repLevels[i] = 0
		}
	}
	return array, defLevels, repLevels, nil
}

func (w *Writer) writeBoolArrayColumn(cw *file.BooleanColumnChunkWriter) error {
	for _, row := range w.batch {
		boolArray, defLevels, repLevels, err := writeArrayColumn[bool](row, cw, tfJson.ToBool)
		if err != nil {
			return err
		}
		_, err = cw.WriteBatch(boolArray, defLevels, repLevels)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeInt64Column(cw *file.Int64ColumnChunkWriter) error {
	values, defLevels, repLevels, err := writeSingleColumn[int64](w, cw, tfJson.ToInt64)
	if err != nil {
		return err
	}
	_, err = cw.WriteBatch(values, defLevels, repLevels)
	return err
}

func (w *Writer) writeInt64ArrayColumn(cw *file.Int64ColumnChunkWriter) error {
	for _, row := range w.batch {
		int64Array, defLevels, repLevels, err := writeArrayColumn[int64](row, cw, tfJson.ToInt64)
		if err != nil {
			return err
		}
		_, err = cw.WriteBatch(int64Array, defLevels, repLevels)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeFloat64Column(cw *file.Float64ColumnChunkWriter) error {
	values, defLevels, repLevels, err := writeSingleColumn[float64](w, cw, tfJson.ToFloat64)
	if err != nil {
		return err
	}
	_, err = cw.WriteBatch(values, defLevels, repLevels)
	return err
}

func (w *Writer) writeFloat64ArrayColumn(cw *file.Float64ColumnChunkWriter) error {
	for _, row := range w.batch {
		f64Array, defLevels, repLevels, err := writeArrayColumn[float64](row, cw, tfJson.ToFloat64)
		if err != nil {
			return err
		}
		_, err = cw.WriteBatch(f64Array, defLevels, repLevels)
		if err != nil {
			return err
		}
	}
	return nil
}

func toByteArray(v interface{}) (parquet.ByteArray, bool) {
	s, ok := tfJson.ToString(v)
	if !ok {
		return nil, ok
	}
	return parquet.ByteArray(s), true
}

func (w *Writer) writeByteArrayColumn(cw *file.ByteArrayColumnChunkWriter) error {
	values, defLevels, repLevels, err := writeSingleColumn[parquet.ByteArray](w, cw, toByteArray)
	if err != nil {
		return err
	}
	_, err = cw.WriteBatch(values, defLevels, repLevels)
	return err
}

func (w *Writer) writeByteArrayArrayColumn(cw *file.ByteArrayColumnChunkWriter) error {
	for _, row := range w.batch {
		baArray, defLevels, repLevels, err := writeArrayColumn[parquet.ByteArray](row, cw, toByteArray)
		if err != nil {
			return err
		}
		_, err = cw.WriteBatch(baArray, defLevels, repLevels)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeColumn(cw file.ColumnChunkWriter) error {
	isArray := cw.Descr().SchemaNode().RepetitionType() == parquet.Repetitions.Repeated
	if bwr, ok := cw.(*file.BooleanColumnChunkWriter); ok {
		if isArray {
			return w.writeBoolArrayColumn(bwr)
		}
		return w.writeBoolColumn(bwr)
	}
	if int64wr, ok := cw.(*file.Int64ColumnChunkWriter); ok {
		if isArray {
			return w.writeInt64ArrayColumn(int64wr)
		}
		return w.writeInt64Column(int64wr)
	}
	if f64wr, ok := cw.(*file.Float64ColumnChunkWriter); ok {
		if isArray {
			return w.writeFloat64ArrayColumn(f64wr)
		}
		return w.writeFloat64Column(f64wr)
	}
	if bawr, ok := cw.(*file.ByteArrayColumnChunkWriter); ok {
		if isArray {
			return w.writeByteArrayArrayColumn(bawr)
		}
		return w.writeByteArrayColumn(bawr)
	}
	return errors.New("invalid write")
}

func (w *Writer) Write(data map[string]interface{}) error {
	if uint(len(w.batch)) >= w.batchSize {
		if err := w.WriteBatch(); err != nil {
			return err
		}
	}
	w.batch = append(w.batch, data)
	return nil
}

func (w *Writer) WriteBatch() error {
	rgw := w.writer.AppendRowGroup()
	log.Logger().Debugf("writing %v rows of json data", len(w.batch))
	for range rgw.NumColumns() {
		cw, err := rgw.NextColumn()
		if err != nil {
			rgw.Close()
			return err
		}
		err = w.writeColumn(cw)
		if err != nil {
			rgw.Close()
			return err
		}
	}
	log.Logger().Debugf("written row group size %v bytes", rgw.TotalBytesWritten())
	w.batch = nil
	return rgw.Close()
}
