package parquet_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	pqSchema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
	tfJson "github.com/thermofisher/json2parquet/json"
	"github.com/thermofisher/json2parquet/parquet"
)

func TestWriteBooleanParquet(t *testing.T) {
	jsonStr := `{"available": true}` + "\n" +
		`{"available":false}` + "\n" +
		`{"available":true,"optional":false}`
	var input bytes.Buffer
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("boolean.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}

func TestWriteInt64Parquet(t *testing.T) {
	jsonStr := `{"available": 13}` + "\n" +
		`{"available":37}` + "\n" +
		`{"available":42,"optional":1}`
	var input bytes.Buffer
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("int64.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}

func TestWriteBoolArraysParquet(t *testing.T) {
	var input bytes.Buffer
	jsonStr := `{"available": [true,false],"array":[true]}` + "\n" +
		`{"available":[false,true,false],"optional":[]}` + "\n" +
		`{"available":[true],"optional":[false]}`
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("boolArrays.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}

func TestWriteInt64ArraysParquet(t *testing.T) {
	var input bytes.Buffer
	jsonStr := `{"available": [1,3,3,37],"array":[42]}` + "\n" +
		`{"available":[1,9,22],"optional":[]}` + "\n" +
		`{"available":[7],"optional":[9]}`
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("int64Arrays.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}

func TestWriteFloat64ArraysParquet(t *testing.T) {
	var input bytes.Buffer
	jsonStr := `{"available": [1.3,3.37],"array":[4.2]}` + "\n" +
		`{"available":[1.9,22],"optional":[]}` + "\n" +
		`{"available":[7.0],"optional":[9.1]}`
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("float64Arrays.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}

func TestWriteStringArraysParquet(t *testing.T) {
	var input bytes.Buffer
	jsonStr := `{"available": ["Hello, Dave ", "Open", "the pod bay doors, HAL"],"array":["Open the doors!"]}` + "\n" +
		`{"available":["Look Dave, I can see", "you're really upset about this"],"optional":[]}` + "\n" +
		`{"available":["Good day, gentlemen"],"optional":["I know I've made some very poor decisions recently"]}`
	input.WriteString(jsonStr)
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := parquet.NewSchemaBuilder()
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		require.NoError(t, errU)
	})
	require.NoError(t, err)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("stringArrays.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	input.WriteString(jsonStr)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
}
