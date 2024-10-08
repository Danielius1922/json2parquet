package parquet_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thermofisher/json"
	tfJson "github.com/thermofisher/json"
	"github.com/thermofisher/parquet"
	"github.com/thermofisher/schema"
)

func TestWriteBooleanParquet(t *testing.T) {
	reader, err := tfJson.NewFromFile("../data/booleans.ndjson")
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	wr, err := parquet.NewWriter("boolean.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	reader, err = tfJson.NewFromFile("../data/booleans.ndjson")
	require.NoError(t, err)
	reader.Read(func(data json.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
}

func TestWriteInt64Parquet(t *testing.T) {
	reader, err := tfJson.NewFromFile("../data/integers.ndjson")
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	wr, err := parquet.NewWriter("int64.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	reader, err = tfJson.NewFromFile("../data/integers.ndjson")
	require.NoError(t, err)
	reader.Read(func(data json.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
}

func TestWriteMixedParquet(t *testing.T) {
	reader, err := tfJson.NewFromFile("../data/mixed.ndjson")
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	wr, err := parquet.NewWriter("mixed64.parquet", 100, sc)
	require.NoError(t, err)
	defer wr.Close()

	reader, err = tfJson.NewFromFile("../data/mixed.ndjson")
	require.NoError(t, err)
	reader.Read(func(data json.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})

}
