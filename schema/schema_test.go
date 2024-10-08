package schema_test

import (
	"bytes"
	"os"
	"testing"

	pqSchema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
	"github.com/thermofisher/json"
	tfJson "github.com/thermofisher/json"
	"github.com/thermofisher/schema"
)

func TestBooleanSchemas(t *testing.T) {
	var input bytes.Buffer
	input.Write([]byte(`{"required": true}` + "\n" + `{"required": false, "optional": true}`))
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	var output bytes.Buffer
	pqSchema.PrintSchema(sc.Root(), &output, 2)

	expected :=
		`repeated group field_id=-1 schema {
  required boolean field_id=-1 required;
  optional boolean field_id=-1 optional;
}
`
	require.Equal(t, expected, output.String())
}

func TestInt64Schemas(t *testing.T) {
	var input bytes.Buffer
	input.Write([]byte(`{"battery": 42}` + "\n" + `{"charged": 0, "battery": 100}`))
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
}

func TestFloat64Schemas(t *testing.T) {
	var input bytes.Buffer
	input.Write([]byte(`{"temperature": 23}` + "\n" +
		`{"temperature": -5.3, "humility": 0.43}` + "\n" +
		`{"temperature": 37, "humility": 0.55}`))
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	reader.Read(func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	})

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
}

func TestMixedSchemas(t *testing.T) {
	var input bytes.Buffer
	input.Write([]byte(`{"required":true, "battery":42, "optional":false, "temperature": -5.3}` + "\n" +
		`{"charged":0, "temperature": 33, "battery":100, "required": false, "humility": 0.1}` + "\n" +
		`{"charged":99, "temperature": -71, "battery":1, "required": false}`))
	reader, err := tfJson.New(&input)
	require.NoError(t, err)
	require.NotNil(t, reader)

	sb := schema.NewSchemaBuilder()
	updateSchema := func(data json.NDJsonRecord) {
		err := sb.UpdateSchema(data)
		require.NoError(t, err)
	}
	reader.Read(updateSchema)

	sc, err := sb.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)
}

// TODO strings, string formated as datetime, arrays
