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

func testBuildSchemaForJSON(t *testing.T, json string) {
	var input bytes.Buffer
	input.WriteString(json)
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
}

func TestBooleanSchema(t *testing.T) {
	boolJSON := `{"required": true}` + "\n" + `{"required":false, "optional":true}`
	testBuildSchemaForJSON(t, boolJSON)
}

func TestInt64Schema(t *testing.T) {
	intJSON := `{"battery":42}` + "\n" + `{"charged":0, "battery":100}`
	testBuildSchemaForJSON(t, intJSON)
}

func TestFloat64Schema(t *testing.T) {
	floatJSON := `{"temperature": 23}` + "\n" +
		`{"temperature": -5.3, "humility": 0.43}` + "\n" +
		`{"temperature": 37, "humility": 0.55}`
	testBuildSchemaForJSON(t, floatJSON)
}

func TestStringSchema(t *testing.T) {
	stringJSON := `{"text": "AQID"}` + "\n" +
		`{"text": "I have the high ground", "winner": "Dan"}` + "\n" +
		`{"text": "Noooooooooooooooo!", "winner": ""}`
	testBuildSchemaForJSON(t, stringJSON)
}

func TestByteArrayAndStringSchema(t *testing.T) {
	stringJSON := `{"base64": "SGVsbG8sIFdvcmxkIQ=="}` + "\n" +
		`{"base64": "T3BlbkFJ", "person": "Daniel"}` + "\n" +
		`{"base64": "anNvbiB0byBwYXJxdWV0!", "person": "Eva"}`
	testBuildSchemaForJSON(t, stringJSON)
}

func TestBoolArraySchema(t *testing.T) {
	boolArrayJSON := `{"availability":[]}` + "\n" +
		`{"availability":[true, false, false], "presence": [false]}` + "\n" +
		`{"availability":[false], "presence": []}`
	testBuildSchemaForJSON(t, boolArrayJSON)
}

func TestIntAndFloatArraySchema(t *testing.T) {
	numericJSON := `{"float":[], "int":[42]}` + "\n" +
		`{"float":[32]}` + "\n" +
		`{"float":[13, 3.4, 37], "int":[]}`
	testBuildSchemaForJSON(t, numericJSON)
}

func TestStringArraySchema(t *testing.T) {
	stringJSON := `{"base64": ["SGVsbG8sIFdvcmxkIQ=="], "string":[]}` + "\n" +
		`{"string": [""]}` + "\n" +
		`{"base64": ["anNvbiB0byBwYXJxdWV0!"]}`
	testBuildSchemaForJSON(t, stringJSON)
}

// TODO time formated as datetime
