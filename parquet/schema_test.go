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

func testBuildSchemaForJSON(t *testing.T, json string, expSchema string) {
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

	sc, err := sb.Schema().Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(sc.Root(), os.Stdout, 2)

	require.Equal(t, expSchema, sc.String())
}

func TestBooleanSchema(t *testing.T) {
	boolJSON := `{"required": true}` + "\n" + `{"required":false, "optional":true}`
	schema := `required group field_id=-1 schema {
  optional boolean field_id=-1 optional;
  required boolean field_id=-1 required;
}
`
	testBuildSchemaForJSON(t, boolJSON, schema)
}

func TestInt64Schema(t *testing.T) {
	intJSON := `{"battery":42}` + "\n" + `{"charged":0, "battery":100}`
	schema := `required group field_id=-1 schema {
  required int64 field_id=-1 battery;
  optional int64 field_id=-1 charged;
}
`
	testBuildSchemaForJSON(t, intJSON, schema)
}

func TestFloat64Schema(t *testing.T) {
	floatJSON := `{"temperature": 23}` + "\n" +
		`{"temperature": -5.3, "humility": 0.43}` + "\n" +
		`{"temperature": 37, "humility": 0.55}`
	schema := `required group field_id=-1 schema {
  optional double field_id=-1 humility;
  required double field_id=-1 temperature;
}
`
	testBuildSchemaForJSON(t, floatJSON, schema)
}

func TestStringSchema(t *testing.T) {
	stringJSON := `{"text": "AQID"}` + "\n" +
		`{"text": "I have the high ground", "winner": "Dan"}` + "\n" +
		`{"text": "Noooooooooooooooo!", "winner": ""}`
	schema := `required group field_id=-1 schema {
  required byte_array field_id=-1 text (String);
  optional byte_array field_id=-1 winner (String);
}
`
	testBuildSchemaForJSON(t, stringJSON, schema)
}

func TestByteArrayAndStringSchema(t *testing.T) {
	stringJSON := `{"base64": "SGVsbG8sIFdvcmxkIQ=="}` + "\n" +
		`{"base64": "T3BlbkFJ", "person": "Daniel"}` + "\n" +
		`{"base64": "anNvbiB0byBwYXJxdWV0!", "person": "Eva"}`
	schema := `required group field_id=-1 schema {
  required byte_array field_id=-1 base64 (String);
  optional byte_array field_id=-1 person (String);
}
`
	testBuildSchemaForJSON(t, stringJSON, schema)
}

func TestRFC3339StringSchema(t *testing.T) {
	stringJSON := `{"rfc3339": "2006-01-02T15:04:05Z", "toString": "2014-04-15T18:00:15+02:00", "toStringFromBase64": "2014-04-15T18:00:15+02:00"}` + "\n" +
		`{"rfc3339": "2014-04-15T18:00:15-07:00", "toString": "string", "toStringFromBase64": "SGVsbG8sIFdvcmxkIQ=="}`
	schema := `required group field_id=-1 schema {
  required int64 field_id=-1 rfc3339 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
  required byte_array field_id=-1 toString (String);
  required byte_array field_id=-1 toStringFromBase64 (String);
}
`
	testBuildSchemaForJSON(t, stringJSON, schema)

	stringJSON2 := `{"toString": "string", "toStringFromBase64": "SGVsbG8sIFdvcmxkIQ=="}` + "\n" +
		`{"optional": "2006-01-02T15:04:05Z", "toString": "2014-04-15T18:00:15+02:00", "toStringFromBase64": "2014-04-15T18:00:15+02:00"}`
	schema2 := `required group field_id=-1 schema {
  optional int64 field_id=-1 optional (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
  required byte_array field_id=-1 toString (String);
  required byte_array field_id=-1 toStringFromBase64 (String);
}
`
	testBuildSchemaForJSON(t, stringJSON2, schema2)
}

func TestBoolArraySchema(t *testing.T) {
	boolArrayJSON := `{"availability":[]}` + "\n" +
		`{"availability":[true, false, false], "presence": [false]}` + "\n" +
		`{"availability":[false], "presence": []}`
	schema := `required group field_id=-1 schema {
  required group field_id=-1 availability (List) {
    repeated boolean field_id=-1 element;
  }
  optional group field_id=-1 presence (List) {
    repeated boolean field_id=-1 element;
  }
}
`
	testBuildSchemaForJSON(t, boolArrayJSON, schema)
}

func TestIntAndFloatArraySchema(t *testing.T) {
	numericJSON := `{"float":[], "int":[42]}` + "\n" +
		`{"float":[32]}` + "\n" +
		`{"float":[13, 3.4, 37], "int":[]}`
	schema := `required group field_id=-1 schema {
  required group field_id=-1 float (List) {
    repeated double field_id=-1 element;
  }
  optional group field_id=-1 int (List) {
    repeated int64 field_id=-1 element;
  }
}
`
	testBuildSchemaForJSON(t, numericJSON, schema)
}

func TestStringArraySchema(t *testing.T) {
	stringJSON := `{"base64": ["SGVsbG8sIFdvcmxkIQ=="], "string":[]}` + "\n" +
		`{"string": [""]}` + "\n" +
		`{"base64": ["anNvbiB0byBwYXJxdWV0!"]}`
	schema := `required group field_id=-1 schema {
  optional group field_id=-1 base64 (List) {
    repeated byte_array field_id=-1 element (String);
  }
  optional group field_id=-1 string (List) {
    repeated byte_array field_id=-1 element;
  }
}
`
	testBuildSchemaForJSON(t, stringJSON, schema)
}

func TestRFC3339StringArraySchema(t *testing.T) {
	stringJSON := `{"rfc3339": []}` + "\n" +
		`{"rfc3339": ["2014-04-15T18:00:15-07:00"]}`
	schema := `required group field_id=-1 schema {
  required group field_id=-1 rfc3339 (List) {
    repeated int64 field_id=-1 element (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
  }
}
`
	testBuildSchemaForJSON(t, stringJSON, schema)

	stringJSON2 := `{"string":["string"]}` + "\n" +
		`{"optional": ["2014-04-15T18:00:15-07:00"], "string": ["2014-04-15T18:00:15-07:00"]}`
	schema2 := `required group field_id=-1 schema {
  optional group field_id=-1 optional (List) {
    repeated int64 field_id=-1 element (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
  }
  required group field_id=-1 string (List) {
    repeated byte_array field_id=-1 element (String);
  }
}
`
	testBuildSchemaForJSON(t, stringJSON2, schema2)
}
