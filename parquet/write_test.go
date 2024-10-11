package parquet_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	pqSchema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
	tfJson "github.com/thermofisher/json2parquet/json"
	"github.com/thermofisher/json2parquet/parquet"
)

func testConvertJSON2Parquet(t *testing.T, json string, expSchema string, expRows int64) {
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

	sc := sb.Schema()
	pqSc, err := sc.Schema()
	require.NoError(t, err)
	pqSchema.PrintSchema(pqSc.Root(), os.Stdout, 2)
	wr, err := parquet.NewWriter("test.parquet", 1000, sc)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove("test.parquet")
	}()

	input.WriteString(json)
	reader, err = tfJson.New(&input)
	require.NoError(t, err)
	err = reader.Read(context.Background(), func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		require.NoError(t, errW)
	})
	require.NoError(t, err)
	wr.Close()

	f, err := os.Open("test.parquet")
	require.NoError(t, err)
	defer f.Close()
	parquetReader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	metadata := parquetReader.MetaData()
	if expSchema != "" {
		require.Equal(t, expSchema, metadata.Schema.String())
	}
	require.Equal(t, expRows, metadata.NumRows)
}

func TestWriteBooleanParquet(t *testing.T) {
	jsonStr := `{"available": true}` + "\n" +
		`{"available":false}` + "\n" +
		`{"available":true,"optional":false}`

	schema := `required group field_id=-1 schema {
  required boolean field_id=-1 available;
  optional boolean field_id=-1 optional;
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteInt64Parquet(t *testing.T) {
	jsonStr := `{"available": 13}` + "\n" +
		`{"available":37}` + "\n" +
		`{"available":42,"optional":1}` + "\n" +
		`{"available":0}`
	schema := `required group field_id=-1 schema {
  required int64 field_id=-1 available;
  optional int64 field_id=-1 optional;
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 4)
}

func TestWriteFloat64Parquet(t *testing.T) {
	jsonStr := `{"available": 4.2}` + "\n" +
		`{"available":13}` + "\n" +
		`{"available":42,"optional":1.0}` + "\n" +
		`{"available":0}`
	schema := `required group field_id=-1 schema {
  required double field_id=-1 available;
  optional double field_id=-1 optional;
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 4)
}

func TestWriteStringParquet(t *testing.T) {
	jsonStr := `{"text": "AQID"}` + "\n" +
		`{"text": "I have the high ground", "winner": "Dan"}` + "\n" +
		`{"text": "Noooooooooooooooo!", "winner": ""}`
	schema := `required group field_id=-1 schema {
  required byte_array field_id=-1 text (String);
  optional byte_array field_id=-1 winner (String);
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteByteArrayAndStringParquet(t *testing.T) {
	jsonStr := `{"base64": "SGVsbG8sIFdvcmxkIQ=="}` + "\n" +
		`{"base64": "T3BlbkFJ", "person": "Daniel"}` + "\n" +
		`{"base64": "anNvbiB0byBwYXJxdWV0!", "person": "Eva"}`
	schema := `required group field_id=-1 schema {
  required byte_array field_id=-1 base64 (String);
  optional byte_array field_id=-1 person (String);
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteRFC3339StringParquet(t *testing.T) {
	jsonStr := `{"rfc3339": "2006-01-02T15:04:05Z"}` + "\n" +
		`{"rfc3339": "2014-04-15T18:00:15-07:00"}`
	schema := `required group field_id=-1 schema {
  required int64 field_id=-1 rfc3339 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 2)
}

func TestWriteBoolArraysParquet(t *testing.T) {
	jsonStr := `{"available": [true,false],"array":[true]}` + "\n" +
		`{"available":[false,true,false],"optional":[]}` + "\n" +
		`{"available":[true],"optional":[false]}`

	schema := `required group field_id=-1 schema {
  optional group field_id=-1 array (List) {
    repeated boolean field_id=-1 element;
  }
  required group field_id=-1 available (List) {
    repeated boolean field_id=-1 element;
  }
  optional group field_id=-1 optional (List) {
    repeated boolean field_id=-1 element;
  }
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteInt64ArraysParquet(t *testing.T) {
	jsonStr := `{"available": [1,3,3,37], "array":[42]}` + "\n" +
		`{"available":[1,9,22], "optional":[]}` + "\n" +
		`{"available":[7], "optional":[9]}`
	schema := `required group field_id=-1 schema {
  optional group field_id=-1 array (List) {
    repeated int64 field_id=-1 element;
  }
  required group field_id=-1 available (List) {
    repeated int64 field_id=-1 element;
  }
  optional group field_id=-1 optional (List) {
    repeated int64 field_id=-1 element;
  }
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteFloat64ArraysParquet(t *testing.T) {
	jsonStr := `{"available": [1.3,3.37],"array":[4.2], "optional":[]}` + "\n" +
		`{"available":[1.9,22]}` + "\n" +
		`{"available":[7.0],"optional":[9.1]}` + "\n" +
		`{"available":[],"array":[123.4]}`
	schema := `required group field_id=-1 schema {
  optional group field_id=-1 array (List) {
    repeated double field_id=-1 element;
  }
  required group field_id=-1 available (List) {
    repeated double field_id=-1 element;
  }
  optional group field_id=-1 optional (List) {
    repeated double field_id=-1 element;
  }
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 4)
}

func TestWriteStringArraysParquet(t *testing.T) {
	jsonStr := `{"available": ["Hello, Dave ", "Open", "the pod bay doors, HAL"],"array":["Open the doors!"]}` + "\n" +
		`{"available":["Look Dave, I can see", "you're really upset about this"],"optional":[]}` + "\n" +
		`{"available":["Good day, gentlemen"],"optional":["I know I've made some very poor decisions recently"]}`
	schema := `required group field_id=-1 schema {
  optional group field_id=-1 array (List) {
    repeated byte_array field_id=-1 element (String);
  }
  required group field_id=-1 available (List) {
    repeated byte_array field_id=-1 element (String);
  }
  optional group field_id=-1 optional (List) {
    repeated byte_array field_id=-1 element (String);
  }
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}

func TestWriteRFC3339StringArrayParquet(t *testing.T) {
	jsonStr := `{"rfc3339": ["2014-04-15T18:00:15-07:00", "2014-04-16T18:00:15-07:00"]}` + "\n" +
		`{"rfc3339": []}` + "\n" +
		`{"rfc3339": ["2014-04-17T18:00:15-07:00"]}`

	schema := `required group field_id=-1 schema {
  required group field_id=-1 rfc3339 (List) {
    repeated int64 field_id=-1 element (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
  }
}
`
	testConvertJSON2Parquet(t, jsonStr, schema, 3)
}
