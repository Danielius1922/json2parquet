# Implementation

This implementation uses <github.com/apache/arrow-go> to write a parquet file.

The implementation runs through the JSON line by line twice. The first is used to infer the parquet schema - the number of columns in the data, their names and types.

Supported JSON types and their deduced parquet type:

boolean                           |   boolean
integer                           |   int64
floating point number             |   float64
base64 encoded string             |   byte array
string                            |   byte array (with string logical type)
array of booleans                 |   list of repeated booleans
array of integers                 |   list of repeated int64s
array of floating point integers  |   list of float64s
array of base64 encoded strings   |   list of byte arrays
array of strings                  |   list of byte arrays (with string logical type)

Nested objects are not supported.

## Build and run

```sh
go build .
./json2parquet
```

Build and run docker image:

```sh
docker build --target service -t json2parquet .

docker run -d --name json2parquet json2parquet ${ARGS}
```

## Run linters

golangci-lint run  --timeout 5m

## Run tests

go test -race -v  -cover -coverpkg=./... -covermode=atomic ./...

## TODO

- datetime
