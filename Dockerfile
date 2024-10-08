FROM golang:1.22-alpine AS build
RUN apk add --no-cache build-base
WORKDIR $GOPATH/src/github.com/thermofisher/json2parquet
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Ensure static linking by disabling CGO
RUN CGO_ENABLED=0 go build -o /usr/local/bin/json2parquet .

FROM scratch AS service
COPY --from=build /usr/local/bin/json2parquet /json2parquet
ENTRYPOINT ["/json2parquet"]
