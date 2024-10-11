package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	pqSchema "github.com/apache/arrow-go/v18/parquet/schema"
	tfJson "github.com/thermofisher/json2parquet/json"
	tfLog "github.com/thermofisher/json2parquet/log"
	"github.com/thermofisher/json2parquet/parquet"
	"go.uber.org/zap"
)

func main() {
	var verbose bool
	var inferOnly bool
	var batchSize uint
	var output string

	flag.BoolVar(&verbose, "v", false, "Enable verbose mode")
	flag.BoolVar(&inferOnly, "i", false, "Infer the parquet schema from json data and exit")
	flag.UintVar(&batchSize, "b", 1000, "Batch size of the stored JSON data before it is send to parquet writer to process. Also the parquet row group size.")
	flag.StringVar(&output, "o", "out.parquet", "Specify the output file (default is out.parquet)")

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("A simple conversion tool that reads a ndjson file and output a parquet file.")
		fmt.Println()
		fmt.Printf("Usage: %v [options] <filename>\n", os.Args[0])
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
		fmt.Println("\nPositional arguments:")
		fmt.Println("  filename    Path to the input JSON file")
		os.Exit(1)
	}

	if batchSize == 0 {
		log.Fatalln("batch size cannot be zero")
	}

	var logger *zap.Logger
	var err error
	if verbose {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		log.Fatalf("failed create logger: %v", err)
	}
	tfLog.SetLogger(logger.Sugar())

	filename := flag.Arg(0)

	ctx, cancel := context.WithCancel(context.Background())

	// Run a goroutine that listens for signals and cancels the context when received
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		tfLog.Logger().Debugf("received signal %v\n", sig)
		cancel()
	}()

	reader, err := tfJson.NewFromFile(filename)
	if err != nil {
		log.Fatalf("failed to open file(%v): %v", filename, err)
	}
	reader.SetSkipNestedObjects(true) // TODO: remove when nested objects are supported

	fmt.Printf("Infering parquet schema\n\n")
	sb := parquet.NewSchemaBuilder()
	err = reader.Read(ctx, func(data tfJson.NDJsonRecord) {
		errU := sb.UpdateSchema(data)
		if errU != nil {
			tfLog.Logger().Errorf("failed to create schema: %v", errU)
			cancel()
		}
	})
	if err != nil {
		log.Fatalf("failed to infer parquet schema from JSON data: %v", err)
	}

	sc := sb.Schema()
	sc2, err := sc.Schema()
	if err != nil {
		log.Fatalf("failed to build parquet schema: %v", err)
	}
	pqSchema.PrintSchema(sc2.Root(), os.Stdout, 2)
	fmt.Println()

	if inferOnly {
		os.Exit(0)
	}

	fmt.Printf("Reading JSON data and writing data to %v\n\n", output)
	reader, err = tfJson.NewFromFile(filename)
	if err != nil {
		log.Fatalf("failed to open file(%v): %v", filename, err)
	}
	reader.SetSkipNestedObjects(true)

	wr, err := parquet.NewWriter(output, batchSize, sc)
	if err != nil {
		log.Fatalf("failed to create parquet file write: %v", err)
	}
	defer wr.Close()

	err = reader.Read(ctx, func(data tfJson.NDJsonRecord) {
		errW := wr.Write(data)
		if errW != nil {
			tfLog.Logger().Errorf("failed to write data: %v", errW)
			cancel()
		}
	})
	if err != nil {
		wr.Close()
		log.Fatalf("failed to infer parquet schema from JSON data: %v", err) //nolint:gocritic
	}
}
