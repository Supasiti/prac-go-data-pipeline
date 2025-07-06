package main

import (
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/dataflow"
)

const (
	srcPath = "./tests/data/source_1000.txt"
	errPath = "./tests/data/errors.txt"

	batchSize  = 20
	indexName  = "person"
	queueSize  = 1000
	numIndexer = 2
)

func checkErr(err error, msg string) {
	if err != nil {
		slog.Error(msg, slog.Any("err", err))
		os.Exit(1)
	}
}

func main() {
	// init config
	slog.Info("getting config...")
	cfg, err := config.NewConfig()
	checkErr(err, "error getting config from .env file")

	// init dataflow runner
	slog.Info("initialising dataflow runner...")
	runner, err := dataflow.NewRunner(dataflow.RunnerConfig{
		QueueSize:        queueSize,
		OpenSearch:       cfg.OpenSearch,
		NumIndexer:       numIndexer,
		IndexerBatchSize: batchSize,
		IndexName:        indexName,
	})
	checkErr(err, "error initialising dataflow runner")

	// open file for error reports
	slog.Info("opening file for error reports ...", slog.String("file", srcPath))
	errFile, err := os.OpenFile(errPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	checkErr(err, "error open file for error report")
	defer errFile.Close()

	// open file for reading
	slog.Info("opening source file ...", slog.String("file", srcPath))
	srcFile, err := os.Open(srcPath)
	checkErr(err, "error opening file")
	defer srcFile.Close()

	runner.Run(srcFile, errFile)
}
