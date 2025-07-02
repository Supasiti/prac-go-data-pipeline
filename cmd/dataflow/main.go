package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/errorreport"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
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
	start := time.Now()

	// init config
	slog.Info("getting config...")
	cfg, err := config.NewConfig()
	checkErr(err, "error getting config from .env file")

	ctx, cancel := context.WithCancel(context.Background())

	// init error report
	slog.Info("initialising error reporter...")
	errCh := make(chan error, queueSize)

	errFile, err := os.OpenFile(errPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	checkErr(err, "error open file for error report")
	defer errFile.Close()

	errReporter := errorreport.New(errCh, errFile)
	go errReporter.AcceptErrors(cancel)

	// init transformer
	slog.Info("initialising transformer ...")
	tfm := transformer.NewTransformer(queueSize)

	// init indexers
	slog.Info("initialising indexers ...")
	client, err := opensearch.NewClient(*cfg.OpenSearch)
	checkErr(err, "error initiate opensearch client")

	var wg sync.WaitGroup
	for range numIndexer {
		indexer := opensearch.NewIndexer(opensearch.IndexerConfig{
			Client:    client,
			IndexName: indexName,
			BufSize:   batchSize,
			InCh:      tfm.Documents(),
		})
		wg.Add(1)

		go func() {
			defer wg.Done()
			indexer.Start(errCh)
		}()
	}

	// opening source file
	slog.Info("opening source file ...", slog.String("file", srcPath))
	srcFile, err := os.Open(srcPath)
	checkErr(err, "error opening file")
	defer srcFile.Close()

	// start scanning
	go tfm.ScanFile(ctx, srcFile, errCh)

	wg.Wait()
	close(errCh)

	slog.Info("finished execution", slog.Duration("excution_time", time.Since(start)))
}
