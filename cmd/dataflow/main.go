package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
)

const (
	filePath   = "./tests/data/source_1000.txt"
	batchSize  = 20
	indexName  = "person"
	queueSize  = 1000
	numIndexer = 2
)

func main() {
	start := time.Now()
	slog.Info("getting config...")
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("error getting config from .env file")
		return
	}

	slog.Info("initialising transformer and indexer...")
	tfm := transformer.NewTransformer(queueSize)

	client, err := opensearch.NewClient(*cfg.OpenSearch)
	if err != nil {
		slog.Error("error initiate opensearch client", slog.Any("error", err))
		return
	}

	var wg sync.WaitGroup
	for range numIndexer {
		indexer := opensearch.NewIndexer(opensearch.IndexerConfig{
			Client:    client,
			IndexName: indexName,
			BufSize:   batchSize,
		})
		wg.Add(1)

		go func() {
			defer wg.Done()
			indexer.Start(tfm.Documents())
		}()
	}

	slog.Info("opening file...", slog.String("file", filePath))
	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("error opening file", slog.Any("error", err))
		return
	}
	defer file.Close()

	go tfm.ScanFile(context.Background(), file)

	wg.Wait()
	slog.Info("finished execution", slog.Duration("excution_time", time.Since(start)))
}
