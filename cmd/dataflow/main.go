package main

import (
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
)

const (
	filePath   = "./tests/data/source_1000000.txt"
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
	ch := make(chan *opensearch.Document, queueSize)
	tfm := transformer.NewTransformer()

	client, err := opensearch.NewClient(*cfg.OpenSearch)
	if err != nil {
		slog.Error("error initiate opensearch client", slog.Any("error", err))
		return
	}

	var wg sync.WaitGroup
	for range numIndexer {
		indexer := opensearch.NewIndexer(client, indexName, batchSize)
		wg.Add(1)

		go func() {
			defer wg.Done()
			indexer.Start(ch)
		}()
	}

	slog.Info("opening file...", slog.String("file", filePath))
	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("error opening file", slog.Any("error", err))
	}
	defer file.Close()

	go tfm.ScanFile(file, ch)

	wg.Wait()
	slog.Info("finished execution", slog.Duration("excution_time", time.Since(start)))
}
