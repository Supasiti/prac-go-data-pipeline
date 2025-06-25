package main

import (
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/models/document"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
)

const (
	filePath = "./tests/data/source_10.txt"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("error getting config from .env file")
		return
	}

	ch := make(chan *document.Document, 1000)
	tfm := transformer.NewTransformer()

	client, err := opensearch.NewClient(*cfg.OpenSearch)
	if err != nil {
		slog.Error("error initiate opensearch client", slog.Any("error", err))
		return
	}

	indexer := opensearch.NewIndexer(client, "person", 10)

	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("error opening file", slog.Any("error", err))
	}
	defer file.Close()

	tfm.ScanFile(file, ch)
	indexer.StartIndexing(ch)
}
