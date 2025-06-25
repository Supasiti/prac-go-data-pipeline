package main

import (
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/models/document"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
)

const (
	filePath = "./tests/data/source_10.txt"
)

func main() {
	ch := make(chan *document.Document, 1000)
	tfm := transformer.NewTransformer()

	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("Error opening file", slog.Any("error", err))
	}
	defer file.Close()

	tfm.ScanFile(file, ch)

	for doc := range ch {
		slog.Info("Incoming docs", slog.Any("document", *doc))
	}
}
