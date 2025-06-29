package transformer

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
)

func sourceToDocument(src *Source) *opensearch.Document {
	return &opensearch.Document{
		Id:        src.Id,
		FirstName: src.FirstName,
		LastName:  src.LastName,
	}
}

type Transformer struct{}

func NewTransformer() *Transformer {
	return &Transformer{}
}

func (w *Transformer) ScanFile(file *os.File, outCh chan<- *opensearch.Document) {
	defer close(outCh)

	slog.Info("starting scanning")
	count := 0

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		var src Source
		if err := json.Unmarshal(line, &src); err != nil {
			// deal with error later
			slog.Warn("error unmarshalling a row")
			continue
		}
		outCh <- sourceToDocument(&src)
		count++
	}

	if err := scanner.Err(); err != nil {
		slog.Error("error reading file", slog.Any("error", err))
	}

	slog.Info("finished scanning file", slog.Int("rows", count))
}
