package transformer

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/models/document"
	"github.com/Supasiti/prac-go-data-pipeline/internal/models/source"
)

func sourceToDocument(src *source.Source) *document.Document {
	return &document.Document{
		Id:        src.Id,
		FirstName: src.FirstName,
		LastName:  src.LastName,
	}
}

type Transformer struct{}

func NewTransformer() *Transformer {
	return &Transformer{}
}

func (w *Transformer) ScanFile(file *os.File, outCh chan<- *document.Document) {
	defer close(outCh)

	var src source.Source
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &src); err != nil {
			slog.Warn("error unmarshalling a row")
			continue
		}
		outCh <- sourceToDocument(&src)
	}

	if err := scanner.Err(); err != nil {
		slog.Error("error reading file", slog.Any("error", err))
	}
}
