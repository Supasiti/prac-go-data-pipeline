package transformer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
)

type Transformer struct {
	outCh chan *opensearch.Document
	count int
}

func NewTransformer(queueSize int) *Transformer {
	return &Transformer{
		outCh: make(chan *opensearch.Document, queueSize),
		count: 0,
	}
}

func (t *Transformer) ScanFile(ctx context.Context, file *os.File, errCh chan<- error) {
	defer t.cleanup()
	t.count = 0

	slog.Info("starting scanning")
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			errCh <- fmt.Errorf("stop scanning %w", ctx.Err())
			return
		default:
			line := scanner.Bytes()
			var src Source
			if err := json.Unmarshal(line, &src); err != nil {
				errCh <- fmt.Errorf("error unmarshalling a row: %w", err)
				continue
			}

			// can do validation here

			t.outCh <- sourceToDocument(&src)
			t.count++
		}
	}

	if err := scanner.Err(); err != nil {
		errCh <- fmt.Errorf("error scanning file: %w", err)
	}
}

func (t *Transformer) Documents() <-chan *opensearch.Document {
	return t.outCh
}

func (t *Transformer) cleanup() {
	close(t.outCh)
	slog.Info("finished scanning file", slog.Int("rows", t.count))
}
