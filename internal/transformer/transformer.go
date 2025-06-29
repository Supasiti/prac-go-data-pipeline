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
	errCh chan error
	count int
}

func NewTransformer(queueSize int) *Transformer {
	return &Transformer{
		outCh: make(chan *opensearch.Document, queueSize),
		errCh: make(chan error, queueSize),
		count: 0,
	}
}

func (t *Transformer) ScanFile(ctx context.Context, file *os.File) {
	defer t.cleanup()
	t.count = 0

	slog.Info("starting scanning")
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			t.errCh <- fmt.Errorf("cancel scanning %v", ctx.Err())
			return
		default:
			line := scanner.Bytes()
			var src Source
			if err := json.Unmarshal(line, &src); err != nil {
				err = fmt.Errorf("error unmarshalling a row: %v", err)
				t.errCh <- err
				continue
			}

			// can do validation here

			t.outCh <- sourceToDocument(&src)
			t.count++
		}
	}

	if err := scanner.Err(); err != nil {
		err = fmt.Errorf("error reading file: %v", err)
		t.errCh <- err
	}
}

func (t *Transformer) Documents() <-chan *opensearch.Document {
	return t.outCh
}

func (t *Transformer) Err() <-chan error {
	return t.errCh
}

func (t *Transformer) cleanup() {
	close(t.outCh)
	close(t.errCh)
	slog.Info("finished scanning file", slog.Int("rows", t.count))
}
