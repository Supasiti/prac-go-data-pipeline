package dataflow

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/errorreport"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
)

type RunnerConfig struct {
	QueueSize  int
	OpenSearch *config.OpenSearchConfig

	// indexer
	NumIndexer       int
	IndexerBatchSize uint16
	IndexName        string
}

type Runner struct {
	queueSize   int
	errReporter *errorreport.ErrorReport
	indexers    []*opensearch.Indexer
}

func NewRunner(cfg RunnerConfig) (*Runner, error) {
	// init indexer
	slog.Info("initialising indexers ...")
	client, err := opensearch.NewClient(*cfg.OpenSearch)
	if err != nil {
		return nil, fmt.Errorf("error initiate opensearch client: %w", err)
	}

	indexers := make([]*opensearch.Indexer, cfg.NumIndexer)
	for i := range cfg.NumIndexer {
		indexers[i] = opensearch.NewIndexer(opensearch.IndexerConfig{
			Client:    client,
			IndexName: cfg.IndexName,
			BufSize:   cfg.IndexerBatchSize,
		})
	}

	return &Runner{
		queueSize:   cfg.QueueSize,
		errReporter: errorreport.New(),
		indexers:    indexers,
	}, nil
}

func (r *Runner) Run(srcFile io.Reader, errTarget io.Writer) {
	slog.Info("Running dataflow ...")
	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, r.queueSize)

	go r.errReporter.AcceptErrors(errCh, errTarget, cancel)

	tfm := transformer.NewTransformer(r.queueSize)

	var wg sync.WaitGroup
	for _, indexer := range r.indexers {
		wg.Add(1)

		go func() {
			defer wg.Done()
			indexer.Start(tfm.Documents(), errCh)
		}()
	}

	// start scanning
	go tfm.ScanFile(ctx, srcFile, errCh)

	wg.Wait()
	close(errCh)

	slog.Info("finished indexing", slog.Duration("excution_time", time.Since(start)))
}
