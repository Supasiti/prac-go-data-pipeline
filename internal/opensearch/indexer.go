package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type IndexerConfig struct {
	Client    *opensearchapi.Client
	IndexName string
	BufSize   uint16
}

type Indexer struct {
	client *opensearchapi.Client
	index  string

	buf     []*Document
	bufSize uint16
	cursor  uint16 // the next slot in the buffer for document
	count   uint64
}

func NewIndexer(cfg IndexerConfig) *Indexer {
	return &Indexer{
		client: cfg.Client,
		index:  cfg.IndexName,

		buf:     make([]*Document, cfg.BufSize),
		bufSize: cfg.BufSize,
		cursor:  0,
		count:   0,
	}
}

func (i *Indexer) Start(inCh <-chan *Document, errCh chan<- error) {
	slog.Info("starting indexing")
	defer i.onExit(errCh)

	for {
		doc, ok := <-inCh
		if !ok {
			slog.Info("channel is closed")
			return
		}

		if i.cursor >= i.bufSize {
			i.cursor = 0 // otherwise get out of bound error
		}

		i.buf[i.cursor] = doc
		i.cursor++

		if i.cursor != i.bufSize {
			continue
		}

		if err := i.indexDocuments(); err != nil {
			errCh <- err
		}
	}
}

func (i *Indexer) onExit(errCh chan<- error) {
	// clean up the rest in buffer
	if i.cursor < i.bufSize {
		if err := i.indexDocuments(); err != nil {
			errCh <- err
		}
	}
	slog.Info("finished indexing", slog.Uint64("count", i.count))
}

func (i *Indexer) indexDocuments() error {
	docs := i.buf[0:i.cursor]
	bulkBody, err := i.bulkBodyReader(docs)
	if err != nil {
		return fmt.Errorf("error creating bulk req for %v: %w", Ids(docs), err)
	}

	resp, err := i.client.Bulk(context.Background(), opensearchapi.BulkReq{Body: bulkBody})
	if err != nil {
		return fmt.Errorf("error bulk insert for %v: %w", Ids(docs), err)
	}

	// for partial errors
	if resp.Errors {
		ids := make([]string, len(docs))
		for _, item := range resp.Items {
			if item["update"].Error != nil {
				ids = append(ids, item["update"].ID)
				slog.Error("error bulk insert", slog.Any("item", item))
			} else {
				i.count += 1
			}
		}
		return fmt.Errorf("partial error bulk insert for %v", ids)
	}

	i.count += uint64(i.cursor)
	return nil
}

func (i *Indexer) bulkBodyReader(docs []*Document) (io.Reader, error) {
	b := &strings.Builder{}

	for _, doc := range docs {
		docStr, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("error json marshalling to bulk request: %w", err)
		}

		fmt.Fprintf(b, `{ "update": { "_index": "%s", "_id": "%s" } }`, i.index, doc.Id)
		b.WriteString("\n")
		fmt.Fprintf(b, `{ "doc" : %s, "doc_as_upsert": true }`, docStr)
		b.WriteString("\n")
	}

	return strings.NewReader(b.String()), nil
}
