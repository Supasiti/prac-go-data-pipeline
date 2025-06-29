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

type Indexer struct {
	Client *opensearchapi.Client
	Index  string

	Buf     []*Document
	BufSize uint16
	Cursor  uint16
	count   uint64
}

func NewIndexer(client *opensearchapi.Client, index string, bufSize uint16) *Indexer {
	return &Indexer{
		Client:  client,
		Index:   index,
		Buf:     make([]*Document, bufSize),
		BufSize: bufSize,
		Cursor:  0,
		count:   0,
	}
}

func (i *Indexer) Start(ch <-chan *Document, done chan<- struct{}) {
	defer close(done)
	slog.Info("starting indexing")

	for {
		doc, ok := <-ch
		if !ok {
			slog.Info("channel is closed")
			if i.Cursor < i.BufSize {
				i.indexDocuments()
			}
			break
		}

		if i.Cursor >= i.BufSize {
			i.Cursor = 0 // otherwise get out of bound error
		}

		i.Buf[i.Cursor] = doc
		i.Cursor++

		if i.Cursor != i.BufSize {
			continue
		}

		i.indexDocuments()
		// ignore error for now
	}

	slog.Info("finished indexing", slog.Uint64("count", i.count))
}

func (i *Indexer) indexDocuments() error {
	docs := i.Buf[0:i.Cursor]
	bulkBody, err := i.bulkBodyReader(docs)
	if err != nil {
		return fmt.Errorf("error creating bulk req: %v", err)
	}

	resp, err := i.Client.Bulk(context.Background(), opensearchapi.BulkReq{Body: bulkBody})
	if err != nil {
		return fmt.Errorf("error bulk insert: %v", err)
	}
	if resp.Errors {
		for _, item := range resp.Items {
			slog.Error("error bulk insert", slog.Any("item", item))
		}
	}

	// deal with partial failure later
	i.count += uint64(i.Cursor)
	return nil
}

func (i *Indexer) bulkBodyReader(docs []*Document) (io.Reader, error) {
	blkStr := ""

	for _, doc := range docs {
		docStr, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("error json marshalling to bulk request: %v", err)
		}

		blkStr += fmt.Sprintf("{ \"update\": { \"_index\": \"%s\", \"_id\": \"%s\" } }\n", i.Index, doc.Id)
		blkStr += fmt.Sprintf("{ \"doc\" : %s, \"doc_as_upsert\": true }\n", docStr)

	}

	return strings.NewReader(blkStr), nil
}
