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
	Buf    []*Document

	BufSize uint16
	Cursor  uint16
}

func NewIndexer(client *opensearchapi.Client, index string, bufSize uint16) *Indexer {
	return &Indexer{
		Client:  client,
		Index:   index,
		Buf:     make([]*Document, bufSize),
		BufSize: bufSize,
		Cursor:  0,
	}
}

func (i *Indexer) Start(ch <-chan *Document, done chan<- struct{}) {
	defer close(done)
	slog.Info("starting indexing")

	for {
		doc, ok := <-ch
		if !ok {
			slog.Info("channel is closed")
			break
		}

		i.indexDocument(doc)
	}
}

func (i *Indexer) indexDocument(doc *Document) {
	if i.Cursor >= i.BufSize {
		// otherwise get out of index error
		i.Cursor = 0
	}

	i.Buf[i.Cursor] = doc
	i.Cursor++

	if i.Cursor != i.BufSize {
		return
	}

	bulkBody, err := i.bulkBodyReader(i.Buf)
	if err != nil {
		// deal with error later
		return
	}

	_, err = i.Client.Bulk(context.Background(), opensearchapi.BulkReq{Body: bulkBody})
	if err != nil {
		// deal with error later
		slog.Error("error bulk insert", slog.Any("err", err))
		return
	}
	// deal with partial failure later
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
