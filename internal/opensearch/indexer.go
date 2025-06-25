package opensearch

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/Supasiti/prac-go-data-pipeline/internal/models/document"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type Indexer struct {
	Client *opensearchapi.Client
	Index  string
	Buf    []*document.Document

	BufSize uint16
	Cursor  uint16
}

type BulkMetadata struct {
	Index string `json:"_index"`
	Id    string `json:"_id"`
}

type BulkAction struct {
	Update *BulkMetadata `json:"update"`
}

type UpsertDocument struct {
	doc         *interface{} `json:"doc"`
	DocAsUpsert bool         `json:"doc_as_upsert"`
}

func NewIndexer(client *opensearchapi.Client, index string, bufSize uint16) *Indexer {
	return &Indexer{
		Client:  client,
		Index:   index,
		Buf:     make([]*document.Document, bufSize),
		BufSize: bufSize,
		Cursor:  0,
	}
}

func (i *Indexer) StartIndexing(ch <-chan *document.Document) {

	for {
		doc, ok := <-ch
		if !ok {
			slog.Info("channel is closed")
			break
		}
		slog.Info("document received", slog.Any("doc", doc))

		i.Buf[i.Cursor] = doc
		i.Cursor++

		if i.Cursor == i.BufSize {
			slog.Info("reach buffer capacity")
			i.toBulkRequest(i.Buf)
		}
	}
}

func (i *Indexer) toBulkRequest(docs []*document.Document) (io.Reader, error) {
	blkStr := ""

	for _, doc := range docs {
		docStr, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("error json marshalling to bulk request: %v", err)
		}

		blkStr += fmt.Sprintf("{ \"update\": { \"_index\": \"%s\", \"_id\": \"%s\" } }\n", i.Index, doc.Id)
		blkStr += fmt.Sprintf("{ \"doc\" : %s, \"doc_as_upsert\": true }\n", docStr)

	}
	fmt.Print(blkStr)
	return nil, nil

}
