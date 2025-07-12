package dataflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	"github.com/Supasiti/prac-go-data-pipeline/internal/dataflow"
	"github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	opensearchcontainer "github.com/testcontainers/testcontainers-go/modules/opensearch"
)

const (
	srcPath = "../../tests/data/source_1000.txt"

	queueSize        = 100
	numIndexer       = 2
	indexerBatchSize = uint16(20)
)

var indices = []string{"person"}

func TestRunner_Run(t *testing.T) {
	ctx := context.Background()

	ctr, err := opensearchcontainer.Run(ctx, "opensearchproject/opensearch:latest")
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Printf("failed to terminate container: %s", err)
		}
	}()
	require.NoError(t, err)

	addr, err := ctr.Address(ctx)
	require.NoError(t, err)

	cfg := config.OpenSearchConfig{Url: addr}
	client, err := opensearch.NewClient(cfg)
	require.NoErrorf(t, err, "error creating client")

	t.Run("Dataflow Run", func(t *testing.T) {

		runner, err := dataflow.NewRunner(dataflow.RunnerConfig{
			QueueSize:        queueSize,
			OpenSearch:       &cfg,
			NumIndexer:       numIndexer,
			IndexerBatchSize: indexerBatchSize,
			IndexName:        indices[0],
		})
		require.NoError(t, err)

		// set up source and error target
		errWriter := &strings.Builder{}
		srcFile, err := os.Open(srcPath)
		require.NoError(t, err)
		defer srcFile.Close()

		// -----
		// act
		runner.Run(srcFile, errWriter)
		_, err = client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{Indices: indices})
		require.NoError(t, err)

		// ------
		// assert

		// no error
		assert.Equal(t, errWriter.String(), "")

		// count
		countResp, err := client.Cat.Count(ctx, &opensearchapi.CatCountReq{
			Indices: indices,
		})
		assert.NoErrorf(t, err, "error getting count from opensearch")
		assert.Equal(t, 1000, countResp.Counts[0].Count)

		content := strings.NewReader(`{
    "query": {
        "multi_match": {
			"query": "Eleazar",
			"fields": ["firstName"]
        }
    }
}`)
		docResp, err := client.Search(ctx, &opensearchapi.SearchReq{
			Indices: indices,
			Body:    content,
		})
		require.NoErrorf(t, err, "error searching for document")
		assert.Equal(t, 1, docResp.Hits.Total.Value)

		docRaw, err := docResp.Hits.Hits[0].Source.MarshalJSON()
		require.NoError(t, err)

		var doc opensearch.Document
		err = json.Unmarshal(docRaw, &doc)
		assert.NoErrorf(t, err, "error unmarshalling source")
		assert.Equal(t, "Eleazar", doc.FirstName)
	})
}
