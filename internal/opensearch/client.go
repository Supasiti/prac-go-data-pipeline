package opensearch

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/config"
	opensearchbase "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func NewClient(cfg config.OpenSearchConfig) (*opensearchapi.Client, error) {
	c := opensearchapi.Config{
		Client: opensearchbase.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Addresses: []string{cfg.Url},
			Username:  cfg.Username,
			Password:  cfg.Password,

			RetryOnStatus: []int{502, 503, 504, 429}, // Retry on these HTTP status codes
			RetryBackoff: func(i int) time.Duration {
				return time.Duration(i) * 500 * time.Millisecond // simple incremental backoff
			},
			MaxRetries: 5,
		},
	}

	return opensearchapi.NewClient(c)
}
