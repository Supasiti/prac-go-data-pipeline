package opensearch

import (
	"crypto/tls"
	"net/http"

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
		},
	}

	return opensearchapi.NewClient(c)
}
