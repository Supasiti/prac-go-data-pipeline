package transformer

import "github.com/Supasiti/prac-go-data-pipeline/internal/opensearch"

type Source struct {
	Id          string `json:"id"`
	Prefix      string `json:"prefix"`
	Postfix     string `json:"postfix"`
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	MiddleName  string `json:"middleName"`
	Gender      string `json:"gender"`
	DateOfBirth string `json:"dateOfBirth"`
}

func sourceToDocument(src *Source) *opensearch.Document {
	return &opensearch.Document{
		Id:        src.Id,
		FirstName: src.FirstName,
		LastName:  src.LastName,
	}
}
