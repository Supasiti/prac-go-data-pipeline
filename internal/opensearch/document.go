package opensearch

type Document struct {
	Id        string `json:"id"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func Ids(docs []*Document) []string {
	result := make([]string, len(docs))
	for i, d := range docs {
		result[i] = d.Id
	}
	return result
}
