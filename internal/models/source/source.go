package source

type Source struct {
	Prefix      string `json:"prefix"`
	Postfix     string `json:"postfix"`
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	MiddleName  string `json:"middleName"`
	Gender      string `json:"gender"`
	DateOfBirth string `json:"dateOfBirth"`
}
