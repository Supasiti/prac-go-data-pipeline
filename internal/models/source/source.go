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

func New(opts ...func(*Source)) *Source {
	p := &Source{}
	for _, o := range opts {
		o(p)
	}
	return p
}

func WithPrefix(prefix string) func(*Source) {
	return func(s *Source) {
		s.Prefix = prefix
	}
}

func WithPostFix(postfix string) func(*Source) {
	return func(s *Source) {
		s.Postfix = postfix
	}
}

func WithFirstname(firstname string) func(*Source) {
	return func(s *Source) {
		s.FirstName = firstname
	}
}

func WithLastname(lastname string) func(*Source) {
	return func(s *Source) {
		s.LastName = lastname
	}
}

func WithMiddlename(name string) func(*Source) {
	return func(s *Source) {
		s.MiddleName = name
	}
}

func WithGender(gender string) func(*Source) {
	return func(s *Source) {
		s.Gender = gender
	}
}

func WithDateOfBirth(dob string) func(*Source) {
	return func(s *Source) {
		s.DateOfBirth = dob
	}
}
