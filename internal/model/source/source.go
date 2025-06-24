package source

type Source struct {
	Prefix      string
	Postfix     string
	Firstname   string
	Lastname    string
	Middlename  string
	Gender      string
	DateOfBirth string
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
		s.Firstname = firstname
	}
}

func WithLastname(lastname string) func(*Source) {
	return func(s *Source) {
		s.Lastname = lastname
	}
}

func WithMiddlename(name string) func(*Source) {
	return func(s *Source) {
		s.Middlename = name
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
