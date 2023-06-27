package services

type FormatType string

const (
	Json FormatType = "json"
)

type Stream struct {
	Topic  string
	Format FormatType
	Schema *Schema
}

type Configuration struct {
	InstanceId string
}

func LoadConfiguration(path string) (*Configuration, error) {
	return nil, nil
}
