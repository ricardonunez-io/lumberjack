package analyzer

type Config struct {
	APIKey string
	Model  string
}

func DefaultConfig(apiKey string) Config {
	return Config{
		APIKey: apiKey,
		Model:  "claude-opus-4-6",
	}
}
