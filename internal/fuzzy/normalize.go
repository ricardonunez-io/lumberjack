package fuzzy

import "regexp"

var patterns = []*regexp.Regexp{
	regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`),
	regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?\b`),
	regexp.MustCompile(`\b[0-9a-fA-F]{24,}\b`),
	regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?`),
	regexp.MustCompile(`/[\w./]+(:\d+)?`),
	regexp.MustCompile(`\b\d+(\.\d+)?\b`),
}

var placeholders = []string{
	"<UUID>",
	"<IP>",
	"<HEX>",
	"<TIMESTAMP>",
	"<PATH>",
	"<NUM>",
}

func Normalize(msg string) string {
	for i, p := range patterns {
		msg = p.ReplaceAllString(msg, placeholders[i])
	}
	return msg
}
