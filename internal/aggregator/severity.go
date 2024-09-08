package aggregator

import "strings"

type severity string
type severityOptions []severity

func (option severity) Match(input string) bool {
	return strings.ToUpper(input) == string(option)
}

func (options severityOptions) Includes(input string) bool {
	for _, i := range options {
		if i.Match(input) {
			return true
		}
	}
	return false
}

const (
	ALL    severity = "ALL"
	MEDIUM severity = "MEDIUM"
	SEVERE severity = "SEVERE"
)

var ValidLogSeverities severityOptions = severityOptions{
	"ALL",    // will aggregate and summarize all logs, including DEBUG and INFO logs
	"MEDIUM", // will aggregate and summarize WARNING logs and ERROR logs, but not DEBUG or INFO logs
	"SEVERE", // will aggregate and summarize only ERROR logs
}

func ShouldSkipLog(logStatus string, logSeverity string) bool {
	status := strings.ToLower(logStatus)
	if MEDIUM.Match(logSeverity) && (status == "info" || status == "debug") {
		return true
	}
	if SEVERE.Match(logSeverity) && (status == "info" || status == "warning" || status == "debug") {
		return true
	}
	return false
}
