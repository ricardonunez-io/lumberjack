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
	STATUS_NAME    string = "Status"
	HOST_NAME      string = "Host"
	SERVICE_NAME   string = "Service"
	INFO_STATUS    string = "info"
	DEBUG_STATUS   string = "debug"
	ERROR_STATUS   string = "error"
	WARNING_STATUS string = "warning"

	ALL    severity = "ALL"
	MEDIUM severity = "MEDIUM"
	SEVERE severity = "SEVERE"
)

var ValidLogSeverities severityOptions = severityOptions{
	"ALL",    // will aggregate and summarize all logs, including DEBUG and INFO logs
	"MEDIUM", // will aggregate and summarize WARNING logs and ERROR logs, but not DEBUG or INFO logs
	"SEVERE", // will aggregate and summarize only ERROR logs
}
