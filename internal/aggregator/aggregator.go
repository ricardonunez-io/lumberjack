package aggregator

import (
	"fmt"
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/rs/zerolog/log"
)

type Aggregates struct {
	Status  AggregateType
	Host    AggregateType
	Service AggregateType
}

type AggregateType struct {
	Name string
	AggregateData
}

type AggregateData struct {
	Logs   map[string][]datadogV2.Log
	Counts map[string]int
}

func Aggregate(responses []datadogV2.Log, logSeverity string) Aggregates {
	data := map[string]*AggregateData{
		STATUS_NAME:  {Logs: make(map[string][]datadogV2.Log), Counts: make(map[string]int)},
		HOST_NAME:    {Logs: make(map[string][]datadogV2.Log), Counts: make(map[string]int)},
		SERVICE_NAME: {Logs: make(map[string][]datadogV2.Log), Counts: make(map[string]int)},
	}

	for _, ddLog := range responses {
		if ddLog.Attributes.Status == nil ||
			ddLog.Attributes.Host == nil ||
			ddLog.Attributes.Service == nil {
			log.Debug().Msg(fmt.Sprintf("Skipping over invalid log (%v)", ddLog.Id))
			continue
		}

		logStatus := strings.ToLower(*ddLog.Attributes.Status)
		if (MEDIUM.Match(logSeverity) && (logStatus == "info" || logStatus == "debug")) ||
			(SEVERE.Match(logSeverity) && (logStatus == "info" || logStatus == "warning" || logStatus == "debug")) {
			log.Debug().Msg(fmt.Sprintf("Skipping over log with non-matching status of %v (%v)", logStatus, ddLog.Id))
			continue
		}

		aggregateLog(data[STATUS_NAME], ddLog.Attributes.Status, ddLog)
		aggregateLog(data[HOST_NAME], ddLog.Attributes.Host, ddLog)
		aggregateLog(data[SERVICE_NAME], ddLog.Attributes.Service, ddLog)
	}

	return Aggregates{
		Status:  AggregateType{Name: STATUS_NAME, AggregateData: *data[STATUS_NAME]},
		Host:    AggregateType{Name: HOST_NAME, AggregateData: *data[HOST_NAME]},
		Service: AggregateType{Name: SERVICE_NAME, AggregateData: *data[SERVICE_NAME]},
	}
}

func aggregateLog(data *AggregateData, key *string, log datadogV2.Log) {
	if key == nil {
		return
	}

	if _, exists := data.Logs[*key]; !exists {
		data.Logs[*key] = []datadogV2.Log{}
	}

	data.Logs[*key] = append(data.Logs[*key], log)
	data.Counts[*key]++
}
