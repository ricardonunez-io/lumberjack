package aggregator

import (
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/rs/zerolog/log"
)

type HistoricalAggregates struct {
	Status  HistoricalAggregateType
	Host    HistoricalAggregateType
	Service HistoricalAggregateType
}

type HistoricalAggregateType struct {
	Name string
	HistoricalAggregateData
}

type HistoricalAggregateData struct {
	Intervals []IntervalAggregateData
}

type IntervalAggregateData struct {
	Logs   map[string]int
	Counts int
}

func AggregateHistoricalData(responses []datadogV2.Log, interval time.Duration, logSeverity string) HistoricalAggregates {
	data := map[string]*HistoricalAggregateData{
		STATUS_NAME:  {Intervals: []IntervalAggregateData{}},
		HOST_NAME:    {Intervals: []IntervalAggregateData{}},
		SERVICE_NAME: {Intervals: []IntervalAggregateData{}},
	}

	if len(responses) == 0 {
		return HistoricalAggregates{}
	}

	earliestTime := time.Unix(0, (*responses[0].Attributes.Timestamp).UnixNano())
	latestTime := earliestTime
	for _, ddLog := range responses {
		if ddLog.Attributes.Timestamp == nil {
			continue
		}
		logTime := time.Unix(0, (*ddLog.Attributes.Timestamp).UnixNano())
		if logTime.Before(earliestTime) {
			earliestTime = logTime
		}
		if logTime.After(latestTime) {
			latestTime = logTime
		}
	}

	numIntervals := int(latestTime.Sub(earliestTime)/interval) + 1
	for _, d := range data {
		d.Intervals = make([]IntervalAggregateData, numIntervals)
		for i := range d.Intervals {
			d.Intervals[i] = IntervalAggregateData{Logs: make(map[string]int)}
		}
	}

	for _, ddLog := range responses {
		if ddLog.Attributes.Status == nil ||
			ddLog.Attributes.Host == nil ||
			ddLog.Attributes.Service == nil ||
			ddLog.Attributes.Timestamp == nil {
			log.Debug().Msg(fmt.Sprintf("Skipping over invalid log (%v)", ddLog.Id))
			continue
		}

		logStatus := strings.ToLower(*ddLog.Attributes.Status)
		if (MEDIUM.Match(logSeverity) && (logStatus == "info" || logStatus == "debug")) ||
			(SEVERE.Match(logSeverity) && (logStatus == "info" || logStatus == "warning" || logStatus == "debug")) {
			log.Debug().Msg(fmt.Sprintf("Skipping over log with non-matching status of %v (%v)", logStatus, ddLog.Id))
			continue
		}

		logTime := time.Unix(0, (*ddLog.Attributes.Timestamp).UnixNano())
		intervalIndex := int(logTime.Sub(earliestTime) / interval)

		aggregateHistoricalLog(data[STATUS_NAME], ddLog.Attributes.Status, ddLog.Attributes.Message, intervalIndex)
		aggregateHistoricalLog(data[HOST_NAME], ddLog.Attributes.Host, ddLog.Attributes.Message, intervalIndex)
		aggregateHistoricalLog(data[SERVICE_NAME], ddLog.Attributes.Service, ddLog.Attributes.Message, intervalIndex)
	}

	return HistoricalAggregates{
		Status:  HistoricalAggregateType{Name: STATUS_NAME, HistoricalAggregateData: *data[STATUS_NAME]},
		Host:    HistoricalAggregateType{Name: HOST_NAME, HistoricalAggregateData: *data[HOST_NAME]},
		Service: HistoricalAggregateType{Name: SERVICE_NAME, HistoricalAggregateData: *data[SERVICE_NAME]},
	}
}

func aggregateHistoricalLog(data *HistoricalAggregateData, key, message *string, intervalIndex int) {
	if key == nil || message == nil || intervalIndex < 0 || intervalIndex >= len(data.Intervals) {
		return
	}

	interval := &data.Intervals[intervalIndex]
	interval.Logs[*message]++
	interval.Counts++
}
