package aggregator

import (
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/ricardonunez-io/lumberjack/internal/ingestor"
	"github.com/rs/zerolog/log"
)

type AggregationResult struct {
	Comparisons    map[string]map[string]float64
	CurrentLogs    Aggregates
	HistoricalLogs HistoricalAggregates
}

func RunPeriodicAggregation(
	client *datadog.APIClient,
	timeInterval time.Duration,
	query string,
	logSeverity string,
	timeIntervalKey, historicalTimeIntervalKey string,
) <-chan AggregationResult {
	log.Info().Msg("Starting periodic aggregation")
	resultChan := make(chan AggregationResult)

	go func() {
		runAggregation(client, query, logSeverity, timeIntervalKey, historicalTimeIntervalKey, timeInterval, resultChan)
		ticker := time.NewTicker(timeInterval)
		defer ticker.Stop()

		for range ticker.C {
			runAggregation(client, query, logSeverity, timeIntervalKey, historicalTimeIntervalKey, timeInterval, resultChan)
		}
	}()

	return resultChan
}

func runAggregation(
	client *datadog.APIClient,
	query, logSeverity, timeIntervalKey, historicalTimeIntervalKey string,
	timeInterval time.Duration,
	resultChan chan<- AggregationResult,
) {
	log.Info().Msg("Running aggregation")

	currentLogs, err := ingestor.GetIngestorFromTimeInterval(timeIntervalKey, query, client)
	if err != nil {
		log.Err(err).Msg("Failed to ingest logs for current interval")
		return
	}

	historicalLogs, err := ingestor.GetIngestorFromTimeInterval(historicalTimeIntervalKey, query, client)
	if err != nil {
		log.Err(err).Msg("Failed to ingest logs for historical interval")
		return
	}

	currentAggregates := Aggregate(currentLogs, logSeverity)
	historicalAggregates := AggregateHistoricalData(historicalLogs, timeInterval, logSeverity)

	comparisons := make(map[string]map[string]float64)
	for _, aggregateType := range []string{STATUS_NAME, HOST_NAME, SERVICE_NAME} {
		currentInsights := ExtractInsights(currentAggregates, aggregateType)
		historicalInsights := ExtractInsights(ConvertToAggregates(historicalAggregates), aggregateType)
		comparison := CompareWithHistorical(currentInsights, historicalInsights)
		comparisons[aggregateType] = comparison
	}

	resultChan <- AggregationResult{
		Comparisons:    comparisons,
		CurrentLogs:    currentAggregates,
		HistoricalLogs: historicalAggregates,
	}

	log.Info().Msg("Aggregation completed successfully")
}

func ConvertToAggregates(historicalAggs HistoricalAggregates) Aggregates {
	return Aggregates{
		Status:  convertToAggregateType(historicalAggs.Status),
		Host:    convertToAggregateType(historicalAggs.Host),
		Service: convertToAggregateType(historicalAggs.Service),
	}
}

func convertToAggregateType(historicalAggs HistoricalAggregateType) AggregateType {
	logs := make(map[string][]datadogV2.Log)
	counts := make(map[string]int)

	for _, interval := range historicalAggs.Intervals {
		for message, count := range interval.Logs {
			logs[message] = append(logs[message], datadogV2.Log{})
			counts[message] += count
		}
	}

	return AggregateType{
		Name: historicalAggs.Name,
		AggregateData: AggregateData{
			Logs:   logs,
			Counts: counts,
		},
	}
}
