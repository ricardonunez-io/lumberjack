package aggregator

import (
	"context"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/ricardonunez-io/lumberjack/internal/ingestor"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
	"github.com/rs/zerolog/log"
)

type AggregationResult struct {
	Comparisons    map[string]map[string]float64 `json:"comparisons"`
	CurrentLogs    Aggregates                    `json:"currentLogs"`
	HistoricalLogs HistoricalAggregates          `json:"historicalLogs"`
	Schema         schema.Schema                 `json:"schema"`
}

type AggregationConfig struct {
	Client                    *datadog.APIClient
	TimeInterval              time.Duration
	Query                     string
	LogSeverity               string
	TimeIntervalKey           string
	HistoricalTimeIntervalKey string
	SchemaCache               *schema.Cache
}

func RunPeriodicAggregation(ctx context.Context, cfg AggregationConfig) <-chan AggregationResult {
	log.Info().Msg("Starting periodic aggregation")
	resultChan := make(chan AggregationResult)

	go func() {
		defer close(resultChan)

		runAggregation(cfg, resultChan)
		ticker := time.NewTicker(cfg.TimeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Stopping periodic aggregation")
				return
			case <-ticker.C:
				runAggregation(cfg, resultChan)
			}
		}
	}()

	return resultChan
}

func runAggregation(cfg AggregationConfig, resultChan chan<- AggregationResult) {
	log.Info().Msg("Running aggregation cycle")

	currentLogs, err := ingestor.GetIngestorFromTimeInterval(cfg.TimeIntervalKey, cfg.Query, cfg.Client)
	if err != nil {
		log.Err(err).Msg("Failed to ingest logs for current interval")
		return
	}

	historicalLogs, err := ingestor.GetIngestorFromTimeInterval(cfg.HistoricalTimeIntervalKey, cfg.Query, cfg.Client)
	if err != nil {
		log.Err(err).Msg("Failed to ingest logs for historical interval")
		return
	}

	allLogs := append(currentLogs, historicalLogs...)
	s := cfg.SchemaCache.Get(allLogs)

	log.Info().
		Int("schemaFields", len(s.Fields)).
		Int("currentLogs", len(currentLogs)).
		Int("historicalLogs", len(historicalLogs)).
		Msg("Schema resolved")

	currentAggregates := Aggregate(currentLogs, s, cfg.LogSeverity)
	historicalAggregates := AggregateHistorical(historicalLogs, s, cfg.TimeInterval, cfg.LogSeverity)

	comparisons := make(map[string]map[string]float64)
	historicalAsAggregates := HistoricalToAggregates(historicalAggregates)

	for _, f := range s.Fields {
		currentInsights := ExtractInsights(currentAggregates, f.Name)
		historicalInsights := ExtractInsights(historicalAsAggregates, f.Name)
		comparison := CompareInsights(currentInsights, historicalInsights)
		comparisons[f.Name] = comparison
	}

	resultChan <- AggregationResult{
		Comparisons:    comparisons,
		CurrentLogs:    currentAggregates,
		HistoricalLogs: historicalAggregates,
		Schema:         s,
	}

	log.Info().Msg("Aggregation cycle completed")
}
