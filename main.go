package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ricardonunez-io/lumberjack/internal/aggregator"
	"github.com/ricardonunez-io/lumberjack/internal/analyzer"
	"github.com/ricardonunez-io/lumberjack/internal/ingestor"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
	slackpkg "github.com/ricardonunez-io/lumberjack/internal/slack"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Info().Msg("Starting Lumberjack")

	ddApiKey := os.Getenv("DD_API_KEY")
	ddAppKey := os.Getenv("DD_APPLICATION_KEY")
	if ddApiKey == "" || ddAppKey == "" {
		log.Fatal().Msg("DD_API_KEY and DD_APPLICATION_KEY are required")
	}

	anthropicKey := os.Getenv("ANTHROPIC_API_KEY")
	if anthropicKey == "" {
		log.Fatal().Msg("ANTHROPIC_API_KEY is required")
	}

	slackConfig := slackpkg.Config{
		BotToken:  os.Getenv("SLACK_BOT_TOKEN"),
		ChannelID: os.Getenv("SLACK_CHANNEL_ID"),
	}
	if slackConfig.BotToken == "" || slackConfig.ChannelID == "" {
		log.Fatal().Msg("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required")
	}

	analyzerConfig := analyzer.DefaultConfig(anthropicKey)

	logSeverity := os.Getenv("LOG_SEVERITY")
	if !aggregator.ValidLogSeverities.Includes(logSeverity) {
		log.Warn().Str("value", logSeverity).Msg("Invalid LOG_SEVERITY, defaulting to MEDIUM")
		logSeverity = "MEDIUM"
	}

	query := os.Getenv("DD_QUERY")
	if query == "" {
		log.Info().Msg("No DD_QUERY set, defaulting to empty query (all logs)")
		query = "*"
	}

	timeIntervalKey := os.Getenv("TIME_INTERVAL")
	if !ingestor.ValidTimeIntervals.Includes(timeIntervalKey) {
		log.Warn().Str("value", timeIntervalKey).Msg("Invalid TIME_INTERVAL, defaulting to FIFTEEN_MINUTES")
		timeIntervalKey = "FIFTEEN_MINUTES"
	}
	timeInterval := ingestor.TimeIntervalToDurationMapping[timeIntervalKey]

	historicalTimeIntervalKey := os.Getenv("HISTORICAL_TIME_INTERVAL")
	if !ingestor.ValidTimeIntervals.Includes(historicalTimeIntervalKey) {
		log.Warn().Str("value", historicalTimeIntervalKey).Msg("Invalid HISTORICAL_TIME_INTERVAL, defaulting to ONE_DAY")
		historicalTimeIntervalKey = "ONE_DAY"
	}

	if ingestor.TimeIntervalToDurationMapping[historicalTimeIntervalKey] <= timeInterval {
		log.Warn().Msg("HISTORICAL_TIME_INTERVAL must be greater than TIME_INTERVAL, defaulting to ONE_DAY")
		historicalTimeIntervalKey = "ONE_DAY"
	}

	log.Info().
		Str("timeInterval", timeIntervalKey).
		Str("historicalTimeInterval", historicalTimeIntervalKey).
		Str("logSeverity", logSeverity).
		Str("query", query).
		Msg("Configuration loaded")

	ddClient := ingestor.InitializeDataDog()
	schemaCache := schema.NewCache(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	aggCfg := aggregator.AggregationConfig{
		Client:                    ddClient,
		TimeInterval:              timeInterval,
		Query:                     query,
		LogSeverity:               logSeverity,
		TimeIntervalKey:           timeIntervalKey,
		HistoricalTimeIntervalKey: historicalTimeIntervalKey,
		SchemaCache:               schemaCache,
	}

	resultChan := aggregator.RunPeriodicAggregation(ctx, aggCfg)

	for result := range resultChan {
		if err := processResult(ctx, result, slackConfig, analyzerConfig); err != nil {
			log.Err(err).Msg("Error processing aggregation result")
		}
	}

	log.Info().Msg("Lumberjack stopped")
}

func processResult(ctx context.Context, result aggregator.AggregationResult, slackCfg slackpkg.Config, analyzerCfg analyzer.Config) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregation result: %w", err)
	}

	analysis, err := analyzer.Analyze(ctx, string(data), analyzerCfg)
	if err != nil {
		return fmt.Errorf("analyzer error: %w", err)
	}

	if !analysis.SendSummary {
		log.Info().
			Int("signalStrength", analysis.SignalStrength).
			Str("severity", analysis.Severity).
			Msg("Analysis below alert threshold, skipping Slack notification")
		return nil
	}

	log.Info().
		Int("signalStrength", analysis.SignalStrength).
		Str("severity", analysis.Severity).
		Msg("Sending analysis to Slack")

	if err := slackpkg.SendMessage(*analysis, slackCfg); err != nil {
		return fmt.Errorf("slack error: %w", err)
	}

	return nil
}
