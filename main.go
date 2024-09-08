package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ricardonunez-io/lumberjack/internal/aggregator"
	"github.com/ricardonunez-io/lumberjack/internal/ingestor"
	"github.com/ricardonunez-io/lumberjack/internal/laminar"
	"github.com/ricardonunez-io/lumberjack/internal/slack"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	log.Info().Msg("Starting Lumberjack application")
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	ddApiKey := os.Getenv("DD_API_KEY")
	ddAppKey := os.Getenv("DD_APPLICATION_KEY")
	if ddApiKey == "" || ddAppKey == "" {
		log.Fatal().
			Str("DD_API_KEY", ddApiKey).
			Str("DD_API_KEY", ddAppKey).
			Msg("Please pass in both a DD_API_KEY and DD_APPLICATION_KEY")
	}

	ddApiClient := ingestor.InitializeDataDog()

	slackConfig := slack.Config{
		BotToken:  os.Getenv("SLACK_BOT_TOKEN"),
		ChannelID: os.Getenv("SLACK_CHANNEL_ID"),
	}
	if slackConfig.BotToken == "" || slackConfig.ChannelID == "" {
		log.Fatal().
			Msg("Please pass in both a SLACK_BOT_TOKEN and SLACK_CHANNEL_ID")
	}
	log.Info().Msg("Slack configuration loaded successfully")

	laminarConfig := laminar.Config{
		LaminarKey:   os.Getenv("LAMINAR_API_KEY"),
		AnthropicKey: os.Getenv("ANTHROPIC_API_KEY"),
	}
	if laminarConfig.LaminarKey == "" || laminarConfig.AnthropicKey == "" {
		log.Fatal().
			Msg("Please pass in both a LAMINAR_API_KEY and ANTHROPIC_API_KEY")
	}
	log.Info().Msg("Laminar configuration loaded successfully")

	logSeverity := os.Getenv("LOG_SEVERITY")
	if aggregator.ValidLogSeverities.Includes(logSeverity) {
		log.Warn().
			Msg(fmt.Sprintf("Invalid `LOG_SEVERITY` level selected (`%v`), defaulting to `MEDIUM`", logSeverity))
		logSeverity = "MEDIUM"
	}

	query := os.Getenv("DD_QUERY")
	if aggregator.ValidLogSeverities.Includes(query) {
		log.Warn().
			Msg("No `DD_QUERY` provided, defaulting to no filters")
		query = "MEDIUM"
	}
	log.Info().Str("query", query).Msg("DataDog query set")

	timeIntervalKey := os.Getenv("TIME_INTERVAL")
	if !ingestor.ValidTimeIntervals.Includes(timeIntervalKey) {
		log.Warn().
			Msg(fmt.Sprintf("Invalid `TIME_INTERVAL` level selected (`%v`), defaulting to `FIFTEEN_MINUTES`", timeIntervalKey))
		timeIntervalKey = "FIFTEEN_MINUTES"
	}
	timeInterval := ingestor.TimeIntervalToDurationMapping[timeIntervalKey]

	historicalTimeIntervalKey := os.Getenv("HISTORICAL_TIME_INTERVAL")
	if !ingestor.ValidTimeIntervals.Includes(historicalTimeIntervalKey) &&
		ingestor.TimeIntervalToDurationMapping[historicalTimeIntervalKey] > timeInterval {
		log.Warn().Msg(fmt.Sprintf("Invalid `HISTORICAL_TIME_INTERVAL` level selected (`%v`), defaulting to `FIFTEEN_MINUTES`", historicalTimeIntervalKey))
		historicalTimeIntervalKey = "ONE_MONTH"
	}

	log.Info().
		Str("timeInterval", timeIntervalKey).
		Str("historicalTimeInterval", historicalTimeIntervalKey).
		Msg("Time intervals set")

	resultChan := aggregator.RunPeriodicAggregation(
		ddApiClient,
		timeInterval,
		query,
		logSeverity,
		timeIntervalKey,
		historicalTimeIntervalKey,
	)

	for result := range resultChan {
		err := processAggregationResult(result, slackConfig, laminarConfig)
		if err != nil {
			log.Err(err).
				Msg("Error processing aggregation result")
		}
	}
}

func processAggregationResult(aggResult aggregator.AggregationResult, slackConfig slack.Config, laminarConfig laminar.Config) error {
	aggResultJson, err := json.Marshal(aggResult)
	if err != nil {
		return fmt.Errorf("error marshaling aggregation result into JSON: %v", err.Error())
	}

	aggResultStr := string(aggResultJson)

	res, err := laminar.Run(aggResultStr, laminarConfig)
	if res == nil {
		err = fmt.Errorf("invalid response received from Laminar pipeline")
	}
	if err != nil {
		return fmt.Errorf("error when running Laminar pipeline: %v", err.Error())
	}

	if !res.SendSummary {
		return nil
	}
	log.Info().Msg("Skipping summary send based on Laminar response")

	err = slack.SendMessage(*res, slackConfig)
	if err != nil {
		return fmt.Errorf("error posting message to Slack: %v", err.Error())
	}
	log.Info().Msg("Summary sent to Slack successfully")

	return nil
}
