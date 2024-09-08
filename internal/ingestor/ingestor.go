package ingestor

import (
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/rs/zerolog/log"
)

type IngestorFunc func(query string) ([]datadogV2.Log, error)

func GetIngestorFromTimeInterval(key, query string, client *datadog.APIClient) ([]datadogV2.Log, error) {
	switch key {
	case "ONE_MINUTES":
		return LastMinute(client, query)
	case "FIVE_MINUTES":
		return LastFiveMinutes(client, query)
	case "TEN_MINUTES":
		return LastTenMinutes(client, query)
	case "FIFTEEN_MINUTES":
		return LastFifteenMinutes(client, query)
	case "THIRTY_MINUTES":
		return LastThirtyMinutes(client, query)
	case "ONE_HOUR":
		return LastHour(client, query)
	case "SIX_HOURS":
		return LastSixHours(client, query)
	case "TWELVE_HOURS":
		return LastTwelveHours(client, query)
	case "ONE_DAY":
		return LastDay(client, query)
	case "ONE_WEEK":
		return LastWeek(client, query)
	case "ONE_MONTH":
		return LastMonth(client, query)
	default:
		return LastFiveMinutes(client, query)
	}
}

func IngestWithinTimeRange(tr TimeRange, client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	log.Info().
		Str("query", query).
		Str("start", tr.Start().String()).
		Str("end", tr.End().String()).
		Msg("Ingesting logs within time range")
	return IngestFromDataDog(tr.Start(), tr.End(), client, query)
}

func IngestFromDataDog(from, to time.Time, client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	defaultRespValue := []datadogV2.Log{}

	api := datadogV2.NewLogsApi(client)
	params := datadogV2.NewListLogsGetOptionalParameters()

	sort := datadogV2.LOGSSORT_TIMESTAMP_DESCENDING
	params.Sort = &sort

	params.FilterFrom = &from
	params.FilterTo = &to
	params.FilterQuery = &query

	resp, _, err := api.ListLogsGet(Ctx, *params)

	if err != nil {
		log.Err(err).
			Msg("Error when calling `LogsApi.ListLogsGet`")
		return defaultRespValue, err
	}

	log.Info().
		Int("logCount", len(resp.Data)).
		Msg("Successfully retrieved logs from DataDog")

	return resp.Data, err
}

func LastMinute(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(ONE_MINUTE), client, query)
}

func LastFiveMinutes(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(FIVE_MINUTES), client, query)
}

func LastTenMinutes(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(TEN_MINUTES), client, query)
}

func LastFifteenMinutes(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(FIFTEEN_MINUTES), client, query)
}

func LastThirtyMinutes(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(THIRTY_MINUTES), client, query)
}

func LastHour(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(ONE_HOUR), client, query)
}

func LastSixHours(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(SIX_HOURS), client, query)
}

func LastTwelveHours(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(TWELVE_HOURS), client, query)
}

func LastDay(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(ONE_DAY), client, query)
}

func LastWeek(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(ONE_WEEK), client, query)
}

func LastMonth(client *datadog.APIClient, query string) ([]datadogV2.Log, error) {
	return IngestWithinTimeRange(NewDurationRange(ONE_MONTH), client, query)
}
