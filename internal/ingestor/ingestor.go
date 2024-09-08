package ingestor

import (
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/rs/zerolog/log"
)

func GetIngestorFromTimeInterval(key, query string, client *datadog.APIClient) ([]datadogV2.Log, error) {
	duration, ok := TimeIntervalToDurationMapping[key]
	if !ok {
		duration = FIVE_MINUTES
	}
	return IngestWithinTimeRange(NewDurationRange(duration), client, query)
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
	api := datadogV2.NewLogsApi(client)

	var allLogs []datadogV2.Log
	var cursor *string

	for {
		params := datadogV2.NewListLogsGetOptionalParameters()
		sort := datadogV2.LOGSSORT_TIMESTAMP_DESCENDING
		params.Sort = &sort
		params.FilterFrom = &from
		params.FilterTo = &to
		params.FilterQuery = &query

		if cursor != nil {
			params.PageCursor = cursor
		}

		resp, _, err := api.ListLogsGet(Ctx, *params)
		if err != nil {
			log.Err(err).Msg("Error when calling LogsApi.ListLogsGet")
			return allLogs, err
		}

		allLogs = append(allLogs, resp.Data...)

		if resp.Meta == nil || resp.Meta.Page == nil || resp.Meta.Page.After == nil {
			break
		}

		after := *resp.Meta.Page.After
		if after == "" {
			break
		}
		cursor = &after
	}

	log.Info().
		Int("logCount", len(allLogs)).
		Msg("Successfully retrieved logs from DataDog")

	return allLogs, nil
}
