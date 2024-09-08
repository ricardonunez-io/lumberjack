package aggregator

import (
	"fmt"
	"math"
	"sort"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

type AggregateInsights struct {
	TotalCount        int
	UniqueKeys        int
	TopKeys           []KeyCount
	TopMessages       []MessageCount
	AverageLogsPerKey float64
}

type KeyCount struct {
	Key   string
	Count int
}

type MessageCount struct {
	Message string
	Count   int
}

func ExtractInsights(aggregates Aggregates, aggregateType string) AggregateInsights {
	var data AggregateData

	switch aggregateType {
	case STATUS_NAME:
		data = aggregates.Status.AggregateData
	case HOST_NAME:
		data = aggregates.Host.AggregateData
	case SERVICE_NAME:
		data = aggregates.Service.AggregateData
	default:
		return AggregateInsights{}
	}

	totalCount := 0
	for _, count := range data.Counts {
		totalCount += count
	}

	uniqueKeys := len(data.Counts)
	averageLogsPerKey := float64(totalCount) / float64(uniqueKeys)

	topKeys := getTopKeys(data.Counts, 5)
	topMessages := getTopMessages(data.Logs, 5)

	return AggregateInsights{
		TotalCount:        totalCount,
		UniqueKeys:        uniqueKeys,
		TopKeys:           topKeys,
		TopMessages:       topMessages,
		AverageLogsPerKey: averageLogsPerKey,
	}
}

func getTopKeys(counts map[string]int, n int) []KeyCount {
	var keyCounts []KeyCount
	for key, count := range counts {
		keyCounts = append(keyCounts, KeyCount{Key: key, Count: count})
	}

	sort.Slice(keyCounts, func(i, j int) bool {
		return keyCounts[i].Count > keyCounts[j].Count
	})

	if len(keyCounts) > n {
		keyCounts = keyCounts[:n]
	}

	return keyCounts
}

func getTopMessages(logs map[string][]datadogV2.Log, n int) []MessageCount {
	messageCounts := make(map[string]int)
	for _, logList := range logs {
		for _, log := range logList {
			if log.Attributes != nil && log.Attributes.Message != nil {
				messageCounts[*log.Attributes.Message]++
			}
		}
	}

	var topMessages []MessageCount
	for message, count := range messageCounts {
		topMessages = append(topMessages, MessageCount{Message: message, Count: count})
	}

	sort.Slice(topMessages, func(i, j int) bool {
		return topMessages[i].Count > topMessages[j].Count
	})

	if len(topMessages) > n {
		topMessages = topMessages[:n]
	}

	return topMessages
}

func CompareWithHistorical(current AggregateInsights, historical AggregateInsights) map[string]float64 {
	comparison := make(map[string]float64)

	comparison["TotalCountDiff"] = float64(current.TotalCount - historical.TotalCount)
	comparison["UniqueKeysDiff"] = float64(current.UniqueKeys - historical.UniqueKeys)
	comparison["AverageLogsPerKeyDiff"] = current.AverageLogsPerKey - historical.AverageLogsPerKey

	comparison["TotalCountPercentChange"] = percentageChange(float64(historical.TotalCount), float64(current.TotalCount))
	comparison["UniqueKeysPercentChange"] = percentageChange(float64(historical.UniqueKeys), float64(current.UniqueKeys))
	comparison["AverageLogsPerKeyPercentChange"] = percentageChange(historical.AverageLogsPerKey, current.AverageLogsPerKey)

	for _, keyCount := range current.TopKeys {
		historicalCount := getHistoricalKeyCount(keyCount.Key, historical.TopKeys)
		comparison[fmt.Sprintf("%sCountDiff", keyCount.Key)] = float64(keyCount.Count - historicalCount)
		comparison[fmt.Sprintf("%sPercentChange", keyCount.Key)] = percentageChange(float64(historicalCount), float64(keyCount.Count))
	}

	return comparison
}

func percentageChange(old, new float64) float64 {
	if old == 0 {
		if new == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return ((new - old) / old) * 100
}

func getHistoricalKeyCount(key string, historicalTopKeys []KeyCount) int {
	for _, keyCount := range historicalTopKeys {
		if keyCount.Key == key {
			return keyCount.Count
		}
	}
	return 0
}
