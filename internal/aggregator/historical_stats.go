package aggregator

import (
	"math"
	"sort"
)

type HistoricalAggregateInsights struct {
	TotalCount        int
	AverageCount      float64
	MedianCount       float64
	StandardDeviation float64
	TopMessages       []MessageCount
	IntervalCounts    []int
}

func ExtractHistoricalInsights(aggregates HistoricalAggregates, aggregateType string) HistoricalAggregateInsights {
	var data HistoricalAggregateData

	switch aggregateType {
	case STATUS_NAME:
		data = aggregates.Status.HistoricalAggregateData
	case HOST_NAME:
		data = aggregates.Host.HistoricalAggregateData
	case SERVICE_NAME:
		data = aggregates.Service.HistoricalAggregateData
	default:
		return HistoricalAggregateInsights{}
	}

	totalCount := 0
	intervalCounts := make([]int, len(data.Intervals))
	allMessages := make(map[string]int)

	for i, interval := range data.Intervals {
		totalCount += interval.Counts
		intervalCounts[i] = interval.Counts

		for message, count := range interval.Logs {
			allMessages[message] += count
		}
	}

	averageCount := float64(totalCount) / float64(len(data.Intervals))
	medianCount := calculateMedian(intervalCounts)
	standardDeviation := calculateStandardDeviation(intervalCounts, averageCount)
	topMessages := getTopHistoricalMessages(allMessages, 5)

	return HistoricalAggregateInsights{
		TotalCount:        totalCount,
		AverageCount:      averageCount,
		MedianCount:       medianCount,
		StandardDeviation: standardDeviation,
		TopMessages:       topMessages,
		IntervalCounts:    intervalCounts,
	}
}

func calculateMedian(counts []int) float64 {
	sortedCounts := make([]int, len(counts))
	copy(sortedCounts, counts)
	sort.Ints(sortedCounts)

	length := len(sortedCounts)
	if length%2 == 0 {
		return float64(sortedCounts[length/2-1]+sortedCounts[length/2]) / 2
	}
	return float64(sortedCounts[length/2])
}

func calculateStandardDeviation(counts []int, mean float64) float64 {
	variance := 0.0
	for _, count := range counts {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(counts))
	return math.Sqrt(variance)
}

func getTopHistoricalMessages(messages map[string]int, n int) []MessageCount {
	var messageCounts []MessageCount
	for message, count := range messages {
		messageCounts = append(messageCounts, MessageCount{Message: message, Count: count})
	}

	sort.Slice(messageCounts, func(i, j int) bool {
		return messageCounts[i].Count > messageCounts[j].Count
	})

	if len(messageCounts) > n {
		messageCounts = messageCounts[:n]
	}

	return messageCounts
}

func CompareWithStandardInterval(historical HistoricalAggregateInsights, standard HistoricalAggregateInsights) map[string]float64 {
	comparison := make(map[string]float64)

	comparison["TotalCountDiff"] = float64(historical.TotalCount - standard.TotalCount)
	comparison["AverageCountDiff"] = historical.AverageCount - standard.AverageCount
	comparison["MedianCountDiff"] = historical.MedianCount - standard.MedianCount
	comparison["StandardDeviationDiff"] = historical.StandardDeviation - standard.StandardDeviation

	zScores := make([]float64, len(historical.IntervalCounts))
	for i, count := range historical.IntervalCounts {
		zScores[i] = (float64(count) - standard.AverageCount) / standard.StandardDeviation
	}

	comparison["MaxZScore"] = maxFloat64(zScores)
	comparison["MinZScore"] = minFloat64(zScores)
	comparison["AverageZScore"] = averageFloat64(zScores)

	return comparison
}

func maxFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

func minFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func averageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
