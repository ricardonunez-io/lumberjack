package aggregator

import (
	"fmt"
	"math"
	"sort"
)

type Insights struct {
	TotalCount        int        `json:"totalCount"`
	UniqueKeys        int        `json:"uniqueKeys"`
	TopKeys           []KeyCount `json:"topKeys"`
	AverageLogsPerKey float64    `json:"averageLogsPerKey"`
}

type KeyCount struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

func ExtractInsights(agg Aggregates, dimension string) Insights {
	dim, ok := agg.Dimensions[dimension]
	if !ok {
		return Insights{}
	}

	totalCount := 0
	for _, count := range dim.Counts {
		totalCount += count
	}

	uniqueKeys := len(dim.Counts)
	var averageLogsPerKey float64
	if uniqueKeys > 0 {
		averageLogsPerKey = float64(totalCount) / float64(uniqueKeys)
	}

	topKeys := getTopKeys(dim.Counts, 5)

	return Insights{
		TotalCount:        totalCount,
		UniqueKeys:        uniqueKeys,
		TopKeys:           topKeys,
		AverageLogsPerKey: averageLogsPerKey,
	}
}

func CompareInsights(current Insights, historical Insights) map[string]float64 {
	comparison := make(map[string]float64)

	comparison["TotalCountDiff"] = float64(current.TotalCount - historical.TotalCount)
	comparison["UniqueKeysDiff"] = float64(current.UniqueKeys - historical.UniqueKeys)
	comparison["AverageLogsPerKeyDiff"] = current.AverageLogsPerKey - historical.AverageLogsPerKey

	comparison["TotalCountPercentChange"] = percentageChange(float64(historical.TotalCount), float64(current.TotalCount))
	comparison["UniqueKeysPercentChange"] = percentageChange(float64(historical.UniqueKeys), float64(current.UniqueKeys))
	comparison["AverageLogsPerKeyPercentChange"] = percentageChange(historical.AverageLogsPerKey, current.AverageLogsPerKey)

	for _, keyCount := range current.TopKeys {
		historicalCount := getHistoricalKeyCount(keyCount.Key, historical.TopKeys)
		comparison[fmt.Sprintf("%s_CountDiff", keyCount.Key)] = float64(keyCount.Count - historicalCount)
		comparison[fmt.Sprintf("%s_PercentChange", keyCount.Key)] = percentageChange(float64(historicalCount), float64(keyCount.Count))
	}

	return comparison
}

func getTopKeys(counts map[string]int, n int) []KeyCount {
	keyCounts := make([]KeyCount, 0, len(counts))
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

func percentageChange(old, new_ float64) float64 {
	if old == 0 {
		if new_ == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return ((new_ - old) / old) * 100
}

func getHistoricalKeyCount(key string, historicalTopKeys []KeyCount) int {
	for _, keyCount := range historicalTopKeys {
		if keyCount.Key == key {
			return keyCount.Count
		}
	}
	return 0
}

func CalculateMedian(counts []int) float64 {
	if len(counts) == 0 {
		return 0
	}
	sorted := make([]int, len(counts))
	copy(sorted, counts)
	sort.Ints(sorted)

	n := len(sorted)
	if n%2 == 0 {
		return float64(sorted[n/2-1]+sorted[n/2]) / 2
	}
	return float64(sorted[n/2])
}

func CalculateStdDev(counts []int, mean float64) float64 {
	if len(counts) == 0 {
		return 0
	}
	variance := 0.0
	for _, count := range counts {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(counts))
	return math.Sqrt(variance)
}

func MaxFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func MinFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func AverageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
