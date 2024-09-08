package aggregator

import (
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
)

type HistoricalAggregates struct {
	Dimensions map[string]*HistoricalDimensionData `json:"dimensions"`
}

type HistoricalDimensionData struct {
	Intervals []IntervalData `json:"intervals"`
}

type IntervalData struct {
	Messages map[string]int `json:"messages"`
	Count    int            `json:"count"`
}

func AggregateHistorical(responses []datadogV2.Log, s schema.Schema, interval time.Duration, logSeverity string) HistoricalAggregates {
	result := HistoricalAggregates{
		Dimensions: make(map[string]*HistoricalDimensionData),
	}

	if len(responses) == 0 {
		for _, f := range s.Fields {
			result.Dimensions[f.Name] = &HistoricalDimensionData{}
		}
		return result
	}

	var earliest, latest time.Time
	initialized := false
	for _, ddLog := range responses {
		if ddLog.Attributes == nil || ddLog.Attributes.Timestamp == nil {
			continue
		}
		ts := *ddLog.Attributes.Timestamp
		if !initialized {
			earliest = ts
			latest = ts
			initialized = true
			continue
		}
		if ts.Before(earliest) {
			earliest = ts
		}
		if ts.After(latest) {
			latest = ts
		}
	}

	if !initialized {
		for _, f := range s.Fields {
			result.Dimensions[f.Name] = &HistoricalDimensionData{}
		}
		return result
	}

	numIntervals := int(latest.Sub(earliest)/interval) + 1
	for _, f := range s.Fields {
		hd := &HistoricalDimensionData{
			Intervals: make([]IntervalData, numIntervals),
		}
		for i := range hd.Intervals {
			hd.Intervals[i] = IntervalData{Messages: make(map[string]int)}
		}
		result.Dimensions[f.Name] = hd
	}

	for _, ddLog := range responses {
		if ddLog.Attributes == nil || ddLog.Attributes.Timestamp == nil {
			continue
		}

		if ddLog.Attributes.Status != nil && ShouldSkipLog(*ddLog.Attributes.Status, logSeverity) {
			continue
		}

		ts := *ddLog.Attributes.Timestamp
		idx := int(ts.Sub(earliest) / interval)
		if idx < 0 || idx >= numIntervals {
			continue
		}

		var msg string
		if ddLog.Attributes.Message != nil {
			msg = *ddLog.Attributes.Message
		}

		values := extractHistoricalFieldValues(ddLog, s)
		for fieldName := range values {
			dim, ok := result.Dimensions[fieldName]
			if !ok {
				continue
			}
			dim.Intervals[idx].Count++
			if msg != "" {
				dim.Intervals[idx].Messages[msg]++
			}
		}
	}

	return result
}

func extractHistoricalFieldValues(l datadogV2.Log, s schema.Schema) map[string]string {
	values := make(map[string]string)
	if l.Attributes == nil {
		return values
	}

	for _, f := range s.Fields {
		switch f.Name {
		case "status":
			if l.Attributes.Status != nil {
				values[f.Name] = strings.ToLower(*l.Attributes.Status)
			}
		case "host":
			if l.Attributes.Host != nil {
				values[f.Name] = *l.Attributes.Host
			}
		case "service":
			if l.Attributes.Service != nil {
				values[f.Name] = *l.Attributes.Service
			}
		default:
			val := getNestedValue(l.Attributes.Attributes, f.Name)
			if val != "" {
				values[f.Name] = val
			}
		}
	}

	return values
}

func HistoricalToAggregates(hist HistoricalAggregates) Aggregates {
	agg := Aggregates{
		Dimensions: make(map[string]*DimensionData),
	}

	for name, hd := range hist.Dimensions {
		dim := &DimensionData{
			Counts: make(map[string]int),
		}
		for _, interval := range hd.Intervals {
			for msg, count := range interval.Messages {
				dim.Counts[msg] += count
			}
		}
		agg.Dimensions[name] = dim
	}

	return agg
}

func ExtractHistoricalInsights(hist HistoricalAggregates, dimension string) HistoricalInsights {
	dim, ok := hist.Dimensions[dimension]
	if !ok || len(dim.Intervals) == 0 {
		return HistoricalInsights{}
	}

	totalCount := 0
	intervalCounts := make([]int, len(dim.Intervals))

	for i, interval := range dim.Intervals {
		totalCount += interval.Count
		intervalCounts[i] = interval.Count
	}

	avg := float64(totalCount) / float64(len(dim.Intervals))
	median := CalculateMedian(intervalCounts)
	stddev := CalculateStdDev(intervalCounts, avg)

	return HistoricalInsights{
		TotalCount:        totalCount,
		AverageCount:      avg,
		MedianCount:       median,
		StandardDeviation: stddev,
		IntervalCounts:    intervalCounts,
	}
}

type HistoricalInsights struct {
	TotalCount        int     `json:"totalCount"`
	AverageCount      float64 `json:"averageCount"`
	MedianCount       float64 `json:"medianCount"`
	StandardDeviation float64 `json:"standardDeviation"`
	IntervalCounts    []int   `json:"intervalCounts"`
}

func CompareHistoricalInsights(current HistoricalInsights, baseline HistoricalInsights) map[string]float64 {
	comparison := make(map[string]float64)

	comparison["TotalCountDiff"] = float64(current.TotalCount - baseline.TotalCount)
	comparison["AverageCountDiff"] = current.AverageCount - baseline.AverageCount
	comparison["MedianCountDiff"] = current.MedianCount - baseline.MedianCount
	comparison["StandardDeviationDiff"] = current.StandardDeviation - baseline.StandardDeviation

	if baseline.StandardDeviation > 0 {
		zScores := make([]float64, len(current.IntervalCounts))
		for i, count := range current.IntervalCounts {
			zScores[i] = (float64(count) - baseline.AverageCount) / baseline.StandardDeviation
		}
		comparison["MaxZScore"] = MaxFloat64(zScores)
		comparison["MinZScore"] = MinFloat64(zScores)
		comparison["AverageZScore"] = AverageFloat64(zScores)
	}

	return comparison
}
