package aggregator

import (
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
)

func makeHistoricalLogs() []datadogV2.Log {
	base := time.Now().Add(-2 * time.Hour)
	return []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("web-01"), Service: strPtr("api"),
			Message: strPtr("timeout"), Timestamp: timePtr(base),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("web-01"), Service: strPtr("api"),
			Message: strPtr("timeout"), Timestamp: timePtr(base.Add(5 * time.Minute)),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("warning"), Host: strPtr("web-02"), Service: strPtr("worker"),
			Message: strPtr("high memory"), Timestamp: timePtr(base.Add(30 * time.Minute)),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("web-01"), Service: strPtr("api"),
			Message: strPtr("timeout"), Timestamp: timePtr(base.Add(time.Hour)),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("info"), Host: strPtr("web-01"), Service: strPtr("api"),
			Message: strPtr("request ok"), Timestamp: timePtr(base.Add(90 * time.Minute)),
		}},
	}
}

func TestAggregateHistorical_Basic(t *testing.T) {
	logs := makeHistoricalLogs()
	s := testSchema()
	hist := AggregateHistorical(logs, s, 30*time.Minute, "ALL")

	statusDim := hist.Dimensions["status"]
	if statusDim == nil {
		t.Fatal("status dimension should exist")
	}
	if len(statusDim.Intervals) == 0 {
		t.Fatal("should have intervals")
	}

	totalCount := 0
	for _, interval := range statusDim.Intervals {
		totalCount += interval.Count
	}
	if totalCount != 5 {
		t.Errorf("total count: got %d, want 5", totalCount)
	}
}

func TestAggregateHistorical_SeverityFilter(t *testing.T) {
	logs := makeHistoricalLogs()
	s := testSchema()
	hist := AggregateHistorical(logs, s, 30*time.Minute, "SEVERE")

	statusDim := hist.Dimensions["status"]
	totalCount := 0
	for _, interval := range statusDim.Intervals {
		totalCount += interval.Count
	}
	if totalCount != 3 {
		t.Errorf("SEVERE total: got %d, want 3 (only errors)", totalCount)
	}
}

func TestAggregateHistorical_EmptyLogs(t *testing.T) {
	s := testSchema()
	hist := AggregateHistorical(nil, s, time.Hour, "ALL")
	for _, dim := range hist.Dimensions {
		if len(dim.Intervals) != 0 {
			t.Error("empty logs should produce no intervals")
		}
	}
}

func TestAggregateHistorical_IntervalBucketing(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	logs := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"),
			Message: strPtr("a"), Timestamp: timePtr(base),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"),
			Message: strPtr("b"), Timestamp: timePtr(base.Add(45 * time.Minute)),
		}},
	}
	s := testSchema()
	hist := AggregateHistorical(logs, s, 30*time.Minute, "ALL")

	statusDim := hist.Dimensions["status"]
	if len(statusDim.Intervals) < 2 {
		t.Fatalf("should have at least 2 intervals, got %d", len(statusDim.Intervals))
	}
	if statusDim.Intervals[0].Count != 1 {
		t.Errorf("first interval: got %d, want 1", statusDim.Intervals[0].Count)
	}
}

func TestAggregateHistorical_MessageTracking(t *testing.T) {
	base := time.Now()
	logs := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"),
			Message: strPtr("timeout"), Timestamp: timePtr(base),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"),
			Message: strPtr("timeout"), Timestamp: timePtr(base),
		}},
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"),
			Message: strPtr("other error"), Timestamp: timePtr(base),
		}},
	}
	s := testSchema()
	hist := AggregateHistorical(logs, s, time.Hour, "ALL")

	statusDim := hist.Dimensions["status"]
	if len(statusDim.Intervals) == 0 {
		t.Fatal("should have intervals")
	}
	if statusDim.Intervals[0].Messages["timeout"] != 2 {
		t.Errorf("timeout count: got %d, want 2", statusDim.Intervals[0].Messages["timeout"])
	}
}

func TestHistoricalToAggregates(t *testing.T) {
	hist := HistoricalAggregates{
		Dimensions: map[string]*HistoricalDimensionData{
			"status": {
				Intervals: []IntervalData{
					{Messages: map[string]int{"error": 5, "timeout": 3}, Count: 8},
					{Messages: map[string]int{"error": 2}, Count: 2},
				},
			},
		},
	}

	agg := HistoricalToAggregates(hist)
	statusDim := agg.Dimensions["status"]
	if statusDim == nil {
		t.Fatal("status dimension should exist")
	}
	if statusDim.Counts["error"] != 7 {
		t.Errorf("error count: got %d, want 7", statusDim.Counts["error"])
	}
	if statusDim.Counts["timeout"] != 3 {
		t.Errorf("timeout count: got %d, want 3", statusDim.Counts["timeout"])
	}
}

func TestExtractHistoricalInsights(t *testing.T) {
	hist := HistoricalAggregates{
		Dimensions: map[string]*HistoricalDimensionData{
			"status": {
				Intervals: []IntervalData{
					{Count: 10},
					{Count: 20},
					{Count: 30},
				},
			},
		},
	}

	insights := ExtractHistoricalInsights(hist, "status")
	if insights.TotalCount != 60 {
		t.Errorf("TotalCount: got %d, want 60", insights.TotalCount)
	}
	if insights.AverageCount != 20 {
		t.Errorf("AverageCount: got %f, want 20", insights.AverageCount)
	}
	if insights.MedianCount != 20 {
		t.Errorf("MedianCount: got %f, want 20", insights.MedianCount)
	}
}

func TestExtractHistoricalInsights_Missing(t *testing.T) {
	hist := HistoricalAggregates{
		Dimensions: map[string]*HistoricalDimensionData{},
	}
	insights := ExtractHistoricalInsights(hist, "missing")
	if insights.TotalCount != 0 {
		t.Error("missing dimension should return zero insights")
	}
}

func TestCompareHistoricalInsights(t *testing.T) {
	current := HistoricalInsights{
		TotalCount:        100,
		AverageCount:      50,
		MedianCount:       45,
		StandardDeviation: 10,
		IntervalCounts:    []int{40, 60},
	}
	baseline := HistoricalInsights{
		TotalCount:        80,
		AverageCount:      40,
		MedianCount:       38,
		StandardDeviation: 8,
		IntervalCounts:    []int{35, 45},
	}

	comp := CompareHistoricalInsights(current, baseline)
	if comp["TotalCountDiff"] != 20 {
		t.Errorf("TotalCountDiff: got %f, want 20", comp["TotalCountDiff"])
	}
	if comp["AverageCountDiff"] != 10 {
		t.Errorf("AverageCountDiff: got %f, want 10", comp["AverageCountDiff"])
	}
	if _, ok := comp["MaxZScore"]; !ok {
		t.Error("should have MaxZScore")
	}
}

func TestCompareHistoricalInsights_ZeroStdDev(t *testing.T) {
	current := HistoricalInsights{
		TotalCount:     100,
		IntervalCounts: []int{50, 50},
	}
	baseline := HistoricalInsights{
		TotalCount:        100,
		StandardDeviation: 0,
		IntervalCounts:    []int{50, 50},
	}

	comp := CompareHistoricalInsights(current, baseline)
	if _, ok := comp["MaxZScore"]; ok {
		t.Error("should not compute z-scores when stddev is 0")
	}
}

func TestAggregateHistorical_DynamicSchema(t *testing.T) {
	base := time.Now()
	logs := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Status:    strPtr("error"),
			Message:   strPtr("test"),
			Timestamp: timePtr(base),
			Attributes: map[string]interface{}{
				"env": "prod",
			},
		}},
	}
	s := schema.Schema{
		Fields: []schema.Field{
			{Name: "status", Type: schema.FieldTypeString},
			{Name: "env", Type: schema.FieldTypeString},
		},
	}
	hist := AggregateHistorical(logs, s, time.Hour, "ALL")

	envDim := hist.Dimensions["env"]
	if envDim == nil {
		t.Fatal("env dimension should exist")
	}
	totalCount := 0
	for _, interval := range envDim.Intervals {
		totalCount += interval.Count
	}
	if totalCount != 1 {
		t.Errorf("env total: got %d, want 1", totalCount)
	}
}
