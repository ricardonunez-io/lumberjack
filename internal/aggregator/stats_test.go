package aggregator

import (
	"math"
	"testing"

	"github.com/ricardonunez-io/lumberjack/internal/fuzzy"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
)

func TestExtractInsights_Basic(t *testing.T) {
	agg := Aggregates{
		Dimensions: map[string]*DimensionData{
			"status": {
				Counts: map[string]int{
					"error":   10,
					"warning": 5,
					"info":    85,
				},
				MessageGroups: []fuzzy.MessageGroup{},
			},
		},
	}

	insights := ExtractInsights(agg, "status")
	if insights.TotalCount != 100 {
		t.Errorf("TotalCount: got %d, want 100", insights.TotalCount)
	}
	if insights.UniqueKeys != 3 {
		t.Errorf("UniqueKeys: got %d, want 3", insights.UniqueKeys)
	}
	expected := 100.0 / 3.0
	if math.Abs(insights.AverageLogsPerKey-expected) > 0.01 {
		t.Errorf("AverageLogsPerKey: got %f, want %f", insights.AverageLogsPerKey, expected)
	}
}

func TestExtractInsights_MissingDimension(t *testing.T) {
	agg := Aggregates{
		Dimensions: map[string]*DimensionData{},
	}
	insights := ExtractInsights(agg, "nonexistent")
	if insights.TotalCount != 0 {
		t.Error("missing dimension should return zero insights")
	}
}

func TestExtractInsights_EmptyCounts(t *testing.T) {
	agg := Aggregates{
		Dimensions: map[string]*DimensionData{
			"status": {Counts: map[string]int{}},
		},
	}
	insights := ExtractInsights(agg, "status")
	if insights.TotalCount != 0 {
		t.Errorf("TotalCount: got %d, want 0", insights.TotalCount)
	}
	if insights.AverageLogsPerKey != 0 {
		t.Errorf("AverageLogsPerKey: got %f, want 0", insights.AverageLogsPerKey)
	}
}

func TestExtractInsights_TopKeys(t *testing.T) {
	counts := map[string]int{}
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		counts[key] = (i + 1) * 10
	}
	agg := Aggregates{
		Dimensions: map[string]*DimensionData{
			"dim": {Counts: counts},
		},
	}
	insights := ExtractInsights(agg, "dim")
	if len(insights.TopKeys) != 5 {
		t.Fatalf("TopKeys: got %d, want 5", len(insights.TopKeys))
	}
	if insights.TopKeys[0].Count < insights.TopKeys[1].Count {
		t.Error("TopKeys should be sorted descending")
	}
}

func TestCompareInsights_NoDiff(t *testing.T) {
	a := Insights{TotalCount: 100, UniqueKeys: 5, AverageLogsPerKey: 20}
	b := Insights{TotalCount: 100, UniqueKeys: 5, AverageLogsPerKey: 20}
	comp := CompareInsights(a, b)

	if comp["TotalCountDiff"] != 0 {
		t.Errorf("TotalCountDiff: got %f, want 0", comp["TotalCountDiff"])
	}
	if comp["TotalCountPercentChange"] != 0 {
		t.Errorf("TotalCountPercentChange: got %f, want 0", comp["TotalCountPercentChange"])
	}
}

func TestCompareInsights_Increase(t *testing.T) {
	current := Insights{TotalCount: 200, UniqueKeys: 10, AverageLogsPerKey: 20}
	historical := Insights{TotalCount: 100, UniqueKeys: 5, AverageLogsPerKey: 20}
	comp := CompareInsights(current, historical)

	if comp["TotalCountDiff"] != 100 {
		t.Errorf("TotalCountDiff: got %f, want 100", comp["TotalCountDiff"])
	}
	if comp["TotalCountPercentChange"] != 100 {
		t.Errorf("TotalCountPercentChange: got %f, want 100", comp["TotalCountPercentChange"])
	}
}

func TestCompareInsights_ZeroBaseline(t *testing.T) {
	current := Insights{TotalCount: 50}
	historical := Insights{TotalCount: 0}
	comp := CompareInsights(current, historical)

	if !math.IsInf(comp["TotalCountPercentChange"], 1) {
		t.Errorf("TotalCountPercentChange from 0: got %f, want +Inf", comp["TotalCountPercentChange"])
	}
}

func TestCompareInsights_BothZero(t *testing.T) {
	current := Insights{TotalCount: 0}
	historical := Insights{TotalCount: 0}
	comp := CompareInsights(current, historical)

	if comp["TotalCountPercentChange"] != 0 {
		t.Errorf("TotalCountPercentChange both 0: got %f, want 0", comp["TotalCountPercentChange"])
	}
}

func TestCompareInsights_TopKeysDiff(t *testing.T) {
	current := Insights{
		TopKeys: []KeyCount{{Key: "error", Count: 50}, {Key: "warning", Count: 20}},
	}
	historical := Insights{
		TopKeys: []KeyCount{{Key: "error", Count: 30}},
	}
	comp := CompareInsights(current, historical)

	if comp["error_CountDiff"] != 20 {
		t.Errorf("error CountDiff: got %f, want 20", comp["error_CountDiff"])
	}
	if comp["warning_CountDiff"] != 20 {
		t.Errorf("warning CountDiff: got %f, want 20 (no historical baseline)", comp["warning_CountDiff"])
	}
}

func TestCalculateMedian_Odd(t *testing.T) {
	m := CalculateMedian([]int{1, 3, 5})
	if m != 3 {
		t.Errorf("median odd: got %f, want 3", m)
	}
}

func TestCalculateMedian_Even(t *testing.T) {
	m := CalculateMedian([]int{1, 2, 3, 4})
	if m != 2.5 {
		t.Errorf("median even: got %f, want 2.5", m)
	}
}

func TestCalculateMedian_Empty(t *testing.T) {
	m := CalculateMedian(nil)
	if m != 0 {
		t.Errorf("median empty: got %f, want 0", m)
	}
}

func TestCalculateMedian_Single(t *testing.T) {
	m := CalculateMedian([]int{42})
	if m != 42 {
		t.Errorf("median single: got %f, want 42", m)
	}
}

func TestCalculateStdDev_Uniform(t *testing.T) {
	sd := CalculateStdDev([]int{5, 5, 5, 5}, 5)
	if sd != 0 {
		t.Errorf("stddev uniform: got %f, want 0", sd)
	}
}

func TestCalculateStdDev_Varied(t *testing.T) {
	sd := CalculateStdDev([]int{2, 4, 4, 4, 5, 5, 7, 9}, 5)
	if sd < 1.9 || sd > 2.1 {
		t.Errorf("stddev varied: got %f, want ~2.0", sd)
	}
}

func TestCalculateStdDev_Empty(t *testing.T) {
	sd := CalculateStdDev(nil, 0)
	if sd != 0 {
		t.Errorf("stddev empty: got %f, want 0", sd)
	}
}

func TestMaxFloat64(t *testing.T) {
	if MaxFloat64([]float64{1, 5, 3}) != 5 {
		t.Error("MaxFloat64 failed")
	}
	if MaxFloat64(nil) != 0 {
		t.Error("MaxFloat64 empty should be 0")
	}
}

func TestMinFloat64(t *testing.T) {
	if MinFloat64([]float64{1, 5, 3}) != 1 {
		t.Error("MinFloat64 failed")
	}
	if MinFloat64(nil) != 0 {
		t.Error("MinFloat64 empty should be 0")
	}
}

func TestAverageFloat64(t *testing.T) {
	avg := AverageFloat64([]float64{2, 4, 6})
	if avg != 4 {
		t.Errorf("AverageFloat64: got %f, want 4", avg)
	}
	if AverageFloat64(nil) != 0 {
		t.Error("AverageFloat64 empty should be 0")
	}
}

func TestShouldSkipLog(t *testing.T) {
	tests := []struct {
		status   string
		severity string
		want     bool
	}{
		{"error", "ALL", false},
		{"info", "ALL", false},
		{"info", "MEDIUM", true},
		{"debug", "MEDIUM", true},
		{"warning", "MEDIUM", false},
		{"error", "MEDIUM", false},
		{"info", "SEVERE", true},
		{"warning", "SEVERE", true},
		{"debug", "SEVERE", true},
		{"error", "SEVERE", false},
	}

	for _, tc := range tests {
		got := ShouldSkipLog(tc.status, tc.severity)
		if got != tc.want {
			t.Errorf("ShouldSkipLog(%q, %q): got %v, want %v", tc.status, tc.severity, got, tc.want)
		}
	}
}

func TestPercentageChange(t *testing.T) {
	if percentageChange(100, 150) != 50 {
		t.Error("50% increase")
	}
	if percentageChange(100, 50) != -50 {
		t.Error("50% decrease")
	}
	if percentageChange(0, 0) != 0 {
		t.Error("0 to 0")
	}
	if !math.IsInf(percentageChange(0, 100), 1) {
		t.Error("0 to positive should be +Inf")
	}
}

func TestGetTopKeys_LessThanN(t *testing.T) {
	counts := map[string]int{"a": 1, "b": 2}
	top := getTopKeys(counts, 5)
	if len(top) != 2 {
		t.Errorf("getTopKeys: got %d, want 2", len(top))
	}
}

func TestGetTopKeys_Empty(t *testing.T) {
	top := getTopKeys(map[string]int{}, 5)
	if len(top) != 0 {
		t.Errorf("getTopKeys empty: got %d, want 0", len(top))
	}
}

func TestSeverityIncludes(t *testing.T) {
	_ = schema.Schema{}

	if !ValidLogSeverities.Includes("ALL") {
		t.Error("should include ALL")
	}
	if !ValidLogSeverities.Includes("medium") {
		t.Error("should include medium (case-insensitive)")
	}
	if ValidLogSeverities.Includes("INVALID") {
		t.Error("should not include INVALID")
	}
}
