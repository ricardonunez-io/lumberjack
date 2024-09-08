package aggregator

import (
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
)

func strPtr(s string) *string        { return &s }
func timePtr(t time.Time) *time.Time { return &t }

func testSchema() schema.Schema {
	return schema.Schema{
		Fields: []schema.Field{
			{Name: "status", Type: schema.FieldTypeString},
			{Name: "host", Type: schema.FieldTypeString},
			{Name: "service", Type: schema.FieldTypeString},
		},
	}
}

func makeLogs() []datadogV2.Log {
	return []datadogV2.Log{
		{
			Attributes: &datadogV2.LogAttributes{
				Status:  strPtr("error"),
				Host:    strPtr("web-01"),
				Service: strPtr("api"),
				Message: strPtr("connection timeout"),
			},
		},
		{
			Attributes: &datadogV2.LogAttributes{
				Status:  strPtr("error"),
				Host:    strPtr("web-02"),
				Service: strPtr("api"),
				Message: strPtr("connection timeout"),
			},
		},
		{
			Attributes: &datadogV2.LogAttributes{
				Status:  strPtr("warning"),
				Host:    strPtr("web-01"),
				Service: strPtr("worker"),
				Message: strPtr("high memory usage"),
			},
		},
		{
			Attributes: &datadogV2.LogAttributes{
				Status:  strPtr("info"),
				Host:    strPtr("web-01"),
				Service: strPtr("api"),
				Message: strPtr("request completed"),
			},
		},
	}
}

func TestAggregate_AllSeverity(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "ALL")

	statusDim := agg.Dimensions["status"]
	if statusDim == nil {
		t.Fatal("status dimension should exist")
	}
	if statusDim.Counts["error"] != 2 {
		t.Errorf("error count: got %d, want 2", statusDim.Counts["error"])
	}
	if statusDim.Counts["warning"] != 1 {
		t.Errorf("warning count: got %d, want 1", statusDim.Counts["warning"])
	}
	if statusDim.Counts["info"] != 1 {
		t.Errorf("info count: got %d, want 1", statusDim.Counts["info"])
	}
}

func TestAggregate_MediumSeverity(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "MEDIUM")

	statusDim := agg.Dimensions["status"]
	if statusDim == nil {
		t.Fatal("status dimension should exist")
	}
	if statusDim.Counts["error"] != 2 {
		t.Errorf("error count: got %d, want 2", statusDim.Counts["error"])
	}
	if statusDim.Counts["warning"] != 1 {
		t.Errorf("warning count: got %d, want 1", statusDim.Counts["warning"])
	}
	if _, exists := statusDim.Counts["info"]; exists {
		t.Error("info should be filtered out at MEDIUM severity")
	}
}

func TestAggregate_SevereSeverity(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "SEVERE")

	statusDim := agg.Dimensions["status"]
	if statusDim == nil {
		t.Fatal("status dimension should exist")
	}
	if statusDim.Counts["error"] != 2 {
		t.Errorf("error count: got %d, want 2", statusDim.Counts["error"])
	}
	if _, exists := statusDim.Counts["warning"]; exists {
		t.Error("warning should be filtered at SEVERE")
	}
	if _, exists := statusDim.Counts["info"]; exists {
		t.Error("info should be filtered at SEVERE")
	}
}

func TestAggregate_HostDimension(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "ALL")

	hostDim := agg.Dimensions["host"]
	if hostDim == nil {
		t.Fatal("host dimension should exist")
	}
	if hostDim.Counts["web-01"] != 3 {
		t.Errorf("web-01 count: got %d, want 3", hostDim.Counts["web-01"])
	}
	if hostDim.Counts["web-02"] != 1 {
		t.Errorf("web-02 count: got %d, want 1", hostDim.Counts["web-02"])
	}
}

func TestAggregate_ServiceDimension(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "ALL")

	svcDim := agg.Dimensions["service"]
	if svcDim == nil {
		t.Fatal("service dimension should exist")
	}
	if svcDim.Counts["api"] != 3 {
		t.Errorf("api count: got %d, want 3", svcDim.Counts["api"])
	}
	if svcDim.Counts["worker"] != 1 {
		t.Errorf("worker count: got %d, want 1", svcDim.Counts["worker"])
	}
}

func TestAggregate_MessageGroups(t *testing.T) {
	logs := makeLogs()
	s := testSchema()
	agg := Aggregate(logs, s, "ALL")

	statusDim := agg.Dimensions["status"]
	if len(statusDim.MessageGroups) == 0 {
		t.Error("status dimension should have message groups")
	}

	found := false
	for _, g := range statusDim.MessageGroups {
		if g.Template == "connection timeout" {
			found = true
			if g.Count < 2 {
				t.Errorf("connection timeout group count: got %d, want >= 2", g.Count)
			}
		}
	}
	if !found {
		t.Error("should have a 'connection timeout' message group")
	}
}

func TestAggregate_EmptyLogs(t *testing.T) {
	s := testSchema()
	agg := Aggregate(nil, s, "ALL")

	for name, dim := range agg.Dimensions {
		if len(dim.Counts) != 0 {
			t.Errorf("dimension %s should have 0 counts with empty logs", name)
		}
	}
}

func TestAggregate_NilAttributes(t *testing.T) {
	logs := []datadogV2.Log{
		{Attributes: nil},
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("error"), Host: strPtr("h"), Service: strPtr("s"), Message: strPtr("msg")}},
	}
	s := testSchema()
	agg := Aggregate(logs, s, "ALL")

	if agg.Dimensions["status"].Counts["error"] != 1 {
		t.Error("should handle nil attributes gracefully")
	}
}

func TestAggregate_DynamicSchema(t *testing.T) {
	logs := []datadogV2.Log{
		{
			Attributes: &datadogV2.LogAttributes{
				Status:  strPtr("error"),
				Message: strPtr("test"),
				Attributes: map[string]interface{}{
					"env": "production",
				},
			},
		},
	}
	s := schema.Schema{
		Fields: []schema.Field{
			{Name: "status", Type: schema.FieldTypeString},
			{Name: "env", Type: schema.FieldTypeString},
		},
	}
	agg := Aggregate(logs, s, "ALL")

	envDim := agg.Dimensions["env"]
	if envDim == nil {
		t.Fatal("env dimension should exist")
	}
	if envDim.Counts["production"] != 1 {
		t.Errorf("production count: got %d, want 1", envDim.Counts["production"])
	}
}

func TestGetNestedValue(t *testing.T) {
	attrs := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": "deep_value",
		},
		"flat": "flat_value",
	}

	if v := getNestedValue(attrs, "flat"); v != "flat_value" {
		t.Errorf("flat: got %q, want 'flat_value'", v)
	}
	if v := getNestedValue(attrs, "level1.level2"); v != "deep_value" {
		t.Errorf("nested: got %q, want 'deep_value'", v)
	}
	if v := getNestedValue(attrs, "missing"); v != "" {
		t.Errorf("missing: got %q, want empty", v)
	}
	if v := getNestedValue(nil, "any"); v != "" {
		t.Errorf("nil attrs: got %q, want empty", v)
	}
}
