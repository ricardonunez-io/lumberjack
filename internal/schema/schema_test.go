package schema

import (
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

func strPtr(s string) *string        { return &s }
func timePtr(t time.Time) *time.Time { return &t }

func makeLogs(count int) []datadogV2.Log {
	logs := make([]datadogV2.Log, count)
	for i := range logs {
		logs[i] = datadogV2.Log{
			Attributes: &datadogV2.LogAttributes{
				Status:    strPtr("error"),
				Host:      strPtr("web-01"),
				Service:   strPtr("api"),
				Message:   strPtr("something failed"),
				Timestamp: timePtr(time.Now()),
				Attributes: map[string]interface{}{
					"env":    "production",
					"region": "us-east-1",
					"nested": map[string]interface{}{
						"level": "deep",
					},
				},
			},
		}
	}
	return logs
}

func TestDiscover_BasicFields(t *testing.T) {
	logs := makeLogs(5)
	s := Discover(logs)

	if !s.HasField("status") {
		t.Error("schema should have status field")
	}
	if !s.HasField("host") {
		t.Error("schema should have host field")
	}
	if !s.HasField("service") {
		t.Error("schema should have service field")
	}
}

func TestDiscover_CustomAttributes(t *testing.T) {
	logs := makeLogs(5)
	s := Discover(logs)

	if !s.HasField("env") {
		t.Error("schema should have env field from attributes")
	}
	if !s.HasField("region") {
		t.Error("schema should have region field from attributes")
	}
}

func TestDiscover_NestedAttributes(t *testing.T) {
	logs := makeLogs(5)
	s := Discover(logs)

	if !s.HasField("nested.level") {
		t.Error("schema should have nested.level field")
	}
}

func TestDiscover_EmptyLogs(t *testing.T) {
	s := Discover(nil)
	if len(s.Fields) != 0 {
		t.Errorf("empty logs: got %d fields, want 0", len(s.Fields))
	}
}

func TestDiscover_NilAttributes(t *testing.T) {
	logs := []datadogV2.Log{
		{Attributes: nil},
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("ok")}},
	}
	s := Discover(logs)
	if !s.HasField("status") {
		t.Error("should still discover fields from valid logs")
	}
}

func TestDiscover_Cardinality(t *testing.T) {
	logs := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("error")}},
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("warning")}},
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("info")}},
	}
	s := Discover(logs)
	for _, f := range s.Fields {
		if f.Name == "status" {
			if f.Cardinality != 3 {
				t.Errorf("status cardinality: got %d, want 3", f.Cardinality)
			}
			return
		}
	}
	t.Error("status field not found")
}

func TestDiscover_FieldTypes(t *testing.T) {
	logs := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Attributes: map[string]interface{}{
				"count":   float64(42),
				"enabled": true,
				"name":    "test",
			},
		}},
	}
	s := Discover(logs)

	for _, f := range s.Fields {
		switch f.Name {
		case "count":
			if f.Type != FieldTypeNumber {
				t.Errorf("count type: got %s, want number", f.Type)
			}
		case "enabled":
			if f.Type != FieldTypeBool {
				t.Errorf("enabled type: got %s, want bool", f.Type)
			}
		case "name":
			if f.Type != FieldTypeString {
				t.Errorf("name type: got %s, want string", f.Type)
			}
		}
	}
}

func TestDiscover_MaxSampleSize(t *testing.T) {
	logs := makeLogs(500)
	s := Discover(logs)
	if len(s.Fields) == 0 {
		t.Error("should discover fields even with > maxSampleSize logs")
	}
}

func TestDiscover_ExamplesLimited(t *testing.T) {
	logs := make([]datadogV2.Log, 20)
	for i := range logs {
		status := "status_" + string(rune('a'+i))
		logs[i] = datadogV2.Log{
			Attributes: &datadogV2.LogAttributes{
				Status: strPtr(status),
			},
		}
	}
	s := Discover(logs)
	for _, f := range s.Fields {
		if f.Name == "status" && len(f.Examples) > maxExamples {
			t.Errorf("too many examples: got %d, want <= %d", len(f.Examples), maxExamples)
		}
	}
}

func TestSchema_FieldNames(t *testing.T) {
	s := Schema{Fields: []Field{
		{Name: "a"}, {Name: "b"}, {Name: "c"},
	}}
	names := s.FieldNames()
	if len(names) != 3 {
		t.Fatalf("FieldNames: got %d, want 3", len(names))
	}
	if names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Errorf("FieldNames: got %v", names)
	}
}

func TestSchema_HasField(t *testing.T) {
	s := Schema{Fields: []Field{{Name: "status"}, {Name: "host"}}}
	if !s.HasField("status") {
		t.Error("HasField should find status")
	}
	if s.HasField("missing") {
		t.Error("HasField should not find missing")
	}
}

func TestCache_RefreshesEveryN(t *testing.T) {
	c := NewCache(3)

	logs1 := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{Status: strPtr("error")}},
	}
	logs2 := []datadogV2.Log{
		{Attributes: &datadogV2.LogAttributes{
			Status: strPtr("error"),
			Host:   strPtr("web-01"),
		}},
	}

	s1 := c.Get(logs1)
	s2 := c.Get(logs2)
	if len(s1.Fields) != len(s2.Fields) {
		t.Error("cache should return same schema before refresh interval")
	}

	_ = c.Get(logs2)
	s4 := c.Get(logs2)
	if !s4.HasField("host") {
		t.Error("cache should have refreshed and discovered host field")
	}
}

func TestCache_Invalidate(t *testing.T) {
	c := NewCache(100)
	logs := makeLogs(5)
	c.Get(logs)

	if c.Current() == nil {
		t.Fatal("current should not be nil after Get")
	}

	c.Invalidate()
	if c.Current() != nil {
		t.Error("current should be nil after Invalidate")
	}
}

func TestCache_Concurrent(t *testing.T) {
	c := NewCache(2)
	logs := makeLogs(10)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Get(logs)
		}()
	}
	wg.Wait()

	if c.Current() == nil {
		t.Error("cache should have a schema after concurrent access")
	}
}
