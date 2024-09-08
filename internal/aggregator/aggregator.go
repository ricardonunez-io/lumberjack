package aggregator

import (
	"fmt"
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/ricardonunez-io/lumberjack/internal/fuzzy"
	"github.com/ricardonunez-io/lumberjack/internal/schema"
	"github.com/rs/zerolog/log"
)

type Aggregates struct {
	Dimensions map[string]*DimensionData `json:"dimensions"`
}

type DimensionData struct {
	Counts        map[string]int       `json:"counts"`
	MessageGroups []fuzzy.MessageGroup `json:"messageGroups"`
	messages      []string
}

func Aggregate(responses []datadogV2.Log, s schema.Schema, logSeverity string) Aggregates {
	agg := Aggregates{
		Dimensions: make(map[string]*DimensionData),
	}

	for _, f := range s.Fields {
		agg.Dimensions[f.Name] = &DimensionData{
			Counts: make(map[string]int),
		}
	}

	for _, ddLog := range responses {
		if ddLog.Attributes == nil {
			continue
		}

		if ddLog.Attributes.Status != nil && ShouldSkipLog(*ddLog.Attributes.Status, logSeverity) {
			log.Debug().Str("id", stringId(ddLog.Id)).Msg("Skipping log due to severity filter")
			continue
		}

		values := extractFieldValues(ddLog, s)
		var msg string
		if ddLog.Attributes.Message != nil {
			msg = *ddLog.Attributes.Message
		}

		for fieldName, value := range values {
			dim, ok := agg.Dimensions[fieldName]
			if !ok {
				continue
			}
			dim.Counts[value]++
			if msg != "" {
				dim.messages = append(dim.messages, msg)
			}
		}
	}

	for _, dim := range agg.Dimensions {
		if len(dim.messages) > 0 {
			dim.MessageGroups = fuzzy.Group(dim.messages)
		}
		dim.messages = nil
	}

	return agg
}

func extractFieldValues(l datadogV2.Log, s schema.Schema) map[string]string {
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

func getNestedValue(attrs interface{}, key string) string {
	if attrs == nil {
		return ""
	}

	m, ok := attrs.(map[string]interface{})
	if !ok {
		return ""
	}

	parts := strings.SplitN(key, ".", 2)
	val, exists := m[parts[0]]
	if !exists {
		return ""
	}

	if len(parts) == 1 {
		return fmt.Sprintf("%v", val)
	}

	return getNestedValue(val, parts[1])
}

func stringId(id *string) string {
	if id == nil {
		return "<nil>"
	}
	return *id
}
