package schema

import (
	"fmt"
	"sort"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

const maxExamples = 5
const maxSampleSize = 200

func Discover(logs []datadogV2.Log) Schema {
	fieldValues := make(map[string]map[string]struct{})
	fieldTypes := make(map[string]FieldType)

	sample := logs
	if len(sample) > maxSampleSize {
		sample = sample[:maxSampleSize]
	}

	for _, l := range sample {
		if l.Attributes == nil {
			continue
		}

		if l.Attributes.Status != nil {
			trackField(fieldValues, fieldTypes, "status", *l.Attributes.Status)
		}
		if l.Attributes.Host != nil {
			trackField(fieldValues, fieldTypes, "host", *l.Attributes.Host)
		}
		if l.Attributes.Service != nil {
			trackField(fieldValues, fieldTypes, "service", *l.Attributes.Service)
		}

		if l.Attributes.Attributes != nil {
			discoverMap(fieldValues, fieldTypes, "", l.Attributes.Attributes)
		}
	}

	var fields []Field
	for name, values := range fieldValues {
		examples := sortedKeys(values, maxExamples)
		fields = append(fields, Field{
			Name:        name,
			Type:        fieldTypes[name],
			Cardinality: len(values),
			Examples:    examples,
		})
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	return Schema{Fields: fields}
}

func discoverMap(fieldValues map[string]map[string]struct{}, fieldTypes map[string]FieldType, prefix string, m map[string]interface{}) {
	for key, val := range m {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		switch v := val.(type) {
		case map[string]interface{}:
			discoverMap(fieldValues, fieldTypes, fullKey, v)
		case []interface{}:
			continue
		case string:
			trackField(fieldValues, fieldTypes, fullKey, v)
		case float64:
			trackField(fieldValues, fieldTypes, fullKey, fmt.Sprintf("%g", v))
			fieldTypes[fullKey] = FieldTypeNumber
		case bool:
			trackField(fieldValues, fieldTypes, fullKey, fmt.Sprintf("%t", v))
			fieldTypes[fullKey] = FieldTypeBool
		case nil:
			continue
		default:
			trackField(fieldValues, fieldTypes, fullKey, fmt.Sprintf("%v", v))
		}
	}
}

func trackField(fieldValues map[string]map[string]struct{}, fieldTypes map[string]FieldType, name, value string) {
	if _, ok := fieldValues[name]; !ok {
		fieldValues[name] = make(map[string]struct{})
		if _, exists := fieldTypes[name]; !exists {
			fieldTypes[name] = FieldTypeString
		}
	}
	fieldValues[name][value] = struct{}{}
}

func sortedKeys(m map[string]struct{}, max int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > max {
		keys = keys[:max]
	}
	return keys
}
