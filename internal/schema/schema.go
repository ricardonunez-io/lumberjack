package schema

type FieldType string

const (
	FieldTypeString  FieldType = "string"
	FieldTypeNumber  FieldType = "number"
	FieldTypeBool    FieldType = "bool"
	FieldTypeUnknown FieldType = "unknown"
)

type Field struct {
	Name        string
	Type        FieldType
	Cardinality int
	Examples    []string
}

type Schema struct {
	Fields []Field
}

func (s Schema) FieldNames() []string {
	names := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		names[i] = f.Name
	}
	return names
}

func (s Schema) HasField(name string) bool {
	for _, f := range s.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}
