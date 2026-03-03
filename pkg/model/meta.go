package model

import "reflect"

// FieldMeta holds schema information for a single struct field,
// derived from struct tags at startup.
type FieldMeta struct {
	FieldName  string            // Go struct field name
	Column     string            // mapped column / key name
	GoType     reflect.Type      // runtime type
	Tags       map[string]string // parsed tag pairs (type, primary_key, …)
	PrimaryKey bool
	AutoIncr   bool
	Nullable   bool
	Index      string // secondary index name, if any
}

// EntityMeta describes the full mapping of a Go struct to a
// storage entity (table, collection, bucket, …).
type EntityMeta struct {
	Name        string       // entity / table name
	GoType      reflect.Type // struct type
	Fields      []FieldMeta
	PrimaryKeys []*FieldMeta // composite PK fields
	PrimaryKey  *FieldMeta  // shortcut to the first PK field (common case)
	fieldIndex  map[string]int
}

// Field returns field metadata by Go struct name.
func (m *EntityMeta) Field(name string) *FieldMeta {
	if idx, ok := m.fieldIndex[name]; ok {
		return &m.Fields[idx]
	}
	return nil
}

// FieldByColumn returns field metadata by storage column name.
func (m *EntityMeta) FieldByColumn(col string) *FieldMeta {
	for i := range m.Fields {
		if m.Fields[i].Column == col {
			return &m.Fields[i]
		}
	}
	return nil
}

// BuildIndex populates the internal lookup map (call after Fields is set).
func (m *EntityMeta) BuildIndex() {
	m.fieldIndex = make(map[string]int, len(m.Fields))
	for i, f := range m.Fields {
		m.fieldIndex[f.FieldName] = i
	}
}
