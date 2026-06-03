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
	Index      string // explicit secondary index name, if any
	Indexed    bool   // a secondary index is requested (name derived if Index is empty)
	Unique     bool   // the index is unique
}

// RelationKind classifies a navigation field on an entity.
type RelationKind int

const (
	// RelationOneToOne is a *T navigation where the foreign key lives on the
	// related (other) table, e.g. User.Profile with Profile.UserID.
	RelationOneToOne RelationKind = iota
	// RelationBelongsTo is a *T navigation where the foreign key lives on this
	// table, e.g. Order.User with Order.UserID.
	RelationBelongsTo
	// RelationOneToMany is a []*T navigation; the foreign key lives on the
	// related table, e.g. User.Orders with Order.UserID.
	RelationOneToMany
	// RelationManyToMany is a []*T navigation resolved through a join table.
	RelationManyToMany
)

// Relation describes a navigation field linking one entity to another.
// It is derived from struct tags and naming conventions at parse time and
// drives eager loading (Include) and foreign-key generation in migrations.
//
// Key columns are named from the owning entity's perspective:
//   - LocalKey is the column on the owning table.
//   - ForeignKey is the column on the related table (or, for BelongsTo, the
//     related table's primary key referenced by the local FK column).
//
// For RelationManyToMany the link is resolved through JoinTable using
// JoinLocalKey (referencing the owner) and JoinForeignKey (referencing the
// target); LocalKey/ForeignKey then name the primary keys on each side.
type Relation struct {
	Field        string       // Go navigation field name (e.g. "Orders")
	Kind         RelationKind // relationship classification
	Target       reflect.Type // related struct type (element type, not pointer)
	TargetEntity string       // related table name (snake_case of Target)

	LocalKey   string // column on the owning table
	ForeignKey string // column on the related table

	// Many-to-many join table linkage (empty for other kinds).
	JoinTable      string
	JoinLocalKey   string // join-table column referencing the owner
	JoinForeignKey string // join-table column referencing the target
}

// EntityMeta describes the full mapping of a Go struct to a
// storage entity (table, collection, bucket, …).
type EntityMeta struct {
	Name        string       // entity / table name
	GoType      reflect.Type // struct type
	Fields      []FieldMeta
	Relations   []Relation   // navigation fields (not stored as columns)
	PrimaryKeys []*FieldMeta // composite PK fields
	PrimaryKey  *FieldMeta   // shortcut to the first PK field (common case)
	fieldIndex  map[string]int
}

// Relation returns relationship metadata by Go navigation field name.
func (m *EntityMeta) Relation(field string) *Relation {
	for i := range m.Relations {
		if m.Relations[i].Field == field {
			return &m.Relations[i]
		}
	}
	return nil
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
