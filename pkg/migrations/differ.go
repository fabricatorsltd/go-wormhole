package migrations

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// ValidateModels reports the first reason a set of models cannot yet be turned
// into a migration. Composite (multi-column) primary keys are not supported by
// the DDL generator, which renders one inline PRIMARY KEY per column; emitting a
// table for such an entity would produce invalid SQL, so it is rejected rather
// than generated wrong. Declare these tables by hand for now. Runtime CRUD on
// composite-key entities is supported by the SQL providers.
func ValidateModels(targets []*model.EntityMeta) error {
	for _, m := range targets {
		if len(m.PrimaryKeys) > 1 {
			return fmt.Errorf("entity %q has a composite primary key, which migration generation does not yet support; define the table manually", m.Name)
		}
		for _, f := range m.Fields {
			if f.Computed {
				return fmt.Errorf("entity %q column %q is computed; generated-column DDL is not yet supported, define the column manually", m.Name, f.Column)
			}
		}
	}
	return nil
}

// ComputeDiff compares target models (from Go structs) against the
// current database schema and returns the list of DDL operations
// needed to bring the database in sync.
func ComputeDiff(targets []*model.EntityMeta, current DatabaseSchema) []MigrationOp {
	var ops []MigrationOp

	if current.Tables == nil {
		current.Tables = make(map[string]*TableSchema)
	}

	targetNames := make(map[string]bool, len(targets))
	fks := buildForeignKeys(targets)

	for _, meta := range targets {
		targetNames[meta.Name] = true
		existing, exists := current.Tables[meta.Name]

		if !exists {
			// Whole table is new
			ops = append(ops, createTableFromMeta(meta, fks[meta.Name]))
			// All of the table's indexes are new.
			ops = append(ops, diffIndexOps(meta, nil)...)
			continue
		}

		// Table exists: diff columns and indexes against the snapshot.
		ops = append(ops, diffColumns(meta, existing, fks[meta.Name])...)
		ops = append(ops, diffIndexOps(meta, existing)...)
	}

	// Detect dropped tables (in DB but not in code)
	for name := range current.Tables {
		if !targetNames[name] {
			ops = append(ops, DropTableOp{Table: name})
		}
	}

	return ops
}

func createTableFromMeta(meta *model.EntityMeta, fks map[string]*ColumnRef) CreateTableOp {
	cols := make([]ColumnDef, 0, len(meta.Fields))
	for _, f := range meta.Fields {
		cd := fieldToColumnDef(f)
		if ref, ok := fks[cd.Name]; ok {
			cd.Ref = ref
		}
		cols = append(cols, cd)
	}
	return CreateTableOp{Table: meta.Name, Columns: cols}
}

// buildForeignKeys resolves the set of foreign keys for every table from the
// relation metadata across all targets. The result is keyed by table name then
// by FK column name. Many-to-many join tables are skipped (the join entity, if
// modeled, declares its own FKs via BelongsTo).
func buildForeignKeys(targets []*model.EntityMeta) map[string]map[string]*ColumnRef {
	byName := make(map[string]*model.EntityMeta, len(targets))
	for _, m := range targets {
		byName[m.Name] = m
	}
	pkColumn := func(table, fallback string) string {
		if m := byName[table]; m != nil && m.PrimaryKey != nil {
			return m.PrimaryKey.Column
		}
		return fallback
	}

	out := make(map[string]map[string]*ColumnRef)
	put := func(table, col string, ref *ColumnRef) {
		if out[table] == nil {
			out[table] = make(map[string]*ColumnRef)
		}
		// One FK may be modeled from both sides (BelongsTo + OneToMany). Keep the
		// first reference, but adopt a referential action from the other side so
		// on_delete is not lost to iteration order when only one side declares it.
		if existing, ok := out[table][col]; ok {
			if existing.OnDelete == "" {
				existing.OnDelete = ref.OnDelete
			}
			return
		}
		out[table][col] = ref
	}

	for _, owner := range targets {
		for _, rel := range owner.Relations {
			switch rel.Kind {
			case model.RelationBelongsTo:
				// FK on the owner referencing the target's primary key.
				put(owner.Name, rel.LocalKey, &ColumnRef{
					Table:    rel.TargetEntity,
					Column:   pkColumn(rel.TargetEntity, rel.ForeignKey),
					OnDelete: rel.OnDelete,
				})
			case model.RelationOneToMany, model.RelationOneToOne:
				// FK on the related table referencing the owner's local key.
				put(rel.TargetEntity, rel.ForeignKey, &ColumnRef{
					Table:    owner.Name,
					Column:   rel.LocalKey,
					OnDelete: rel.OnDelete,
				})
			}
		}
	}
	return out
}

func diffColumns(meta *model.EntityMeta, table *TableSchema, fks map[string]*ColumnRef) []MigrationOp {
	var ops []MigrationOp

	codeFields := make(map[string]bool, len(meta.Fields))

	for _, f := range meta.Fields {
		codeFields[f.Column] = true
		existing, exists := table.Columns[f.Column]

		if !exists {
			// New column
			cd := fieldToColumnDef(f)
			if ref, ok := fks[cd.Name]; ok {
				cd.Ref = ref
			}
			ops = append(ops, AddColumnOp{
				Table:  meta.Name,
				Column: cd,
			})
			continue
		}

		// Column exists — check for type/constraint changes
		newDef := fieldToColumnDef(f)
		// Preserve the introspected DB type when the model did not
		// explicitly override it via a `type:` tag.
		if newDef.SQLType == "" && existing.SQLType != "" {
			newDef.SQLType = existing.SQLType
		}
		if columnChanged(existing, &newDef) {
			ops = append(ops, AlterColumnOp{
				Table:  meta.Name,
				Column: newDef,
			})
		}
	}

	// Columns in DB but not in code
	for colName := range table.Columns {
		if !codeFields[colName] {
			ops = append(ops, DropColumnOp{
				Table:  meta.Name,
				Column: colName,
			})
		}
	}

	return ops
}

func columnChanged(old *ColumnDef, new *ColumnDef) bool {
	oldType := old.SQLType
	newType := new.SQLType
	if oldType == "" && old.GoType != nil {
		oldType = GoTypeToSQL(old.GoType)
	}
	if newType == "" && new.GoType != nil {
		newType = GoTypeToSQL(new.GoType)
	}
	if oldType != newType {
		return true
	}
	if old.Nullable != new.Nullable {
		return true
	}
	if old.Default != new.Default {
		return true
	}
	return false
}

// indexDef is a resolved index used for diffing. Columns holds one entry for a
// single-column index, or several (in field-declaration order) for a composite
// index built from fields that share an explicit index name.
type indexDef struct {
	name    string
	columns []string
	unique  bool
}

// indexName derives a deterministic, table-qualified index name (engines like
// Postgres have a schema-global index namespace). An explicit name wins.
func indexName(table, column, explicit string, unique bool) string {
	if explicit != "" {
		return explicit
	}
	prefix := "idx"
	if unique {
		prefix = "uniq"
	}
	return prefix + "_" + table + "_" + column
}

// modelIndexes returns the indexes the model declares, keyed by index name.
// Fields that share an explicit index name combine into one composite index,
// with columns in field-declaration order; the index is unique if any of its
// fields is marked unique.
func modelIndexes(meta *model.EntityMeta) map[string]indexDef {
	out := map[string]indexDef{}
	for _, f := range meta.Fields {
		if f.Index == "" && !f.Indexed && !f.Unique {
			continue
		}
		n := indexName(meta.Name, f.Column, f.Index, f.Unique)
		d := out[n]
		d.name = n
		d.columns = append(d.columns, f.Column)
		if f.Unique {
			d.unique = true
		}
		out[n] = d
	}
	return out
}

// snapshotIndexes returns the indexes a snapshot table records, keyed by name.
// Composite members are grouped by index name; their order is the snapshot map's
// (non-deterministic), so the diff compares column sets, not column order.
func snapshotIndexes(table *TableSchema) map[string]indexDef {
	out := map[string]indexDef{}
	if table == nil {
		return out
	}
	for _, c := range table.Columns {
		if c.Index == "" && !c.Indexed && !c.Unique {
			continue
		}
		n := indexName(table.Name, c.Name, c.Index, c.Unique)
		d := out[n]
		d.name = n
		d.columns = append(d.columns, c.Name)
		if c.Unique {
			d.unique = true
		}
		out[n] = d
	}
	return out
}

// sameColumnSet reports whether two column lists hold the same set (order
// insensitive), used because the snapshot does not preserve composite column
// order.
func sameColumnSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// diffIndexOps emits CreateIndex/DropIndex only for indexes that actually
// changed between the snapshot and the model, so an unchanged indexed model
// diffs to zero ops (no spurious re-create). Output is sorted for determinism.
func diffIndexOps(meta *model.EntityMeta, existing *TableSchema) []MigrationOp {
	desired := modelIndexes(meta)
	current := snapshotIndexes(existing)

	var creates, drops []string
	for name, d := range desired {
		c, ok := current[name]
		changed := ok && (c.unique != d.unique || !sameColumnSet(c.columns, d.columns))
		if !ok || changed {
			creates = append(creates, name)
		}
		if changed {
			drops = append(drops, name) // recreate with new uniqueness or columns
		}
	}
	for name := range current {
		if _, ok := desired[name]; !ok {
			drops = append(drops, name)
		}
	}
	sort.Strings(creates)
	sort.Strings(drops)

	var ops []MigrationOp
	for _, name := range drops {
		ops = append(ops, DropIndexOp{Name: name, Table: meta.Name})
	}
	for _, name := range creates {
		d := desired[name]
		ops = append(ops, CreateIndexOp{
			Name:    name,
			Table:   meta.Name,
			Columns: d.columns,
			Unique:  d.unique,
		})
	}
	return ops
}

func fieldToColumnDef(f model.FieldMeta) ColumnDef {
	sqlType := ""
	if v, ok := f.Tags["type"]; ok {
		sqlType = v
	}
	def := ""
	if v, ok := f.Tags["default"]; ok {
		def = v
	}
	return ColumnDef{
		Name:       f.Column,
		SQLType:    sqlType,
		PrimaryKey: f.PrimaryKey,
		AutoIncr:   f.AutoIncr,
		Nullable:   f.Nullable,
		Default:    def,
		Index:      f.Index,
		Indexed:    f.Indexed,
		Unique:     f.Unique,
		GoType:     f.GoType,
	}
}

// MetaToSnapshot converts a list of EntityMeta into a DatabaseSchema
// for use as the "current" snapshot.
func MetaToSnapshot(metas []*model.EntityMeta) DatabaseSchema {
	schema := DatabaseSchema{Tables: make(map[string]*TableSchema, len(metas))}
	for _, meta := range metas {
		ts := &TableSchema{
			Name:    meta.Name,
			Columns: make(map[string]*ColumnDef, len(meta.Fields)),
		}
		for _, f := range meta.Fields {
			cd := fieldToColumnDef(f)
			ts.Columns[f.Column] = &cd
		}
		schema.Tables[meta.Name] = ts
	}
	return schema
}

// Ensure reflect is used (for GoType in ColumnDef)
var _ = reflect.TypeOf
