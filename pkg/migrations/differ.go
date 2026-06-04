package migrations

import (
	"reflect"
	"sort"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

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
		// First declaration wins; avoids duplicate FKs when both sides of a
		// relationship are modeled (BelongsTo + OneToMany).
		if _, exists := out[table][col]; !exists {
			out[table][col] = ref
		}
	}

	for _, owner := range targets {
		for _, rel := range owner.Relations {
			switch rel.Kind {
			case model.RelationBelongsTo:
				// FK on the owner referencing the target's primary key.
				put(owner.Name, rel.LocalKey, &ColumnRef{
					Table:  rel.TargetEntity,
					Column: pkColumn(rel.TargetEntity, rel.ForeignKey),
				})
			case model.RelationOneToMany, model.RelationOneToOne:
				// FK on the related table referencing the owner's local key.
				put(rel.TargetEntity, rel.ForeignKey, &ColumnRef{
					Table:  owner.Name,
					Column: rel.LocalKey,
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

// indexDef is a resolved single-column index used for diffing.
type indexDef struct {
	name   string
	column string
	unique bool
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
func modelIndexes(meta *model.EntityMeta) map[string]indexDef {
	out := map[string]indexDef{}
	for _, f := range meta.Fields {
		if f.Index == "" && !f.Indexed && !f.Unique {
			continue
		}
		n := indexName(meta.Name, f.Column, f.Index, f.Unique)
		out[n] = indexDef{name: n, column: f.Column, unique: f.Unique}
	}
	return out
}

// snapshotIndexes returns the indexes a snapshot table records, keyed by name.
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
		out[n] = indexDef{name: n, column: c.Name, unique: c.Unique}
	}
	return out
}

// diffIndexOps emits CreateIndex/DropIndex only for indexes that actually
// changed between the snapshot and the model, so an unchanged indexed model
// diffs to zero ops (no spurious re-create). Output is sorted for determinism.
func diffIndexOps(meta *model.EntityMeta, existing *TableSchema) []MigrationOp {
	desired := modelIndexes(meta)
	current := snapshotIndexes(existing)

	var creates, drops []string
	for name, d := range desired {
		if c, ok := current[name]; !ok || c.unique != d.unique {
			creates = append(creates, name)
			if ok && c.unique != d.unique {
				drops = append(drops, name) // recreate with new uniqueness
			}
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
			Columns: []string{d.column},
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
