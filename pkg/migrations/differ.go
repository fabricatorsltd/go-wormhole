package migrations

import (
	"reflect"

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
			// Add indexes for new table
			ops = append(ops, indexOpsForMeta(meta)...)
			continue
		}

		// Table exists — diff columns
		ops = append(ops, diffColumns(meta, existing, fks[meta.Name])...)
		ops = append(ops, indexOpsForMeta(meta)...)
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

func indexOpsForMeta(meta *model.EntityMeta) []MigrationOp {
	var ops []MigrationOp
	for _, f := range meta.Fields {
		if f.Index == "" && !f.Indexed && !f.Unique {
			continue
		}
		// Index names are schema-global in some engines (e.g. Postgres), so an
		// auto-derived name is table-qualified and deterministic.
		name := f.Index
		if name == "" {
			prefix := "idx"
			if f.Unique {
				prefix = "uniq"
			}
			name = prefix + "_" + meta.Name + "_" + f.Column
		}
		ops = append(ops, CreateIndexOp{
			Name:    name,
			Table:   meta.Name,
			Columns: []string{f.Column},
			Unique:  f.Unique,
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
