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

	for _, meta := range targets {
		targetNames[meta.Name] = true
		existing, exists := current.Tables[meta.Name]

		if !exists {
			// Whole table is new
			ops = append(ops, createTableFromMeta(meta))
			// Add indexes for new table
			ops = append(ops, indexOpsForMeta(meta)...)
			continue
		}

		// Table exists — diff columns
		ops = append(ops, diffColumns(meta, existing)...)
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

func createTableFromMeta(meta *model.EntityMeta) CreateTableOp {
	cols := make([]ColumnDef, 0, len(meta.Fields))
	for _, f := range meta.Fields {
		cols = append(cols, fieldToColumnDef(f))
	}
	return CreateTableOp{Table: meta.Name, Columns: cols}
}

func diffColumns(meta *model.EntityMeta, table *TableSchema) []MigrationOp {
	var ops []MigrationOp

	codeFields := make(map[string]bool, len(meta.Fields))

	for _, f := range meta.Fields {
		codeFields[f.Column] = true
		existing, exists := table.Columns[f.Column]

		if !exists {
			// New column
			ops = append(ops, AddColumnOp{
				Table:  meta.Name,
				Column: fieldToColumnDef(f),
			})
			continue
		}

		// Column exists — check for type/constraint changes
		newDef := fieldToColumnDef(f)
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
		if f.Index != "" {
			ops = append(ops, CreateIndexOp{
				Name:    f.Index,
				Table:   meta.Name,
				Columns: []string{f.Column},
				Unique:  false,
			})
		}
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
