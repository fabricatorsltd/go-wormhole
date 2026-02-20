package migrations

import (
	"sort"
)

// RebuildSnapshot replays a sorted list of MigrationOp sequences
// (one per migration, in chronological order) to reconstruct the
// cumulative DatabaseSchema without connecting to the database.
func RebuildSnapshot(migrationOps [][]MigrationOp) DatabaseSchema {
	schema := DatabaseSchema{Tables: make(map[string]*TableSchema)}

	for _, ops := range migrationOps {
		for _, op := range ops {
			applyOp(&schema, op)
		}
	}
	return schema
}

// RebuildFromMigrations takes a list of Migration structs, sorts them
// by ID, calls each Up() to collect the ops, and rebuilds the schema.
func RebuildFromMigrations(migrations []Migration) DatabaseSchema {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].ID < migrations[j].ID
	})

	var allOps [][]MigrationOp
	for _, m := range migrations {
		b := NewBuilder()
		m.Up(b)
		allOps = append(allOps, b.Ops())
	}
	return RebuildSnapshot(allOps)
}

func applyOp(schema *DatabaseSchema, op MigrationOp) {
	switch o := op.(type) {
	case CreateTableOp:
		ts := &TableSchema{
			Name:    o.Table,
			Columns: make(map[string]*ColumnDef, len(o.Columns)),
		}
		for i := range o.Columns {
			c := o.Columns[i]
			ts.Columns[c.Name] = &c
		}
		schema.Tables[o.Table] = ts

	case DropTableOp:
		delete(schema.Tables, o.Table)

	case AddColumnOp:
		if ts, ok := schema.Tables[o.Table]; ok {
			c := o.Column
			ts.Columns[c.Name] = &c
		}

	case DropColumnOp:
		if ts, ok := schema.Tables[o.Table]; ok {
			delete(ts.Columns, o.Column)
		}

	case AlterColumnOp:
		if ts, ok := schema.Tables[o.Table]; ok {
			c := o.Column
			ts.Columns[c.Name] = &c
		}

	case CreateIndexOp:
		// Indexes are not tracked in the schema snapshot (yet)

	case DropIndexOp:
		// Indexes are not tracked in the schema snapshot (yet)
	}
}
