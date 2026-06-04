package migrations

import (
	"context"
	"database/sql"
	"sort"
	"strings"
)

// IntrospectSchema reads the live database structure into a DatabaseSchema,
// reusing Scaffold's introspection. It supports PostgreSQL (information_schema)
// and SQLite (PRAGMA); MySQL/MSSQL are not yet supported (the introspection
// query uses a Postgres-style placeholder) and return an error rather than a
// misleading result. The migrations history table is excluded. The result is
// comparable against a saved model snapshot to detect drift.
func IntrospectSchema(ctx context.Context, db *sql.DB) (DatabaseSchema, error) {
	tables, err := scaffoldTables(ctx, db)
	if err != nil {
		return DatabaseSchema{}, err
	}

	schema := DatabaseSchema{Tables: make(map[string]*TableSchema, len(tables))}
	for _, t := range tables {
		if t == historyTable {
			continue
		}
		cols, err := scaffoldColumns(ctx, db, t)
		if err != nil {
			return DatabaseSchema{}, err
		}
		ts := &TableSchema{Name: t, Columns: make(map[string]*ColumnDef, len(cols))}
		for _, c := range cols {
			def := ""
			if c.Default.Valid {
				def = c.Default.String
			}
			ts.Columns[c.Name] = &ColumnDef{
				Name:       c.Name,
				SQLType:    strings.ToUpper(c.DataType),
				PrimaryKey: c.IsPK,
				Nullable:   c.Nullable,
				Default:    def,
			}
		}
		schema.Tables[t] = ts
	}
	return schema, nil
}

// DriftKind classifies a difference between the snapshot and the live database.
type DriftKind string

const (
	DriftMissingTable  DriftKind = "missing_table"  // in snapshot, absent in DB
	DriftExtraTable    DriftKind = "extra_table"    // in DB, absent in snapshot
	DriftMissingColumn DriftKind = "missing_column" // in snapshot, absent in DB
	DriftExtraColumn   DriftKind = "extra_column"   // in DB, absent in snapshot
	DriftColumnType    DriftKind = "column_type"    // type bucket changed
)

// Drift is a single difference between the expected (snapshot) schema and the
// live database.
type Drift struct {
	Kind   DriftKind
	Table  string
	Column string // empty for table-level drift
	Want   string // expected type (snapshot side), for column_type
	Got    string // actual type (database side), for column_type
}

func (d Drift) String() string {
	switch d.Kind {
	case DriftMissingTable:
		return "table " + d.Table + " is missing from the database"
	case DriftExtraTable:
		return "table " + d.Table + " exists in the database but not the snapshot"
	case DriftMissingColumn:
		return "column " + d.Table + "." + d.Column + " is missing from the database"
	case DriftExtraColumn:
		return "column " + d.Table + "." + d.Column + " exists in the database but not the snapshot"
	case DriftColumnType:
		return "column " + d.Table + "." + d.Column + " type changed (snapshot " + d.Want + ", database " + d.Got + ")"
	default:
		return string(d.Kind)
	}
}

// DetectDrift compares a saved model snapshot against the live database schema
// and reports the differences. Structural drift (missing/extra tables and
// columns) is reported exactly. Type drift is reported only when the columns
// map to a different Go-type bucket, so dialect spelling differences (e.g.
// TIMESTAMP vs "timestamp with time zone") do not raise false positives.
// Nullability, defaults, and type length/precision are not compared (a
// VARCHAR(50) -> VARCHAR(500) change is not drift). Output is sorted for stable,
// reviewable reporting.
func DetectDrift(snapshot, live DatabaseSchema) []Drift {
	var drifts []Drift

	for name, want := range snapshot.Tables {
		got, ok := live.Tables[name]
		if !ok {
			drifts = append(drifts, Drift{Kind: DriftMissingTable, Table: name})
			continue
		}
		for col, wc := range want.Columns {
			gc, ok := got.Columns[col]
			if !ok {
				drifts = append(drifts, Drift{Kind: DriftMissingColumn, Table: name, Column: col})
				continue
			}
			if typeBucket(wc.SQLType) != typeBucket(gc.SQLType) {
				drifts = append(drifts, Drift{
					Kind: DriftColumnType, Table: name, Column: col,
					Want: wc.SQLType, Got: gc.SQLType,
				})
			}
		}
		for col := range got.Columns {
			if _, ok := want.Columns[col]; !ok {
				drifts = append(drifts, Drift{Kind: DriftExtraColumn, Table: name, Column: col})
			}
		}
	}

	for name := range live.Tables {
		if _, ok := snapshot.Tables[name]; !ok {
			drifts = append(drifts, Drift{Kind: DriftExtraTable, Table: name})
		}
	}

	sort.Slice(drifts, func(i, j int) bool {
		if drifts[i].Table != drifts[j].Table {
			return drifts[i].Table < drifts[j].Table
		}
		if drifts[i].Column != drifts[j].Column {
			return drifts[i].Column < drifts[j].Column
		}
		return drifts[i].Kind < drifts[j].Kind
	})
	return drifts
}

// typeBucket maps a SQL type to its coarse Go-type bucket, so equivalent types
// across dialects (TIMESTAMP/TIMESTAMPTZ, TEXT/VARCHAR, INTEGER/INT) compare
// equal and only genuine type changes register as drift. An empty type is its
// own bucket so a snapshot column with a type never silently matches one without.
func typeBucket(sqlType string) string {
	if strings.TrimSpace(sqlType) == "" {
		return ""
	}
	return sqlTypeToGo(sqlType, false)
}
