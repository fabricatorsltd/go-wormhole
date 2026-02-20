package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ScaffoldResult holds the generated Go source for a single table.
type ScaffoldResult struct {
	TableName  string
	StructName string
	Source     string
}

// Scaffold connects to a database, reads information_schema, and generates
// Go struct source code with db:"..." tags for every user table.
func Scaffold(ctx context.Context, db *sql.DB) ([]ScaffoldResult, error) {
	tables, err := scaffoldTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	var results []ScaffoldResult
	for _, table := range tables {
		cols, err := scaffoldColumns(ctx, db, table)
		if err != nil {
			return nil, fmt.Errorf("columns for %s: %w", table, err)
		}
		src := generateStruct(table, cols)
		results = append(results, ScaffoldResult{
			TableName:  table,
			StructName: toPascal(table),
			Source:     src,
		})
	}
	return results, nil
}

type scaffoldCol struct {
	Name     string
	DataType string
	Nullable bool
	IsPK     bool
	Default  sql.NullString
}

// scaffoldTables reads user tables from information_schema.
// Falls back to sqlite_master for SQLite.
func scaffoldTables(ctx context.Context, db *sql.DB) ([]string, error) {
	// Try information_schema first (Postgres, MySQL)
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
		   AND table_type = 'BASE TABLE'
		 ORDER BY table_name`)
	if err == nil {
		defer rows.Close()
		return collectStrings(rows)
	}

	// Fallback: SQLite
	rows, err = db.QueryContext(ctx,
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\_%' ESCAPE '\'
		 ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectStrings(rows)
}

func collectStrings(rows *sql.Rows) ([]string, error) {
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// scaffoldColumns reads column metadata from information_schema.
// Falls back to PRAGMA table_info for SQLite.
func scaffoldColumns(ctx context.Context, db *sql.DB, table string) ([]scaffoldCol, error) {
	// Try information_schema first
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`, table)
	if err == nil {
		defer rows.Close()
		return scanInfoSchemaCols(rows, ctx, db, table)
	}

	// Fallback: SQLite PRAGMA
	rows, err = db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPragmaCols(rows)
}

func scanInfoSchemaCols(rows *sql.Rows, ctx context.Context, db *sql.DB, table string) ([]scaffoldCol, error) {
	pkCols := primaryKeyCols(ctx, db, table)

	var cols []scaffoldCol
	for rows.Next() {
		var c scaffoldCol
		var nullable string
		if err := rows.Scan(&c.Name, &c.DataType, &nullable, &c.Default); err != nil {
			return nil, err
		}
		c.Nullable = strings.EqualFold(nullable, "YES")
		c.IsPK = pkCols[c.Name]
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func scanPragmaCols(rows *sql.Rows) ([]scaffoldCol, error) {
	var cols []scaffoldCol
	for rows.Next() {
		var cid int
		var name, dtype string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &dtype, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, scaffoldCol{
			Name:     name,
			DataType: dtype,
			Nullable: notNull == 0,
			IsPK:     pk == 1,
			Default:  dflt,
		})
	}
	return cols, rows.Err()
}

func primaryKeyCols(ctx context.Context, db *sql.DB, table string) map[string]bool {
	m := make(map[string]bool)
	rows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'`, table)
	if err != nil {
		return m
	}
	defer rows.Close()
	for rows.Next() {
		var col string
		_ = rows.Scan(&col)
		m[col] = true
	}
	return m
}

// generateStruct produces Go source for a single table.
func generateStruct(table string, cols []scaffoldCol) string {
	structName := toPascal(table)
	var b strings.Builder

	b.WriteString(fmt.Sprintf("type %s struct {\n", structName))
	for _, c := range cols {
		goName := toPascal(c.Name)
		goType := sqlTypeToGo(c.DataType, c.Nullable)
		tag := buildTag(c)
		b.WriteString(fmt.Sprintf("\t%s %s `db:\"%s\"`\n", goName, goType, tag))
	}
	b.WriteString("}\n")
	return b.String()
}

func buildTag(c scaffoldCol) string {
	var parts []string
	parts = append(parts, "column:"+c.Name)
	if c.DataType != "" {
		parts = append(parts, "type:"+strings.ToLower(c.DataType))
	}
	if c.IsPK {
		parts = append(parts, "primary_key")
	}
	if c.Nullable {
		parts = append(parts, "nullable")
	}
	if c.Default.Valid && c.Default.String != "" {
		parts = append(parts, "default:"+c.Default.String)
	}
	return strings.Join(parts, "; ")
}

func sqlTypeToGo(sqlType string, nullable bool) string {
	upper := strings.ToUpper(sqlType)

	// Strip length specifiers
	if idx := strings.Index(upper, "("); idx >= 0 {
		upper = upper[:idx]
	}
	upper = strings.TrimSpace(upper)

	base := "string"
	switch upper {
	case "INTEGER", "INT", "INT4", "SMALLINT", "MEDIUMINT", "TINYINT":
		base = "int"
	case "BIGINT", "INT8":
		base = "int64"
	case "REAL", "FLOAT", "FLOAT4":
		base = "float32"
	case "DOUBLE PRECISION", "DOUBLE", "FLOAT8", "NUMERIC", "DECIMAL":
		base = "float64"
	case "BOOLEAN", "BOOL":
		base = "bool"
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER VARYING", "BPCHAR", "CITEXT":
		base = "string"
	case "BYTEA", "BLOB":
		base = "[]byte"
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "DATETIME":
		base = "time.Time"
	case "SERIAL":
		base = "int"
	case "BIGSERIAL":
		base = "int64"
	}

	if nullable && base != "[]byte" {
		switch base {
		case "int":
			return "sql.NullInt64"
		case "int64":
			return "sql.NullInt64"
		case "float32", "float64":
			return "sql.NullFloat64"
		case "bool":
			return "sql.NullBool"
		case "string":
			return "sql.NullString"
		case "time.Time":
			return "sql.NullTime"
		}
	}
	return base
}

// toPascal converts snake_case to PascalCase.
func toPascal(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		// Common abbreviations stay uppercase
		upper := strings.ToUpper(p)
		switch upper {
		case "ID", "URL", "API", "HTTP", "UUID", "SQL", "JSON", "XML", "HTML", "CSS", "IP":
			b.WriteString(upper)
		default:
			b.WriteString(strings.ToUpper(p[:1]))
			b.WriteString(strings.ToLower(p[1:]))
		}
	}
	return b.String()
}
