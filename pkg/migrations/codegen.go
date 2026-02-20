package migrations

import (
	"fmt"
	"strings"
	"time"
)

// GenerateMigrationFile produces the Go source code for a timestamped
// migration file with Up() and Down() methods, pre-populated from
// the diff operations.
func GenerateMigrationFile(name string, ops []MigrationOp) string {
	ts := time.Now().UTC().Format("20060102150405")
	migrationID := ts + "_" + toSnake(name)

	var up strings.Builder
	var down strings.Builder

	for _, op := range ops {
		writeUpOp(&up, op)
		writeDownOp(&down, op)
	}

	return fmt.Sprintf(`package migrations

import "github.com/mirkobrombin/go-wormhole/pkg/migrations"

func init() {
	Register(migrations.Migration{
		ID: %q,
		Up: func(b *migrations.SchemaBuilder) {
%s		},
		Down: func(b *migrations.SchemaBuilder) {
%s		},
	})
}
`, migrationID, up.String(), down.String())
}

// MigrationFileName returns the file name for a migration.
func MigrationFileName(name string) string {
	ts := time.Now().UTC().Format("20060102150405")
	return ts + "_" + toSnake(name) + ".go"
}

func writeUpOp(w *strings.Builder, op MigrationOp) {
	switch o := op.(type) {
	case CreateTableOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.CreateTable(%q", o.Table))
		for _, c := range o.Columns {
			w.WriteString(",\n\t\t\t\t")
			writeColumnLiteral(w, c)
		}
		w.WriteString(",\n\t\t\t)\n")

	case DropTableOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropTable(%q)\n", o.Table))

	case AddColumnOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.AddColumn(%q, ", o.Table))
		writeColumnLiteral(w, o.Column)
		w.WriteString(")\n")

	case DropColumnOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropColumn(%q, %q)\n", o.Table, o.Column))

	case AlterColumnOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.AlterColumn(%q, ", o.Table))
		writeColumnLiteral(w, o.Column)
		w.WriteString(")\n")

	case CreateIndexOp:
		cols := make([]string, len(o.Columns))
		for i, c := range o.Columns {
			cols[i] = fmt.Sprintf("%q", c)
		}
		w.WriteString(fmt.Sprintf("\t\t\tb.CreateIndex(%q, %q, %v, %s)\n",
			o.Name, o.Table, o.Unique, strings.Join(cols, ", ")))

	case DropIndexOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropIndex(%q)\n", o.Name))
	}
}

func writeDownOp(w *strings.Builder, op MigrationOp) {
	// Reverse operations
	switch o := op.(type) {
	case CreateTableOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropTable(%q)\n", o.Table))

	case DropTableOp:
		// Cannot fully reverse a drop — generate placeholder
		w.WriteString(fmt.Sprintf("\t\t\t// TODO: recreate table %q\n", o.Table))

	case AddColumnOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropColumn(%q, %q)\n", o.Table, o.Column.Name))

	case DropColumnOp:
		// Cannot fully reverse — placeholder
		w.WriteString(fmt.Sprintf("\t\t\t// TODO: re-add column %q to %q\n", o.Column, o.Table))

	case AlterColumnOp:
		w.WriteString(fmt.Sprintf("\t\t\t// TODO: revert column %q in %q\n", o.Column.Name, o.Table))

	case CreateIndexOp:
		w.WriteString(fmt.Sprintf("\t\t\tb.DropIndex(%q)\n", o.Name))

	case DropIndexOp:
		w.WriteString(fmt.Sprintf("\t\t\t// TODO: recreate index %q\n", o.Name))
	}
}

func writeColumnLiteral(w *strings.Builder, c ColumnDef) {
	w.WriteString("migrations.ColumnDef{")
	w.WriteString(fmt.Sprintf("Name: %q", c.Name))
	if c.SQLType != "" {
		w.WriteString(fmt.Sprintf(", SQLType: %q", c.SQLType))
	}
	if c.PrimaryKey {
		w.WriteString(", PrimaryKey: true")
	}
	if c.AutoIncr {
		w.WriteString(", AutoIncr: true")
	}
	if c.Nullable {
		w.WriteString(", Nullable: true")
	}
	if c.Default != "" {
		w.WriteString(fmt.Sprintf(", Default: %q", c.Default))
	}
	w.WriteString("}")
}

// toSnake converts CamelCase/spaces to snake_case for file names.
func toSnake(s string) string {
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 && s[i-1] != '_' {
				b.WriteByte('_')
			}
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
