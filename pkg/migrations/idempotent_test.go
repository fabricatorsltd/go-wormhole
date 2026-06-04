package migrations_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

func idempotentOps() []migrations.MigrationOp {
	return []migrations.MigrationOp{
		migrations.RawSQLOp{SQL: "CREATE TABLE widget (id INT)"},
	}
}

// PostgreSQL wraps the migration in an anonymous DO block guarded by a history
// check and records the migration inside the same block.
func TestGenerateIdempotent_Postgres(t *testing.T) {
	out, err := migrations.GenerateIdempotentSQLScript("0001_init", idempotentOps(), migrations.PostgresDialect{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "_wormhole_migrations_history"`, // self-contained on a fresh DB
		"DO $$",
		`IF NOT EXISTS (SELECT 1 FROM "_wormhole_migrations_history" WHERE "migration_id" = '0001_init')`,
		"CREATE TABLE widget (id INT);",
		`INSERT INTO "_wormhole_migrations_history" ("migration_id") VALUES ('0001_init');`,
		"END IF;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("postgres idempotent script missing %q\n---\n%s", want, out)
		}
	}
}

// SQL Server uses a top-level IF block guarded by the same history check.
func TestGenerateIdempotent_MSSQL(t *testing.T) {
	out, err := migrations.GenerateIdempotentSQLScript("0001_init", idempotentOps(), migrations.MSSQLDialect{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"IF OBJECT_ID(N'_wormhole_migrations_history', N'U') IS NULL", // self-contained on a fresh DB
		"IF NOT EXISTS (SELECT 1 FROM [_wormhole_migrations_history] WHERE [migration_id] = '0001_init')",
		"BEGIN",
		"INSERT INTO [_wormhole_migrations_history] ([migration_id]) VALUES ('0001_init');",
		"END;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("mssql idempotent script missing %q\n---\n%s", want, out)
		}
	}
}

// SQLite and MySQL have no procedural IF outside a routine, so an idempotent
// script cannot be expressed and the generator reports it rather than emitting
// an unguarded script.
func TestGenerateIdempotent_Unsupported(t *testing.T) {
	for _, d := range []migrations.Dialect{migrations.DefaultDialect{}, migrations.MySQLDialect{}} {
		if _, err := migrations.GenerateIdempotentSQLScript("0001_init", idempotentOps(), d); err == nil ||
			!strings.Contains(err.Error(), "not supported") {
			t.Errorf("%T: want unsupported error, got %v", d, err)
		}
	}
}

// Registered (code) migrations render their statements through the builder and
// are then guarded per migration.
func TestScriptMigrationsIdempotent_Postgres(t *testing.T) {
	migs := []migrations.Migration{{
		ID: "0001_init",
		Up: func(b *migrations.SchemaBuilder) {
			b.AddOp(migrations.RawSQLOp{SQL: "CREATE TABLE widget (id INT)"})
		},
	}}
	out, err := migrations.ScriptMigrationsIdempotent(migs, migrations.PostgresDialect{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "DO $$") || !strings.Contains(out, "CREATE TABLE widget (id INT);") ||
		!strings.Contains(out, "'0001_init'") {
		t.Errorf("registered idempotent script wrong:\n%s", out)
	}

	if _, err := migrations.ScriptMigrationsIdempotent(migs, migrations.DefaultDialect{}); err == nil ||
		!strings.Contains(err.Error(), "not supported") {
		t.Errorf("sqlite: want unsupported error, got %v", err)
	}
}

// A migration id containing a single quote is escaped so it cannot break out of
// the string literal in the guard.
func TestGenerateIdempotent_EscapesQuote(t *testing.T) {
	out, err := migrations.GenerateIdempotentSQLScript("o'brien", idempotentOps(), migrations.PostgresDialect{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "'o''brien'") {
		t.Errorf("single quote not escaped: %s", out)
	}
}
