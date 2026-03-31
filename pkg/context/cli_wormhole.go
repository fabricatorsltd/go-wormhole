//go:build wormhole_cli

package context

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/nosqlmigrations"
)

// runCLIIfEnabled intercepts execution when the wormhole_cli build tag is set.
// It parses os.Args and runs the wormhole CLI against the current project, then exits.
// This mirrors the pattern used in the Vanilla OS SDK for compile-time feature hooks.
func (c *DbContext) runCLIIfEnabled() {
	if len(os.Args) < 2 {
		cliUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "migrations":
		if len(os.Args) < 3 {
			cliUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "add":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: <app> migrations add <Name>")
				os.Exit(1)
			}
			cliMigrationsAdd(os.Args[3])
		case "list":
			cliMigrationsList()
		case "script":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: <app> migrations script <Name> [dialect]")
				os.Exit(1)
			}
			dialect := "default"
			if len(os.Args) > 4 {
				dialect = os.Args[4]
			}
			cliMigrationsScript(os.Args[3], dialect)
		default:
			cliUsage()
			os.Exit(1)
		}
	case "database":
		if len(os.Args) < 3 {
			cliUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "update":
			cliDatabaseUpdate()
		default:
			cliUsage()
			os.Exit(1)
		}
	case "nosql-migrations":
		if len(os.Args) < 3 {
			cliUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "add":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: <app> nosql-migrations add <Name>")
				os.Exit(1)
			}
			cliNoSQLMigrationsAdd(os.Args[3])
		case "list":
			cliNoSQLMigrationsList()
		case "apply":
			cliNoSQLMigrationsApply()
		default:
			cliUsage()
			os.Exit(1)
		}
	default:
		cliUsage()
		os.Exit(1)
	}

	os.Exit(0)
}

func cliUsage() {
	fmt.Fprintln(os.Stderr, `wormhole CLI (embedded via -tags wormhole_cli)

Usage:
  <app> migrations add <Name>               Generate a new migration from model diff
  <app> migrations script <Name> [dialect]  Export migration as a .sql file
  <app> migrations list                     List pending migrations
  <app> database update                     Apply pending migrations
  <app> nosql-migrations add <Name>         Generate a NoSQL evolution script
  <app> nosql-migrations list               List NoSQL evolution scripts
  <app> nosql-migrations apply              Apply pending NoSQL evolution scripts

Dialects: default, postgres, mysql, mssql

Environment Variables:
  WORMHOLE_DSN               Database connection string (required for database commands)
  WORMHOLE_DRIVER            SQL driver name (default: sqlite)
  WORMHOLE_DIR               Migrations directory (default: ./migrations)
  WORMHOLE_NOSQL_DIR         NoSQL scripts directory (default: ./nosql-migrations)`)
}

func cliDir() string {
	if d := os.Getenv("WORMHOLE_DIR"); d != "" {
		return d
	}
	return "./migrations"
}

func cliNosqlDir() string {
	if d := os.Getenv("WORMHOLE_NOSQL_DIR"); d != "" {
		return d
	}
	return "./nosql-migrations"
}

func cliResolveDialect(name string) migrations.Dialect {
	switch strings.ToLower(name) {
	case "postgres", "pg":
		return migrations.PostgresDialect{}
	case "mysql":
		return migrations.MySQLDialect{}
	case "mssql", "sqlserver":
		return migrations.MSSQLDialect{}
	default:
		return migrations.DefaultDialect{}
	}
}

func cliMigrationsAdd(name string) {
	dir := cliDir()

	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering models: %v\n", err)
		os.Exit(1)
	}

	if len(models) == 0 {
		fmt.Fprintln(os.Stderr, "No models with `db` tags found in the current directory.")
		os.Exit(1)
	}

	current := migrations.DatabaseSchema{Tables: make(map[string]*migrations.TableSchema)}
	ops := migrations.ComputeDiff(models, current)

	if len(ops) == 0 {
		fmt.Println("No changes detected. Schema is up to date.")
		return
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating migrations directory: %v\n", err)
		os.Exit(1)
	}

	goContent := migrations.GenerateMigrationFile(name, ops)
	goPath := filepath.Join(dir, migrations.MigrationFileName(name))
	if err := os.WriteFile(goPath, []byte(goContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing migration file: %v\n", err)
		os.Exit(1)
	}

	sqlContent := migrations.GenerateSQLScript(ops, migrations.DefaultDialect{})
	sqlPath := filepath.Join(dir, migrations.SQLScriptFileName(name))
	if err := os.WriteFile(sqlPath, []byte(sqlContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing SQL script: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created migration %q in %s (%d operation(s))\n", name, dir, len(ops))
	fmt.Printf("  Go:  %s\n", goPath)
	fmt.Printf("  SQL: %s\n", sqlPath)
}

func cliMigrationsList() {
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}

	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "Error: WORMHOLE_DSN is required")
		os.Exit(1)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := stdctx.Background()
	d := cliResolveDialect(driver)
	runner := migrations.NewRunner(db, d)

	pending, err := runner.Pending(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing migrations: %v\n", err)
		os.Exit(1)
	}

	if len(pending) == 0 {
		fmt.Println("No pending migrations.")
		return
	}

	fmt.Printf("%d pending migration(s):\n", len(pending))
	for _, id := range pending {
		fmt.Printf("  [pending] %s\n", id)
	}
}

func cliMigrationsScript(name, dialectName string) {
	dir := cliDir()
	dialect := cliResolveDialect(dialectName)

	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering models: %v\n", err)
		os.Exit(1)
	}

	current := migrations.DatabaseSchema{Tables: make(map[string]*migrations.TableSchema)}
	ops := migrations.ComputeDiff(models, current)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}

	sqlContent := migrations.GenerateSQLScript(ops, dialect)
	sqlPath := filepath.Join(dir, migrations.SQLScriptFileName(name))
	if err := os.WriteFile(sqlPath, []byte(sqlContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing SQL script: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created SQL script: %s\n", sqlPath)
}

func cliDatabaseUpdate() {
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}

	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "Error: WORMHOLE_DSN is required")
		os.Exit(1)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := stdctx.Background()
	d := cliResolveDialect(driver)
	runner := migrations.NewRunner(db, d)

	if err := runner.Up(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error applying migrations: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Database updated successfully.")
}

func cliNoSQLMigrationsAdd(name string) {
	dir := cliNosqlDir()

	script := nosqlmigrations.GenerateTemplate(name)
	path, err := nosqlmigrations.SaveScript(dir, script)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error saving script: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created NoSQL migration: %s\n", path)
}

func cliNoSQLMigrationsList() {
	dir := cliNosqlDir()

	scripts, err := nosqlmigrations.LoadScripts(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading scripts: %v\n", err)
		os.Exit(1)
	}

	if len(scripts) == 0 {
		fmt.Println("No NoSQL migration scripts found.")
		return
	}

	history := nosqlmigrations.NewFileHistoryStore(filepath.Join(dir, ".history.json"))
	applied, err := history.AppliedSet(stdctx.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading history: %v\n", err)
		os.Exit(1)
	}

	for _, s := range scripts {
		status := "pending"
		if applied[s.ID] {
			status = "applied"
		}
		fmt.Printf("  [%s] %s\n", status, s.ID)
	}
}

func cliNoSQLMigrationsApply() {
	fmt.Fprintln(os.Stderr, "Error: nosql-migrations apply requires a provider-specific executor.")
	fmt.Fprintln(os.Stderr, "Use nosqlmigrations.NewRunner with your provider to apply scripts programmatically.")
	os.Exit(1)
}
