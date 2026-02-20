package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mirkobrombin/go-wormhole/pkg/migrations"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/schema"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "migrations":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "add":
			cmdMigrationsAdd()
		case "list":
			cmdMigrationsList()
		default:
			printUsage()
			os.Exit(1)
		}
	case "database":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "update":
			cmdDatabaseUpdate()
		default:
			printUsage()
			os.Exit(1)
		}
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage:
  wormhole migrations add <Name>   Generate a new migration from model diff
  wormhole migrations list         List pending migrations
  wormhole database update         Apply pending migrations

Environment:
  WORMHOLE_DSN      Database connection string (required for database commands)
  WORMHOLE_DRIVER   SQL driver name (default: sqlite3)
  WORMHOLE_DIR      Migrations directory (default: ./migrations)`)
}

func cmdMigrationsAdd() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "Usage: wormhole migrations add <Name>")
		os.Exit(1)
	}
	name := os.Args[3]
	dir := migrationsDir()

	// Load registered models
	models := loadModels()
	if len(models) == 0 {
		fmt.Fprintln(os.Stderr, "No models registered. Call schema.Parse() on your entities first.")
		os.Exit(1)
	}

	// Load current snapshot (from previous migrations or empty)
	current := loadSnapshot(dir)

	// Compute diff
	ops := migrations.ComputeDiff(models, current)
	if len(ops) == 0 {
		fmt.Println("No changes detected.")
		return
	}

	// Warn about destructive operations
	for _, op := range ops {
		switch o := op.(type) {
		case migrations.DropTableOp:
			fmt.Fprintf(os.Stderr, "\033[33mWARNING: This migration drops table %q — potential data loss!\033[0m\n", o.Table)
		case migrations.DropColumnOp:
			fmt.Fprintf(os.Stderr, "\033[33mWARNING: This migration drops column %q.%q — potential data loss!\033[0m\n", o.Table, o.Column)
		}
	}

	// Generate file
	source := migrations.GenerateMigrationFile(name, ops)
	fileName := migrations.MigrationFileName(name)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created migration: %s\n", path)
	fmt.Printf("Operations: %d\n", len(ops))
	for _, op := range ops {
		fmt.Printf("  - %s\n", describeOp(op))
	}
}

func cmdMigrationsList() {
	db := openDB()
	defer db.Close()

	ctx := context.Background()
	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "ensure history: %v\n", err)
		os.Exit(1)
	}

	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read history: %v\n", err)
		os.Exit(1)
	}

	dir := migrationsDir()
	files := listMigrationFiles(dir)

	if len(files) == 0 {
		fmt.Println("No migration files found.")
		return
	}

	for _, f := range files {
		id := strings.TrimSuffix(f, ".go")
		status := "pending"
		if applied[id] {
			status = "applied"
		}
		fmt.Printf("  [%s] %s\n", status, id)
	}
}

func cmdDatabaseUpdate() {
	db := openDB()
	defer db.Close()

	ctx := context.Background()

	// The actual migration execution needs compiled Go migration files.
	// For the CLI tool, we create a runner and expect migrations to be
	// registered via init() in the migrations package.
	// In practice, users compile their app with migration files included.

	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "ensure history: %v\n", err)
		os.Exit(1)
	}

	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read history: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Applied migrations: %d\n", len(applied))
	fmt.Println("Run your application with migration files compiled in to apply pending migrations.")
	fmt.Println("See: migrations.NewRunner(db).Up(ctx)")
}

// --- helpers ---

func migrationsDir() string {
	if d := os.Getenv("WORMHOLE_DIR"); d != "" {
		return d
	}
	return "./migrations"
}

func openDB() *sql.DB {
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite3"
	}
	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "WORMHOLE_DSN is required")
		os.Exit(1)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	return db
}

func loadModels() []*model.EntityMeta {
	// In a real scenario, models are registered via schema.Parse().
	// The CLI reads from the schema cache. For now, return empty
	// if no models have been parsed in this process.
	_ = schema.Parse
	return nil
}

func loadSnapshot(_ string) migrations.DatabaseSchema {
	// For the initial version, start with an empty schema (greenfield).
	// Future: parse previously generated migration files to reconstruct
	// the cumulative schema state.
	return migrations.DatabaseSchema{Tables: make(map[string]*migrations.TableSchema)}
}

func listMigrationFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files
}

func describeOp(op migrations.MigrationOp) string {
	switch o := op.(type) {
	case migrations.CreateTableOp:
		return fmt.Sprintf("CreateTable(%s) [%d columns]", o.Table, len(o.Columns))
	case migrations.DropTableOp:
		return fmt.Sprintf("DropTable(%s)", o.Table)
	case migrations.AddColumnOp:
		return fmt.Sprintf("AddColumn(%s.%s)", o.Table, o.Column.Name)
	case migrations.DropColumnOp:
		return fmt.Sprintf("DropColumn(%s.%s)", o.Table, o.Column)
	case migrations.AlterColumnOp:
		return fmt.Sprintf("AlterColumn(%s.%s)", o.Table, o.Column.Name)
	case migrations.CreateIndexOp:
		return fmt.Sprintf("CreateIndex(%s on %s)", o.Name, o.Table)
	case migrations.DropIndexOp:
		return fmt.Sprintf("DropIndex(%s)", o.Name)
	default:
		return fmt.Sprintf("%T", op)
	}
}
