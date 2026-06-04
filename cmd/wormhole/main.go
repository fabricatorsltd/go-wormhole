// Command wormhole is a standalone migration CLI, in the spirit of EF Core's
// dotnet ef. It operates on a project directory without compiling it: models
// are read from source, migrations are JSON manifests, and the model snapshot
// is the diff baseline.
//
//	wormhole [-C dir] migrations add <Name>
//	wormhole [-C dir] migrations list
//	wormhole [-C dir] migrations script <Name> [dialect]
//	wormhole [-C dir] database update
//
// add/list/script need no database and no compilation. database update applies
// the JSON manifests using a registered SQL driver; SQLite is bundled, so it
// works out of the box. For Postgres/MySQL, either run the in-project path
// (go run -tags wormhole_cli . database update, which uses your project's
// drivers) or build a wormhole binary that imports the driver.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

const snapshotFile = "schema_snapshot.json"

func main() {
	args := applyChdir(os.Args[1:])
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "migrations":
		if len(args) < 2 {
			printUsage()
			os.Exit(1)
		}
		switch args[1] {
		case "add":
			if len(args) < 3 {
				fatal("Usage: wormhole migrations add <Name>")
			}
			cmdAdd(args[2])
		case "list":
			cmdList()
		case "script":
			if len(args) < 3 {
				fatal("Usage: wormhole migrations script <Name> [dialect] [--idempotent]")
			}
			dialect := "default"
			idempotent := false
			for _, a := range args[3:] {
				if a == "--idempotent" {
					idempotent = true
				} else {
					dialect = a
				}
			}
			cmdScript(args[2], dialect, idempotent)
		default:
			printUsage()
			os.Exit(1)
		}
	case "database":
		if len(args) < 2 {
			printUsage()
			os.Exit(1)
		}
		switch args[1] {
		case "update":
			cmdUpdate()
		case "check":
			cmdCheck()
		default:
			printUsage()
			os.Exit(1)
		}
	default:
		printUsage()
		os.Exit(1)
	}
}

// applyChdir consumes a leading "-C <dir>" so commands run against another
// project directory; the rest of the args are returned unchanged.
func applyChdir(args []string) []string {
	if len(args) >= 2 && args[0] == "-C" {
		if err := os.Chdir(args[1]); err != nil {
			fatalf("cannot enter %q: %v", args[1], err)
		}
		return args[2:]
	}
	return args
}

func migrationsDir() string {
	if d := os.Getenv("WORMHOLE_DIR"); d != "" {
		return d
	}
	return "./migrations"
}

func resolveDialect(name string) migrations.Dialect {
	switch name {
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

func cmdAdd(name string) {
	dir := migrationsDir()

	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fatalf("discover models: %v", err)
	}
	if len(models) == 0 {
		fatal("no models with `db` tags found in the current directory")
	}

	if err := migrations.ValidateModels(models); err != nil {
		fatalf("%v", err)
	}

	snapPath := filepath.Join(dir, snapshotFile)
	current, err := migrations.LoadSnapshot(snapPath)
	if err != nil {
		fatalf("read snapshot: %v", err)
	}

	ops := migrations.ComputeDiff(models, current)
	if len(ops) == 0 {
		fmt.Println("No changes detected. Schema is up to date.")
		return
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatalf("create migrations dir: %v", err)
	}

	base := migrations.MigrationBaseName(name)

	// The .json manifest is the applied source; the .go is a readable mirror.
	manifest, err := migrations.MarshalMigration(base, ops, migrations.ReverseOps(ops))
	if err != nil {
		fatalf("encode migration: %v", err)
	}
	writeOrDie(filepath.Join(dir, base+".json"), manifest)
	writeOrDie(filepath.Join(dir, base+".go"), []byte(migrations.GenerateMigrationFileWithID(base, ops)))

	if err := migrations.WriteSnapshot(snapPath, migrations.MetaToSnapshot(models)); err != nil {
		fatalf("write snapshot: %v", err)
	}

	fmt.Printf("Created migration %s (%d operation(s)) in %s\n", base, len(ops), dir)
}

func cmdList() {
	migs, err := migrations.LoadMigrationDir(migrationsDir())
	if err != nil {
		fatalf("read migrations: %v", err)
	}
	if len(migs) == 0 {
		fmt.Println("No migrations found.")
		return
	}

	applied := appliedStatus()
	for _, m := range migs {
		if applied == nil {
			fmt.Printf("  %s\n", m.ID)
			continue
		}
		status := "pending"
		if applied[m.ID] {
			status = "applied"
		}
		fmt.Printf("  [%s] %s\n", status, m.ID)
	}
}

// appliedStatus best-effort reads the history table when a DSN and a bundled
// driver are available; otherwise it returns nil and list shows IDs only.
func appliedStatus() map[string]bool {
	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		return nil
	}
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil
	}
	defer db.Close()
	ctx := context.Background()
	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		return nil
	}
	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		return nil
	}
	return applied
}

func cmdScript(name, dialect string, idempotent bool) {
	dir := migrationsDir()
	var out string
	var err error
	if idempotent {
		out, err = migrations.ScriptFilesIdempotent(dir, resolveDialect(dialect))
	} else {
		out, err = migrations.ScriptFiles(dir, resolveDialect(dialect))
	}
	if err != nil {
		fatalf("render script: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatalf("create migrations dir: %v", err)
	}
	path := filepath.Join(dir, migrations.SQLScriptFileName(name))
	writeOrDie(path, []byte(out))
	fmt.Printf("Created SQL script: %s\n", path)
}

func cmdUpdate() {
	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		fatal("WORMHOLE_DSN is required for database update")
	}
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fatalf("driver %q is not available in this wormhole binary: %v\n"+
			"Run the in-project path instead: go run -tags wormhole_cli . database update", driver, err)
	}
	defer db.Close()

	applied, err := migrations.ApplyPendingFiles(context.Background(), db, resolveDialect(driver), migrationsDir())
	if err != nil {
		fatalf("apply migrations: %v", err)
	}
	if len(applied) == 0 {
		fmt.Println("Database is up to date.")
		return
	}
	for _, id := range applied {
		fmt.Printf("  Applied: %s\n", id)
	}
	fmt.Printf("Database updated (%d migration(s) applied).\n", len(applied))
}

func cmdCheck() {
	dsn := os.Getenv("WORMHOLE_DSN")
	if dsn == "" {
		fatal("WORMHOLE_DSN is required for database check")
	}
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}

	snap, err := migrations.LoadSnapshot(filepath.Join(migrationsDir(), snapshotFile))
	if err != nil {
		fatalf("read snapshot: %v", err)
	}
	if len(snap.Tables) == 0 {
		fatal("no model snapshot found; run 'migrations add' first")
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fatalf("driver %q is not available in this wormhole binary: %v", driver, err)
	}
	defer db.Close()

	live, err := migrations.IntrospectSchema(context.Background(), db)
	if err != nil {
		fatalf("introspect database: %v", err)
	}

	drifts := migrations.DetectDrift(snap, live)
	if len(drifts) == 0 {
		fmt.Println("No drift. The database matches the model snapshot.")
		return
	}
	fmt.Fprintf(os.Stderr, "Database has drifted from the snapshot (%d difference(s)):\n", len(drifts))
	for _, d := range drifts {
		fmt.Fprintf(os.Stderr, "  - %s\n", d)
	}
	os.Exit(1)
}

func writeOrDie(path string, b []byte) {
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fatalf("write %s: %v", path, err)
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "Error: "+msg)
	os.Exit(1)
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", a...)
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `wormhole - standalone migration CLI (EF Core style)

Usage:
  wormhole [-C dir] migrations add <Name>             Generate a migration from the model diff
  wormhole [-C dir] migrations list                   List migrations (applied/pending if DB reachable)
  wormhole [-C dir] migrations script <Name> [dialect] Render migrations as a .sql script
  wormhole [-C dir] database update                   Apply pending migrations
  wormhole [-C dir] database check                    Warn if the database drifted from the snapshot

Dialects: default, postgres, mysql, mssql

Environment:
  WORMHOLE_DSN     Database connection string (database update; list status)
  WORMHOLE_DRIVER  SQL driver name (default: sqlite)
  WORMHOLE_DIR     Migrations directory (default: ./migrations)

add/list/script need no database and no project compilation. database update
applies the JSON manifests with a registered driver (SQLite is bundled). For
other drivers, run in-project: go run -tags wormhole_cli . database update`)
}
