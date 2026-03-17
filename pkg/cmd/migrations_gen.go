//go:build wormhole_gen_migrations

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/nosqlmigrations"
)

// This file is designed to be copied into user projects.
// The user imports their database drivers in their main project,
// and this generator will use whatever drivers are available.

func main() {
	var (
		action      = flag.String("action", "", "Action to perform: add, list, update, script")
		name        = flag.String("name", "", "Migration name (for add/script actions)")
		dialect     = flag.String("dialect", "default", "SQL dialect for script generation")
		dir         = flag.String("dir", "", "Migrations directory (uses WORMHOLE_DIR env if empty)")
		dsn         = flag.String("dsn", "", "Database connection string (uses WORMHOLE_DSN env if empty)")
		driver      = flag.String("driver", "", "Database driver (uses WORMHOLE_DRIVER env if empty)")
		nosqlDir    = flag.String("nosql-dir", "", "NoSQL migrations directory (uses WORMHOLE_NOSQL_DIR env if empty)")
		nosqlProvider = flag.String("nosql-provider", "", "NoSQL provider (uses WORMHOLE_NOSQL_PROVIDER env if empty)")
	)
	flag.Parse()

	if *action == "" {
		fmt.Fprintln(os.Stderr, "Error: -action flag is required")
		flag.Usage()
		os.Exit(1)
	}

	switch *action {
	case "add":
		handleMigrationAdd(*name, *dir)
	case "list":
		handleMigrationsList(*dir)
	case "update":
		handleDatabaseUpdate(*dsn, *driver, *dir)
	case "script":
		handleMigrationScript(*name, *dialect, *dir)
	case "nosql-add":
		handleNoSQLMigrationAdd(*name, *nosqlDir)
	case "nosql-list":
		handleNoSQLMigrationsList(*nosqlDir)
	case "nosql-update":
		handleNoSQLMigrationsApply(*nosqlProvider, *nosqlDir)
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown action %q\n", *action)
		os.Exit(1)
	}
}

func handleMigrationAdd(name, dir string) {
	if name == "" {
		fmt.Fprintln(os.Stderr, "Error: migration name is required")
		os.Exit(1)
	}

	if dir == "" {
		if d := os.Getenv("WORMHOLE_DIR"); d != "" {
			dir = d
		} else {
			dir = "./migrations"
		}
	}

	// Auto-discover models from the current directory
	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering models: %v\n", err)
		os.Exit(1)
	}

	if len(models) == 0 {
		fmt.Fprintf(os.Stderr, "No models with `db` tags found in current directory.\n")
		fmt.Fprintf(os.Stderr, "Make sure your structs have proper wormhole tags, e.g.:\n")
		fmt.Fprintf(os.Stderr, "  type User struct {\n")
		fmt.Fprintf(os.Stderr, "    ID   int    `db:\"primary_key;auto_increment\"`\n")
		fmt.Fprintf(os.Stderr, "    Name string `db:\"column:name\"`\n")
		fmt.Fprintf(os.Stderr, "  }\n")
		os.Exit(1)
	}

	// Generate migration
	currentSchema := loadSnapshot(dir)
	targetSchema := migrations.SchemaFromEntities(models)
	
	differ := migrations.NewDiffer()
	ops := differ.Diff(currentSchema, targetSchema)
	
	if len(ops) == 0 {
		fmt.Println("No changes detected. Schema is up to date.")
		return
	}

	// Generate timestamp-based migration name
	timestamp := time.Now().Format("20060102150405")
	migrationID := fmt.Sprintf("%s_%s", timestamp, name)

	// Create migrations directory
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}

	// Generate Go migration file
	goContent := generateGoMigration(migrationID, name, ops)
	goPath := filepath.Join(dir, migrationID+".go")
	if err := os.WriteFile(goPath, []byte(goContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing Go migration: %v\n", err)
		os.Exit(1)
	}

	// Generate SQL migration file
	sqlContent := generateSQLMigration(ops, migrations.DefaultDialect{})
	sqlPath := filepath.Join(dir, migrationID+".sql")
	if err := os.WriteFile(sqlPath, []byte(sqlContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing SQL migration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created migration: %s\n", migrationID)
	fmt.Printf("  Go file:  %s\n", goPath)
	fmt.Printf("  SQL file: %s\n", sqlPath)
	fmt.Printf("Operations: %d\n", len(ops))
	for _, op := range ops {
		fmt.Printf("  - %s\n", describeOp(op))
	}
}

func handleMigrationsList(dir string) {
	if dir == "" {
		if d := os.Getenv("WORMHOLE_DIR"); d != "" {
			dir = d
		} else {
			dir = "./migrations"
		}
	}

	files := listMigrationFiles(dir)
	if len(files) == 0 {
		fmt.Println("No migration files found.")
		return
	}

	fmt.Printf("Found %d migration(s):\n", len(files))
	for _, file := range files {
		id := strings.TrimSuffix(file, ".go")
		fmt.Printf("  - %s\n", id)
	}
}

func handleDatabaseUpdate(dsn, driver, dir string) {
	if driver == "" {
		if d := os.Getenv("WORMHOLE_DRIVER"); d != "" {
			driver = d
		} else {
			driver = "sqlite"
		}
	}
	
	if dsn == "" {
		if d := os.Getenv("WORMHOLE_DSN"); d != "" {
			dsn = d
		} else {
			fmt.Fprintln(os.Stderr, "Error: DSN is required (use -dsn flag or WORMHOLE_DSN env var)")
			os.Exit(1)
		}
	}

	if dir == "" {
		if d := os.Getenv("WORMHOLE_DIR"); d != "" {
			dir = d
		} else {
			dir = "./migrations"
		}
	}

	// Open database using user's imported drivers
	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()

	// Ensure history table exists
	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "Error ensuring history table: %v\n", err)
		os.Exit(1)
	}

	// Apply pending migrations
	migrationFiles := listMigrationFiles(dir)
	if len(migrationFiles) == 0 {
		fmt.Println("No migration files found.")
		return
	}

	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading migration history: %v\n", err)
		os.Exit(1)
	}

	pendingCount := 0
	for _, file := range migrationFiles {
		id := strings.TrimSuffix(file, ".go")
		if !applied[id] {
			if err := executeMigrationFile(ctx, db, dir, file, id); err != nil {
				fmt.Fprintf(os.Stderr, "Error executing migration %s: %v\n", id, err)
				os.Exit(1)
			}
			pendingCount++
		}
	}

	if pendingCount == 0 {
		fmt.Println("Database is up to date. No pending migrations.")
	} else {
		fmt.Printf("Successfully applied %d migration(s).\n", pendingCount)
	}
}

func handleMigrationScript(name, dialect, dir string) {
	if name == "" {
		fmt.Fprintln(os.Stderr, "Error: migration name is required")
		os.Exit(1)
	}

	if dir == "" {
		if d := os.Getenv("WORMHOLE_DIR"); d != "" {
			dir = d
		} else {
			dir = "./migrations"
		}
	}

	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering models: %v\n", err)
		os.Exit(1)
	}

	currentSchema := loadSnapshot(dir)
	targetSchema := migrations.SchemaFromEntities(models)
	
	differ := migrations.NewDiffer()
	ops := differ.Diff(currentSchema, targetSchema)
	
	dialectObj := resolveDialect(dialect)
	script := generateSQLMigration(ops, dialectObj)
	
	fileName := fmt.Sprintf("%s_%s.sql", time.Now().Format("20060102150405"), name)
	path := filepath.Join(dir, fileName)
	
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created SQL script: %s\n", path)
	fmt.Printf("Dialect: %s\n", dialect)
	fmt.Printf("Operations: %d\n", len(ops))
	for _, op := range ops {
		fmt.Printf("  - %s\n", describeOp(op))
	}
}

func handleNoSQLMigrationAdd(name, dir string) {
	if name == "" {
		fmt.Fprintln(os.Stderr, "Error: migration name is required")
		os.Exit(1)
	}

	if dir == "" {
		if d := os.Getenv("WORMHOLE_NOSQL_DIR"); d != "" {
			dir = d
		} else {
			dir = "./nosql-migrations"
		}
	}

	timestamp := time.Now().Format("20060102150405")
	scriptID := fmt.Sprintf("%s_%s", timestamp, name)
	
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}

	path := filepath.Join(dir, scriptID+".js")
	template := fmt.Sprintf(`// NoSQL Migration: %s
// Generated: %s

// Add your NoSQL migration logic here
// Example for MongoDB:
// db.users.createIndex({ email: 1 }, { unique: true });
// db.products.updateMany({}, { $set: { version: 2 } });

print("Migration %s executed");
`, name, time.Now().Format(time.RFC3339), scriptID)

	if err := os.WriteFile(path, []byte(template), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created NoSQL migration: %s\n", path)
}

func handleNoSQLMigrationsList(dir string) {
	if dir == "" {
		if d := os.Getenv("WORMHOLE_NOSQL_DIR"); d != "" {
			dir = d
		} else {
			dir = "./nosql-migrations"
		}
	}

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
	applied, err := history.AppliedSet(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading history: %v\n", err)
		os.Exit(1)
	}

	for _, s := range scripts {
		status := "pending"
		if applied[s.ID] {
			status = "applied"
		}
		fmt.Printf("  [%s] %s (%s)\n", status, s.ID, s.Name)
	}
}

func handleNoSQLMigrationsApply(provider, dir string) {
	if provider == "" {
		if p := os.Getenv("WORMHOLE_NOSQL_PROVIDER"); p != "" {
			provider = p
		} else {
			provider = "mongo"
		}
	}

	if dir == "" {
		if d := os.Getenv("WORMHOLE_NOSQL_DIR"); d != "" {
			dir = d
		} else {
			dir = "./nosql-migrations"
		}
	}

	scripts, err := nosqlmigrations.LoadScripts(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading scripts: %v\n", err)
		os.Exit(1)
	}

	if len(scripts) == 0 {
		fmt.Println("No NoSQL migration scripts found.")
		return
	}

	// This would need to be implemented based on the user's imported NoSQL drivers
	fmt.Printf("Would apply %d NoSQL migration(s) using %s provider\n", len(scripts), provider)
	fmt.Println("Note: NoSQL migration application requires user's imported drivers")
}

// Helper functions (copied from original implementation)

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
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files
}

func generateGoMigration(id, name string, ops []migrations.Operation) string {
	return fmt.Sprintf(`package migrations

import (
	"context"
	"database/sql"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

// %s implements the %s migration
type %s struct{}

func (m %s) ID() string { return "%s" }
func (m %s) Name() string { return "%s" }

func (m %s) Up(ctx context.Context, db *sql.DB) error {
	// Add your migration logic here
	// This is generated automatically but can be customized
	return nil
}

func (m %s) Down(ctx context.Context, db *sql.DB) error {
	// Add your rollback logic here
	return nil
}

func init() {
	migrations.Register(&%s{})
}
`, name, name, id, id, id, id, name, id, id, id)
}

func generateSQLMigration(ops []migrations.Operation, dialect migrations.Dialect) string {
	var statements []string
	for _, op := range ops {
		stmt := op.GenerateSQL(dialect)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return strings.Join(statements, ";\n") + ";\n"
}

func resolveDialect(name string) migrations.Dialect {
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

func describeOp(op migrations.Operation) string {
	switch o := op.(type) {
	case *migrations.CreateTableOp:
		return fmt.Sprintf("Create table '%s' with %d columns", o.Table.Name, len(o.Table.Columns))
	case *migrations.DropTableOp:
		return fmt.Sprintf("Drop table '%s'", o.TableName)
	case *migrations.AddColumnOp:
		return fmt.Sprintf("Add column '%s.%s'", o.TableName, o.Column.Name)
	case *migrations.DropColumnOp:
		return fmt.Sprintf("Drop column '%s.%s'", o.TableName, o.ColumnName)
	default:
		return fmt.Sprintf("Operation: %T", op)
	}
}

func executeMigrationFile(ctx context.Context, db *sql.DB, dir, file, id string) error {
	// Try to execute corresponding SQL file
	sqlFile := strings.TrimSuffix(file, ".go") + ".sql"
	sqlPath := filepath.Join(dir, sqlFile)
	
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("read SQL file %s: %w", sqlPath, err)
	}

	// Execute SQL in a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	sqlContent := string(content)
	statements := strings.Split(sqlContent, ";")
	
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute statement %q: %w", stmt, err)
		}
	}

	// Record migration in history
	if _, err := tx.ExecContext(ctx, 
		"INSERT INTO _wormhole_migrations_history (migration_id, applied_at) VALUES (?, ?)",
		id, time.Now().UTC()); err != nil {
		return fmt.Errorf("record migration history: %w", err)
	}

	return tx.Commit()
}