package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initProject()
	case "migrations":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "add":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: wormhole migrations add <Name>")
				os.Exit(1)
			}
			runWithBuildTags("add", os.Args[3])
		case "list":
			runWithBuildTags("list", "")
		case "script":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: wormhole migrations script <Name> [dialect]")
				os.Exit(1)
			}
			dialect := "default"
			if len(os.Args) > 4 {
				dialect = os.Args[4]
			}
			runWithBuildTags("script", os.Args[3], "-dialect", dialect)
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
			runWithBuildTags("update", "")
		default:
			printUsage()
			os.Exit(1)
		}
	case "nosql-migrations":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "add":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: wormhole nosql-migrations add <Name>")
				os.Exit(1)
			}
			runWithBuildTags("nosql-add", os.Args[3])
		case "list":
			runWithBuildTags("nosql-list", "")
		case "apply":
			runWithBuildTags("nosql-update", "")
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
	fmt.Fprintln(os.Stderr, `wormhole - Entity Framework-like CLI for Go

Usage:
  wormhole init                                Initialize wormhole in current project
  wormhole migrations add <Name>              Generate a new migration from model diff
  wormhole migrations script <Name> [dialect]  Export migration as .sql file
  wormhole migrations list                     List pending migrations
  wormhole database update                     Apply pending migrations
  wormhole nosql-migrations add <Name>         Generate NoSQL evolution script
  wormhole nosql-migrations list               List NoSQL evolution scripts
  wormhole nosql-migrations apply              Apply pending NoSQL evolution scripts

Dialects: default, postgres, mysql, mssql

Environment Variables:
  WORMHOLE_DSN               Database connection string (required for database commands)
  WORMHOLE_DRIVER            SQL driver name (default: sqlite)
  WORMHOLE_DIR               Migrations directory (default: ./migrations)
  WORMHOLE_NOSQL_PROVIDER    NoSQL backend (default: mongo)
  WORMHOLE_NOSQL_DSN         NoSQL connection string
  WORMHOLE_NOSQL_DB          NoSQL database name
  WORMHOLE_NOSQL_DIR         NoSQL scripts directory (default: ./nosql-migrations)

How it works:
  wormhole uses build tags to compile your project with your database drivers.
  Run 'wormhole init' first to create wormhole_migrations_gen.go in your project.
  Then import your database drivers in that file and use wormhole commands.

Example:
  wormhole init
  # Edit wormhole_migrations_gen.go to import your drivers
  export WORMHOLE_DSN="user:password@tcp(localhost:3306)/mydb"
  export WORMHOLE_DRIVER="mysql"  
  wormhole migrations add CreateUser
  wormhole database update`)
}

func initProject() {
	generatorPath := "wormhole_migrations_gen.go"

	if _, err := os.Stat(generatorPath); err == nil {
		fmt.Printf("File %s already exists.\n", generatorPath)
		return
	}

	template := `//go:build wormhole_gen_migrations

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

	// Import your database drivers here
	// Examples:
	// _ "github.com/lib/pq"                    // PostgreSQL
	// _ "github.com/go-sql-driver/mysql"       // MySQL  
	// _ "github.com/denisenkom/go-mssqldb"     // SQL Server
	// _ "github.com/glebarez/sqlite"           // SQLite (pure Go)

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/nosqlmigrations"
)

// This file enables wormhole CLI to use your project's database drivers.
// The CLI builds this with: go build -tags wormhole_gen_migrations

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
		fmt.Fprintf(os.Stderr, "No models with ` + "`" + `db` + "`" + ` tags found in current directory.\n")
		fmt.Fprintf(os.Stderr, "Make sure your structs have proper wormhole tags, e.g.:\n")
		fmt.Fprintf(os.Stderr, "  type User struct {\n")
		fmt.Fprintf(os.Stderr, "    ID   int    ` + "`" + `db:\"primary_key;auto_increment\"` + "`" + `\n")
		fmt.Fprintf(os.Stderr, "    Name string ` + "`" + `db:\"column:name\"` + "`" + `\n")
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
	template := fmt.Sprintf(` + "`" + `// NoSQL Migration: %s
// Generated: %s

// Add your NoSQL migration logic here
// Example for MongoDB:
// db.users.createIndex({ email: 1 }, { unique: true });
// db.products.updateMany({}, { $set: { version: 2 } });

print("Migration %s executed");
` + "`" + `, name, time.Now().Format(time.RFC3339), scriptID)

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

	fmt.Printf("Would apply %d NoSQL migration(s) using %s provider\n", len(scripts), provider)
	fmt.Println("Note: NoSQL migration application requires user's imported drivers")
}

// Helper functions

func loadSnapshot(_ string) migrations.DatabaseSchema {
	// For the initial version, start with an empty schema (greenfield).
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
	return fmt.Sprintf(` + "`" + `package migrations

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
` + "`" + `, name, name, id, id, id, id, name, id, id, id)
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
`

	if err := os.WriteFile(generatorPath, []byte(template), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating %s: %v\n", generatorPath, err)
		os.Exit(1)
	}

	fmt.Printf("Created %s\n", generatorPath)
	fmt.Println("Now you can use wormhole commands in this project!")
	fmt.Println()
	fmt.Println("Remember to:")
	fmt.Println("1. Import your database drivers in the imports section of this file")
	fmt.Println("2. Set WORMHOLE_DSN and WORMHOLE_DRIVER environment variables")
	fmt.Println("3. Define your models with proper `db` tags")
	fmt.Println()
	fmt.Println("Example usage:")
	fmt.Println("  wormhole migrations add CreateUser")
	fmt.Println("  wormhole database update")
}

func runWithBuildTags(action, name string, extraArgs ...string) {
	// Check if migrations generator exists
	generatorPath := "wormhole_migrations_gen.go"
	if _, err := os.Stat(generatorPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: %s not found in current directory.\n", generatorPath)
		fmt.Fprintln(os.Stderr, "Create it with: wormhole init")
		os.Exit(1)
	}

	// Build temporary binary with wormhole_gen_migrations tag
	tempBinary := filepath.Join(os.TempDir(), fmt.Sprintf("wormhole_temp_%d", os.Getpid()))
	defer os.Remove(tempBinary)

	fmt.Printf("Building migration runner...\n")
	buildCmd := exec.Command("go", "build", "-tags", "wormhole_gen_migrations", "-o", tempBinary, ".")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr

	if err := buildCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error building migration runner: %v\n", err)
		fmt.Fprintln(os.Stderr, "Make sure your wormhole_migrations_gen.go file is valid and your project compiles.")
		os.Exit(1)
	}

	// Prepare arguments for the temporary binary
	args := []string{"-action", action}
	if name != "" {
		args = append(args, "-name", name)
	}
	args = append(args, extraArgs...)

	// Run the temporary binary
	fmt.Printf("Running migration action: %s\n", action)
	runCmd := exec.Command(tempBinary, args...)
	runCmd.Stdout = os.Stdout
	runCmd.Stderr = os.Stderr
	runCmd.Env = os.Environ()

	if err := runCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running migration action: %v\n", err)
		os.Exit(1)
	}
}
