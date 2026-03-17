package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/glebarez/sqlite" // pure Go sqlite driver (no CGO)
	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/mongo"
	"github.com/fabricatorsltd/go-wormhole/pkg/nosqlmigrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider" // New import
	sqlprovider "github.com/fabricatorsltd/go-wormhole/pkg/sql" // New import with alias
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
		case "script":
			cmdMigrationsScript()
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
		case "sync":
			cmdDBSync()
		default:
			printUsage()
			os.Exit(1)
		}
	case "dbcontext":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "scaffold":
			cmdScaffold()
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
		case "generate":
			cmdNoSQLMigrationsGenerate()
		case "list":
			cmdNoSQLMigrationsList()
		case "apply":
			cmdNoSQLMigrationsApply()
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
  wormhole migrations add <Name>              Generate a new migration from model diff
  wormhole migrations script <Name> [dialect]  Export migration as .sql file
  wormhole migrations list                     List pending migrations
  wormhole database update                     Apply pending migrations
  wormhole database sync <src_prov> <dst_prov> Synchronize data between providers
  wormhole dbcontext scaffold                  Generate Go structs from existing database
  wormhole nosql-migrations generate <Name>    Generate NoSQL evolution script
  wormhole nosql-migrations list               List NoSQL evolution scripts
  wormhole nosql-migrations apply              Apply pending NoSQL evolution scripts

Dialects: default, postgres, mysql, mssql

Environment:
  WORMHOLE_DSN               Database connection string (required for database commands)
  WORMHOLE_DRIVER            SQL driver name (default: sqlite)
  WORMHOLE_DIR               Migrations directory (default: ./migrations)
  WORMHOLE_NOSQL_PROVIDER    NoSQL backend (default: mongo)
  WORMHOLE_NOSQL_DSN         NoSQL connection string (required for apply)
  WORMHOLE_NOSQL_DB          NoSQL database name (required for apply)
  WORMHOLE_NOSQL_DIR         NoSQL scripts directory (default: ./nosql-migrations)
  WORMHOLE_SOURCE_DSN        Source database connection string (required for db sync)
  WORMHOLE_SOURCE_DRIVER     Source SQL driver name (default: sqlite)
  WORMHOLE_SOURCE_DB         Source NoSQL database name (required for mongo source)
  WORMHOLE_DESTINATION_DSN   Destination database connection string (required for db sync)
  WORMHOLE_DESTINATION_DRIVER Destination SQL driver name (default: sqlite)
  WORMHOLE_DESTINATION_DB    Destination NoSQL database name (required for mongo destination)`)
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

	// Generate Go file
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

	// Also generate SQL file for CLI execution
	dialect := resolveDialect("default")
	sqlScript := migrations.GenerateSQLScript(ops, dialect)
	sqlFileName := strings.TrimSuffix(fileName, ".go") + ".sql"
	sqlPath := filepath.Join(dir, sqlFileName)
	if err := os.WriteFile(sqlPath, []byte(sqlScript), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write SQL: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created migration: %s\n", path)
	fmt.Printf("Created SQL script: %s\n", sqlPath)
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

	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "ensure history: %v\n", err)
		os.Exit(1)
	}

	// Load all migration files and try to execute them as SQL scripts
	dir := migrationsDir()
	migrationFiles := listMigrationFiles(dir)

	if len(migrationFiles) == 0 {
		fmt.Println("No migration files found.")
		return
	}

	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read history: %v\n", err)
		os.Exit(1)
	}

	pendingCount := 0
	for _, file := range migrationFiles {
		id := strings.TrimSuffix(file, ".go")
		if !applied[id] {
			// Try to execute as SQL-based migration
			if err := executeMigrationFile(ctx, db, dir, file, id); err != nil {
				fmt.Fprintf(os.Stderr, "failed to execute migration %s: %v\n", id, err)
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

func cmdMigrationsScript() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "Usage: wormhole migrations script <Name> [dialect]")
		os.Exit(1)
	}
	name := os.Args[3]
	dir := migrationsDir()

	dialectName := "default"
	if len(os.Args) >= 5 {
		dialectName = os.Args[4]
	}
	dialect := resolveDialect(dialectName)

	models := loadModels()
	if len(models) == 0 {
		fmt.Fprintln(os.Stderr, "No models registered. Call schema.Parse() on your entities first.")
		os.Exit(1)
	}

	current := loadSnapshot(dir)
	ops := migrations.ComputeDiff(models, current)
	if len(ops) == 0 {
		fmt.Println("No changes detected.")
		return
	}

	script := migrations.GenerateSQLScript(ops, dialect)
	fileName := migrations.SQLScriptFileName(name)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created SQL script: %s\n", path)
	fmt.Printf("Dialect: %s\n", dialectName)
	fmt.Printf("Operations: %d\n", len(ops))
	for _, op := range ops {
		fmt.Printf("  - %s\n", describeOp(op))
	}
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

func cmdScaffold() {
	db := openDB()
	defer db.Close()

	ctx := context.Background()

	results, err := migrations.Scaffold(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scaffold: %v\n", err)
		os.Exit(1)
	}

	if len(results) == 0 {
		fmt.Println("No user tables found.")
		return
	}

	dir := migrationsDir()
	outDir := filepath.Join(filepath.Dir(dir), "models")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	header := "package models\n\n"
	for _, r := range results {
		path := filepath.Join(outDir, r.TableName+".go")
		content := header + r.Source
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("  ✓ %s → %s\n", r.TableName, path)
	}
	fmt.Printf("\nScaffolded %d table(s) into %s/\n", len(results), outDir)
}

func cmdNoSQLMigrationsGenerate() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "Usage: wormhole nosql-migrations generate <Name>")
		os.Exit(1)
	}
	dir := nosqlMigrationsDir()
	s := nosqlmigrations.GenerateTemplate(os.Args[3])
	path, err := nosqlmigrations.SaveScript(dir, s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "save script: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created NoSQL script: %s\n", path)
}

func cmdNoSQLMigrationsList() {
	dir := nosqlMigrationsDir()
	scripts, err := nosqlmigrations.LoadScripts(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load scripts: %v\n", err)
		os.Exit(1)
	}
	if len(scripts) == 0 {
		fmt.Println("No NoSQL migration scripts found.")
		return
	}
	history := nosqlmigrations.NewFileHistoryStore(filepath.Join(dir, ".history.json"))
	applied, err := history.AppliedSet(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "load history: %v\n", err)
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

func cmdNoSQLMigrationsApply() {
	dir := nosqlMigrationsDir()
	scripts, err := nosqlmigrations.LoadScripts(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load scripts: %v\n", err)
		os.Exit(1)
	}
	if len(scripts) == 0 {
		fmt.Println("No NoSQL migration scripts found.")
		return
	}

	providerName := os.Getenv("WORMHOLE_NOSQL_PROVIDER")
	if providerName == "" {
		providerName = "mongo"
	}

	var exec nosqlmigrations.Executor
	switch providerName {
	case "mongo":
		dsn := os.Getenv("WORMHOLE_NOSQL_DSN")
		dbName := os.Getenv("WORMHOLE_NOSQL_DB")
		if dsn == "" || dbName == "" {
			fmt.Fprintln(os.Stderr, "WORMHOLE_NOSQL_DSN and WORMHOLE_NOSQL_DB are required for mongo")
			os.Exit(1)
		}
		p := mongo.New(nil, dbName)
		if err := p.Open(context.Background(), dsn); err != nil {
			fmt.Fprintf(os.Stderr, "open mongo: %v\n", err)
			os.Exit(1)
		}
		defer p.Close()
		exec = nosqlmigrations.NewMongoExecutor(p.Database())
	default:
		fmt.Fprintf(os.Stderr, "unsupported NoSQL provider: %s\n", providerName)
		os.Exit(1)
	}

	history := nosqlmigrations.NewFileHistoryStore(filepath.Join(dir, ".history.json"))
	runner := nosqlmigrations.NewRunner(exec, history)
	applied, err := runner.ApplyPending(context.Background(), scripts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "apply NoSQL migrations: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Applied NoSQL scripts: %d\n", applied)
}

func cmdDBSync() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "Usage: wormhole database sync <source_provider> <destination_provider>")
		os.Exit(1)
	}

	sourceProviderName := os.Args[3]
	destinationProviderName := os.Args[4]

	ctx := context.Background()

	sourceProvider, err := newProvider(ctx, sourceProviderName, "WORMHOLE_SOURCE_DSN", "WORMHOLE_SOURCE_DRIVER", "WORMHOLE_SOURCE_DB")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create source provider: %v\n", err)
		os.Exit(1)
	}
	defer sourceProvider.Close()

	destinationProvider, err := newProvider(ctx, destinationProviderName, "WORMHOLE_DESTINATION_DSN", "WORMHOLE_DESTINATION_DRIVER", "WORMHOLE_DESTINATION_DB")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create destination provider: %v\n", err)
		os.Exit(1)
	}
	defer destinationProvider.Close()

	// For now, ChangeTracker is nil. In a real scenario, this would be initialized.
	migrator := migrations.NewDataMigrator(sourceProvider, destinationProvider, nil)

	if err := migrator.FullSync(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "full synchronization failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Data synchronization completed successfully.")
}

// newProvider creates and initializes a provider based on name and environment variables.
func newProvider(ctx context.Context, providerName, dsnEnv, driverEnv, dbNameEnv string) (provider.Provider, error) {
	var p provider.Provider
	switch providerName {
	case "mongo":
		dsn := os.Getenv(dsnEnv)
		dbName := os.Getenv(dbNameEnv)
		if dsn == "" || dbName == "" {
			return nil, fmt.Errorf("%s and %s are required for mongo provider", dsnEnv, dbNameEnv)
		}
		mp := mongo.New(nil, dbName)
		if err := mp.Open(ctx, dsn); err != nil {
			return nil, fmt.Errorf("failed to open mongo provider: %w", err)
		}
		p = mp
	case "sql": // Generic SQL provider, driver determined by environment
		driver := os.Getenv(driverEnv)
		dsn := os.Getenv(dsnEnv)
		if dsn == "" {
			return nil, fmt.Errorf("%s is required for sql provider", dsnEnv)
		}
		if driver == "" {
			driver = "sqlite" // Default to sqlite if not specified
		}

		db, err := sql.Open(driver, dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open sql DB: %w", err)
		}
		driverName := strings.ToLower(driver)
		opts := []sqlprovider.Option{sqlprovider.WithName(driverName)}
		if strings.Contains(driverName, "postgres") || strings.Contains(driverName, "pgx") {
			opts = append(opts, sqlprovider.WithNumberedParams())
		}
		sp := sqlprovider.New(db, opts...)
		if err := sp.Open(ctx, dsn); err != nil {
			return nil, fmt.Errorf("failed to open sql provider: %w", err)
		}
		p = sp
	default:
		return nil, fmt.Errorf("unsupported provider: %s", providerName)
	}
	return p, nil
}

// --- helpers ---

func migrationsDir() string {
	if d := os.Getenv("WORMHOLE_DIR"); d != "" {
		return d
	}
	return "./migrations"
}

func nosqlMigrationsDir() string {
	if d := os.Getenv("WORMHOLE_NOSQL_DIR"); d != "" {
		return d
	}
	return "./nosql-migrations"
}

func openDB() *sql.DB {
	driver := os.Getenv("WORMHOLE_DRIVER")
	if driver == "" {
		driver = "sqlite"
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
	// Try to discover models from the current directory
	models, err := discovery.DiscoverModels(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to discover models: %v\n", err)
		return nil
	}

	if len(models) == 0 {
		fmt.Fprintf(os.Stderr, "No models with `db` tags found in current directory.\n")
		fmt.Fprintf(os.Stderr, "Make sure your structs have proper wormhole tags, e.g.:\n")
		fmt.Fprintf(os.Stderr, "  type User struct {\n")
		fmt.Fprintf(os.Stderr, "    ID   int    `db:\"primary_key;auto_increment\"`\n")
		fmt.Fprintf(os.Stderr, "    Name string `db:\"column:name\"`\n")
		fmt.Fprintf(os.Stderr, "  }\n")
	}

	return models
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

// executeMigrationFile tries to execute a migration file by parsing and applying its operations
func executeMigrationFile(ctx context.Context, db *sql.DB, dir, fileName, migrationID string) error {
	filePath := filepath.Join(dir, fileName)
	
	// Try to extract SQL from the Go migration file
	sqlStatements, err := extractSQLFromMigrationFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to extract SQL: %w", err)
	}

	// Execute each SQL statement in a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, stmt := range sqlStatements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := tx.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to execute SQL statement: %s, error: %w", stmt, err)
		}
	}

	// Record the migration as applied
	_, err = tx.ExecContext(ctx, 
		"INSERT INTO _wormhole_migrations_history (migration_id, applied_at) VALUES (?, datetime('now'))", 
		migrationID)
	if err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	return tx.Commit()
}

// extractSQLFromMigrationFile parses a Go migration file and extracts SQL statements
// This is a simplified version - in practice, you might want to use go/ast more thoroughly
func extractSQLFromMigrationFile(filePath string) ([]string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Look for SQL script files with the same base name
	dir := filepath.Dir(filePath)
	baseName := strings.TrimSuffix(filepath.Base(filePath), ".go")
	sqlFilePath := filepath.Join(dir, baseName+".sql")

	if _, err := os.Stat(sqlFilePath); err == nil {
		// Use the SQL file if it exists
		sqlContent, err := os.ReadFile(sqlFilePath)
		if err != nil {
			return nil, err
		}
		
		// Split by semicolon (simple approach)
		statements := strings.Split(string(sqlContent), ";")
		var nonEmpty []string
		for _, stmt := range statements {
			if strings.TrimSpace(stmt) != "" {
				nonEmpty = append(nonEmpty, strings.TrimSpace(stmt))
			}
		}
		return nonEmpty, nil
	}

	// Fallback: try to extract SQL from Go migration file comments or strings
	// This is a basic approach - look for SQL in comments or string literals
	lines := strings.Split(string(content), "\n")
	var sqlStatements []string
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Look for SQL in comments starting with -- or /* */
		if strings.HasPrefix(line, "// SQL:") {
			sql := strings.TrimPrefix(line, "// SQL:")
			sql = strings.TrimSpace(sql)
			if sql != "" {
				sqlStatements = append(sqlStatements, sql)
			}
		}
	}

	return sqlStatements, nil
}
