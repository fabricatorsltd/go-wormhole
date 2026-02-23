package migrations_test

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/mirkobrombin/go-wormhole/pkg/migrations"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// --- SchemaBuilder tests ---

func TestBuilderCreateTable(t *testing.T) {
	b := migrations.NewBuilder()
	b.CreateTable("users",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
		migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
		migrations.ColumnDef{Name: "email", SQLType: "VARCHAR(255)", Nullable: true},
	)

	stmts := b.Statements()
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	s := stmts[0]
	if !strings.Contains(s, `"users"`) {
		t.Errorf("missing table name: %s", s)
	}
	if !strings.Contains(s, "PRIMARY KEY") {
		t.Errorf("missing PRIMARY KEY: %s", s)
	}
	if !strings.Contains(s, "AUTOINCREMENT") {
		t.Errorf("missing AUTOINCREMENT: %s", s)
	}
}

func TestBuilderAddDropColumn(t *testing.T) {
	b := migrations.NewBuilder()
	b.AddColumn("users", migrations.ColumnDef{Name: "age", SQLType: "INTEGER"})
	b.DropColumn("users", "temp")

	stmts := b.Statements()
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[0], "ADD COLUMN") {
		t.Errorf("expected ADD COLUMN: %s", stmts[0])
	}
	if !strings.Contains(stmts[1], "DROP COLUMN") {
		t.Errorf("expected DROP COLUMN: %s", stmts[1])
	}
}

func TestBuilderCreateIndex(t *testing.T) {
	b := migrations.NewBuilder()
	b.CreateIndex("idx_users_email", "users", true, "email")

	stmts := b.Statements()
	if !strings.Contains(stmts[0], "UNIQUE INDEX") {
		t.Errorf("expected UNIQUE INDEX: %s", stmts[0])
	}
}

// --- Differ tests ---

func TestDifferNewTable(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "products",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, AutoIncr: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
		},
	}

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})

	var createOp *migrations.CreateTableOp
	for _, op := range ops {
		if ct, ok := op.(migrations.CreateTableOp); ok {
			createOp = &ct
		}
	}
	if createOp == nil {
		t.Fatal("expected CreateTableOp")
	}
	if createOp.Table != "products" {
		t.Errorf("table = %q, want products", createOp.Table)
	}
	if len(createOp.Columns) != 2 {
		t.Errorf("columns = %d, want 2", len(createOp.Columns))
	}
}

func TestDifferAddColumn(t *testing.T) {
	current := migrations.DatabaseSchema{
		Tables: map[string]*migrations.TableSchema{
			"users": {
				Name: "users",
				Columns: map[string]*migrations.ColumnDef{
					"id":   {Name: "id", SQLType: "INTEGER", PrimaryKey: true},
					"name": {Name: "name", SQLType: "TEXT"},
				},
			},
		},
	}

	meta := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
			{FieldName: "Age", Column: "age", GoType: reflect.TypeOf(0)},
		},
	}

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, current)

	found := false
	for _, op := range ops {
		if add, ok := op.(migrations.AddColumnOp); ok {
			if add.Column.Name == "age" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected AddColumnOp for 'age'")
	}
}

func TestDifferDropColumn(t *testing.T) {
	current := migrations.DatabaseSchema{
		Tables: map[string]*migrations.TableSchema{
			"users": {
				Name: "users",
				Columns: map[string]*migrations.ColumnDef{
					"id":   {Name: "id", SQLType: "INTEGER", PrimaryKey: true},
					"name": {Name: "name", SQLType: "TEXT"},
					"old":  {Name: "old", SQLType: "TEXT"},
				},
			},
		},
	}

	meta := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
		},
	}

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, current)

	found := false
	for _, op := range ops {
		if drop, ok := op.(migrations.DropColumnOp); ok && drop.Column == "old" {
			found = true
		}
	}
	if !found {
		t.Error("expected DropColumnOp for 'old'")
	}
}

func TestDifferDropTable(t *testing.T) {
	current := migrations.DatabaseSchema{
		Tables: map[string]*migrations.TableSchema{
			"obsolete": {Name: "obsolete", Columns: map[string]*migrations.ColumnDef{}},
		},
	}

	ops := migrations.ComputeDiff(nil, current)

	found := false
	for _, op := range ops {
		if drop, ok := op.(migrations.DropTableOp); ok && drop.Table == "obsolete" {
			found = true
		}
	}
	if !found {
		t.Error("expected DropTableOp for 'obsolete'")
	}
}

func TestMetaToSnapshot(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "items",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Title", Column: "title", GoType: reflect.TypeOf("")},
		},
	}

	snap := migrations.MetaToSnapshot([]*model.EntityMeta{meta})
	if _, ok := snap.Tables["items"]; !ok {
		t.Fatal("expected 'items' table in snapshot")
	}
	if _, ok := snap.Tables["items"].Columns["id"]; !ok {
		t.Error("expected 'id' column")
	}
	if _, ok := snap.Tables["items"].Columns["title"]; !ok {
		t.Error("expected 'title' column")
	}
}

// --- History table tests ---

func TestHistoryEnsureAndRecord(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		t.Fatal(err)
	}

	// Idempotent
	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		t.Fatal(err)
	}

	applied, err := migrations.AppliedMigrations(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Errorf("expected 0 applied, got %d", len(applied))
	}

	// Record via transaction
	tx, _ := db.BeginTx(ctx, nil)
	if err := migrations.RecordMigration(ctx, tx, "20260101_init"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	applied, _ = migrations.AppliedMigrations(ctx, db)
	if !applied["20260101_init"] {
		t.Error("expected '20260101_init' to be applied")
	}
}

func TestHistoryRemove(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	_ = migrations.EnsureHistoryTable(ctx, db)

	tx, _ := db.BeginTx(ctx, nil)
	_ = migrations.RecordMigration(ctx, tx, "m1")
	_ = tx.Commit()

	tx, _ = db.BeginTx(ctx, nil)
	_ = migrations.RemoveMigration(ctx, tx, "m1")
	_ = tx.Commit()

	applied, _ := migrations.AppliedMigrations(ctx, db)
	if applied["m1"] {
		t.Error("m1 should have been removed")
	}
}

// --- Runner tests ---

func TestRunnerUpAndDown(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	runner := migrations.NewRunner(db)

	runner.Add(migrations.Migration{
		ID: "001_create_users",
		Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("users",
				migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
			)
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropTable("users")
		},
	})

	runner.Add(migrations.Migration{
		ID: "002_add_email",
		Up: func(b *migrations.SchemaBuilder) {
			b.AddColumn("users", migrations.ColumnDef{Name: "email", SQLType: "TEXT", Nullable: true})
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropColumn("users", "email")
		},
	})

	// Check pending
	pending, err := runner.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}

	// Apply all
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify table exists and has the email column
	_, err = db.ExecContext(ctx, `INSERT INTO "users" ("name", "email") VALUES ('Alice', 'alice@test.com')`)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	var name, email string
	row := db.QueryRowContext(ctx, `SELECT "name", "email" FROM "users" WHERE "name" = 'Alice'`)
	if err := row.Scan(&name, &email); err != nil {
		t.Fatal(err)
	}
	if name != "Alice" || email != "alice@test.com" {
		t.Errorf("got name=%q email=%q", name, email)
	}

	// No pending after apply
	pending, _ = runner.Pending(ctx)
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after Up, got %d", len(pending))
	}

	// Rollback last migration
	if err := runner.Down(ctx); err != nil {
		t.Fatal(err)
	}

	// email column should be gone (SQLite doesn't fully support DROP COLUMN
	// in older versions, but modern SQLite 3.35+ does)
	applied, _ := migrations.AppliedMigrations(ctx, db)
	if applied["002_add_email"] {
		t.Error("002_add_email should have been rolled back")
	}
	if !applied["001_create_users"] {
		t.Error("001_create_users should still be applied")
	}
}

func TestRunnerIdempotent(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	runner := migrations.NewRunner(db)
	runner.Add(migrations.Migration{
		ID: "001_init",
		Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("t", migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true})
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropTable("t")
		},
	})

	// Run twice — should not fail
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
}

// --- Codegen tests ---

func TestGenerateMigrationFile(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.CreateTableOp{
			Table: "orders",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
				{Name: "total", SQLType: "REAL"},
			},
		},
		migrations.AddColumnOp{
			Table:  "users",
			Column: migrations.ColumnDef{Name: "age", SQLType: "INTEGER"},
		},
	}

	src := migrations.GenerateMigrationFile("AddOrders", ops)

	if !strings.Contains(src, "package migrations") {
		t.Error("missing package declaration")
	}
	if !strings.Contains(src, "func init()") {
		t.Error("missing init function")
	}
	if !strings.Contains(src, `b.CreateTable("orders"`) {
		t.Error("missing CreateTable call")
	}
	if !strings.Contains(src, `b.AddColumn("users"`) {
		t.Error("missing AddColumn call")
	}
	if !strings.Contains(src, `b.DropTable("orders"`) {
		t.Error("missing Down DropTable")
	}
	if !strings.Contains(src, "add_orders") {
		t.Error("missing snake_case migration ID")
	}
}

// --- GoTypeToSQL tests ---

func TestGoTypeToSQL(t *testing.T) {
	cases := []struct {
		goType reflect.Type
		want   string
	}{
		{reflect.TypeOf(0), "INTEGER"},
		{reflect.TypeOf(int64(0)), "BIGINT"},
		{reflect.TypeOf(float32(0)), "REAL"},
		{reflect.TypeOf(float64(0)), "DOUBLE PRECISION"},
		{reflect.TypeOf(true), "BOOLEAN"},
		{reflect.TypeOf(""), "TEXT"},
		{nil, "TEXT"},
	}

	for _, c := range cases {
		got := migrations.GoTypeToSQL(c.goType)
		if got != c.want {
			t.Errorf("GoTypeToSQL(%v) = %q, want %q", c.goType, got, c.want)
		}
	}
}

// --- Full E2E: differ -> builder -> runner -> real DB ---

func TestE2EDifferToRunner(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Model: users with id, name
	userMeta := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, AutoIncr: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
		},
	}

	// Step 1: diff against empty DB → should produce CreateTable
	ops := migrations.ComputeDiff(
		[]*model.EntityMeta{userMeta},
		migrations.DatabaseSchema{},
	)

	if len(ops) == 0 {
		t.Fatal("expected ops from diff")
	}

	// Step 2: convert to migration and run
	runner := migrations.NewRunner(db)
	runner.Add(migrations.Migration{
		ID: "001_init",
		Up: func(b *migrations.SchemaBuilder) {
			for _, op := range ops {
				switch o := op.(type) {
				case migrations.CreateTableOp:
					b.CreateTable(o.Table, o.Columns...)
				case migrations.CreateIndexOp:
					b.CreateIndex(o.Name, o.Table, o.Unique, o.Columns...)
				}
			}
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropTable("users")
		},
	})

	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}

	// Step 3: verify the table works
	_, err := db.ExecContext(ctx, `INSERT INTO "users" ("name") VALUES ('Bob')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var name string
	row := db.QueryRowContext(ctx, `SELECT "name" FROM "users"`)
	if err := row.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Bob" {
		t.Errorf("got %q, want Bob", name)
	}

	// Step 4: add a column via diff
	userMetaV2 := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, AutoIncr: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
			{FieldName: "Email", Column: "email", GoType: reflect.TypeOf(""), Nullable: true},
		},
	}

	snapshot := migrations.MetaToSnapshot([]*model.EntityMeta{userMeta})
	ops2 := migrations.ComputeDiff([]*model.EntityMeta{userMetaV2}, snapshot)

	hasAdd := false
	for _, op := range ops2 {
		if add, ok := op.(migrations.AddColumnOp); ok && add.Column.Name == "email" {
			hasAdd = true
		}
	}
	if !hasAdd {
		t.Fatal("expected AddColumnOp for email in v2 diff")
	}

	// Apply the add column
	runner.Add(migrations.Migration{
		ID: "002_add_email",
		Up: func(b *migrations.SchemaBuilder) {
			for _, op := range ops2 {
				if add, ok := op.(migrations.AddColumnOp); ok {
					b.AddColumn(add.Table, add.Column)
				}
			}
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropColumn("users", "email")
		},
	})

	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify email column works
	_, err = db.ExecContext(ctx, `UPDATE "users" SET "email" = 'bob@test.com' WHERE "name" = 'Bob'`)
	if err != nil {
		t.Fatalf("update email: %v", err)
	}

	var email sql.NullString
	row = db.QueryRowContext(ctx, `SELECT "email" FROM "users" WHERE "name" = 'Bob'`)
	if err := row.Scan(&email); err != nil {
		t.Fatal(err)
	}
	if !email.Valid || email.String != "bob@test.com" {
		t.Errorf("email = %v, want bob@test.com", email)
	}
}

// --- Postgres Dialect tests ---

func TestPostgresDialectAutoIncrement(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.PostgresDialect{})
	b.CreateTable("users",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
		migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
	)

	stmts := b.Statements()
	s := stmts[0]

	// Postgres should use SERIAL instead of INTEGER + AUTOINCREMENT
	if !strings.Contains(s, "SERIAL") {
		t.Errorf("expected SERIAL in Postgres DDL: %s", s)
	}
	if strings.Contains(s, "AUTOINCREMENT") {
		t.Errorf("should not contain AUTOINCREMENT: %s", s)
	}
	if strings.Contains(s, "AUTO_INCREMENT") {
		t.Errorf("should not contain AUTO_INCREMENT: %s", s)
	}
}

func TestPostgresDialectBigserial(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.PostgresDialect{})
	b.CreateTable("events",
		migrations.ColumnDef{Name: "id", SQLType: "BIGINT", PrimaryKey: true, AutoIncr: true},
	)

	s := b.Statements()[0]
	if !strings.Contains(s, "BIGSERIAL") {
		t.Errorf("expected BIGSERIAL for BIGINT auto-increment: %s", s)
	}
}

// --- MySQL Dialect tests ---

func TestMySQLDialectQuoting(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MySQLDialect{})
	b.CreateTable("orders",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
		migrations.ColumnDef{Name: "total", SQLType: "DECIMAL(10,2)"},
	)

	s := b.Statements()[0]

	// MySQL uses backticks
	if !strings.Contains(s, "`orders`") {
		t.Errorf("expected backtick quoting: %s", s)
	}
	if !strings.Contains(s, "AUTO_INCREMENT") {
		t.Errorf("expected AUTO_INCREMENT: %s", s)
	}
	if strings.Contains(s, "AUTOINCREMENT") {
		t.Errorf("should not contain SQLite AUTOINCREMENT: %s", s)
	}
}

// --- Snapshot Rebuild tests ---

func TestRebuildSnapshot(t *testing.T) {
	ops1 := []migrations.MigrationOp{
		migrations.CreateTableOp{
			Table: "users",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true},
				{Name: "name", SQLType: "TEXT"},
			},
		},
	}

	ops2 := []migrations.MigrationOp{
		migrations.AddColumnOp{
			Table:  "users",
			Column: migrations.ColumnDef{Name: "email", SQLType: "TEXT", Nullable: true},
		},
		migrations.CreateTableOp{
			Table: "orders",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true},
			},
		},
	}

	schema := migrations.RebuildSnapshot([][]migrations.MigrationOp{ops1, ops2})

	// users should have 3 columns
	if ts, ok := schema.Tables["users"]; !ok {
		t.Fatal("missing users table")
	} else {
		if len(ts.Columns) != 3 {
			t.Errorf("users columns = %d, want 3", len(ts.Columns))
		}
		if _, ok := ts.Columns["email"]; !ok {
			t.Error("missing email column after AddColumn")
		}
	}

	// orders should exist
	if _, ok := schema.Tables["orders"]; !ok {
		t.Fatal("missing orders table")
	}
}

func TestRebuildSnapshotDropTable(t *testing.T) {
	ops := [][]migrations.MigrationOp{
		{migrations.CreateTableOp{Table: "temp", Columns: []migrations.ColumnDef{{Name: "id"}}}},
		{migrations.DropTableOp{Table: "temp"}},
	}

	schema := migrations.RebuildSnapshot(ops)
	if _, ok := schema.Tables["temp"]; ok {
		t.Error("temp table should have been dropped")
	}
}

func TestRebuildSnapshotDropColumn(t *testing.T) {
	ops := [][]migrations.MigrationOp{
		{migrations.CreateTableOp{
			Table:   "items",
			Columns: []migrations.ColumnDef{{Name: "id"}, {Name: "old_col"}},
		}},
		{migrations.DropColumnOp{Table: "items", Column: "old_col"}},
	}

	schema := migrations.RebuildSnapshot(ops)
	ts := schema.Tables["items"]
	if _, ok := ts.Columns["old_col"]; ok {
		t.Error("old_col should have been dropped")
	}
	if _, ok := ts.Columns["id"]; !ok {
		t.Error("id should still exist")
	}
}

func TestRebuildFromMigrations(t *testing.T) {
	migs := []migrations.Migration{
		{
			ID: "002_add_email",
			Up: func(b *migrations.SchemaBuilder) {
				b.AddColumn("users", migrations.ColumnDef{Name: "email", SQLType: "TEXT"})
			},
		},
		{
			ID: "001_create_users",
			Up: func(b *migrations.SchemaBuilder) {
				b.CreateTable("users",
					migrations.ColumnDef{Name: "id", SQLType: "INTEGER"},
					migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
				)
			},
		},
	}

	// Migrations are out of order — RebuildFromMigrations must sort by ID
	schema := migrations.RebuildFromMigrations(migs)

	ts := schema.Tables["users"]
	if ts == nil {
		t.Fatal("missing users table")
	}
	if len(ts.Columns) != 3 {
		t.Errorf("columns = %d, want 3", len(ts.Columns))
	}
}

// --- Scaffold tests ---

func TestScaffoldSQLite(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create some tables
	_, err := db.ExecContext(ctx, `CREATE TABLE "products" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"name" TEXT NOT NULL,
		"price" REAL,
		"active" BOOLEAN DEFAULT 1
	)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE "categories" (
		"id" INTEGER PRIMARY KEY,
		"title" TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}

	results, err := migrations.Scaffold(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(results))
	}

	// Check products struct
	var prodResult *migrations.ScaffoldResult
	for i := range results {
		if results[i].TableName == "products" {
			prodResult = &results[i]
		}
	}
	if prodResult == nil {
		t.Fatal("missing products scaffold")
	}

	if prodResult.StructName != "Products" {
		t.Errorf("struct name = %q, want Products", prodResult.StructName)
	}
	if !strings.Contains(prodResult.Source, "type Products struct") {
		t.Errorf("missing struct declaration: %s", prodResult.Source)
	}
	if !strings.Contains(prodResult.Source, `db:"column:id`) {
		t.Errorf("missing id column tag: %s", prodResult.Source)
	}
	if !strings.Contains(prodResult.Source, "primary_key") {
		t.Errorf("missing primary_key tag: %s", prodResult.Source)
	}
}

func TestScaffoldEmptyDB(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	results, err := migrations.Scaffold(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 tables, got %d", len(results))
	}
}

func TestSqlTypeToGoMapping(t *testing.T) {
	// Verify scaffold generates correct Go types
	db := openTestDB(t)
	ctx := context.Background()

	_, _ = db.ExecContext(ctx, `CREATE TABLE "mixed" (
		"id" INTEGER PRIMARY KEY,
		"big" BIGINT,
		"price" REAL,
		"active" BOOLEAN,
		"name" TEXT,
		"data" BLOB
	)`)

	results, _ := migrations.Scaffold(ctx, db)
	if len(results) == 0 {
		t.Fatal("no results")
	}

	src := results[0].Source
	// int for INTEGER PK (not nullable)
	if !strings.Contains(src, "int") {
		t.Errorf("expected int type: %s", src)
	}
}

// --- MSSQL Dialect tests ---

func TestMSSQLDialect_BracketQuoting(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MSSQLDialect{})
	b.CreateTable("users",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
		migrations.ColumnDef{Name: "name", SQLType: "NVARCHAR(255)"},
	)

	stmts := b.Statements()
	s := stmts[0]
	if !strings.Contains(s, "[users]") {
		t.Errorf("expected bracket quoting: %s", s)
	}
	if !strings.Contains(s, "[id]") {
		t.Errorf("expected bracket quoting on column: %s", s)
	}
}

func TestMSSQLDialect_Identity(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MSSQLDialect{})
	b.CreateTable("users",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
	)

	s := b.Statements()[0]
	if !strings.Contains(s, "IDENTITY(1,1)") {
		t.Errorf("expected IDENTITY(1,1): %s", s)
	}
	if strings.Contains(s, "AUTOINCREMENT") {
		t.Errorf("should NOT contain AUTOINCREMENT: %s", s)
	}
}

func TestMSSQLDialect_NoIfNotExists(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MSSQLDialect{})
	b.CreateTable("t",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true},
	)

	s := b.Statements()[0]
	if strings.Contains(s, "IF NOT EXISTS") {
		t.Errorf("MSSQL should NOT use IF NOT EXISTS: %s", s)
	}
}

func TestMSSQLDialect_AddColumn(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MSSQLDialect{})
	b.AddColumn("users", migrations.ColumnDef{Name: "email", SQLType: "NVARCHAR(255)"})

	s := b.Statements()[0]
	if strings.Contains(s, "ADD COLUMN") {
		t.Errorf("MSSQL should use ADD (not ADD COLUMN): %s", s)
	}
	if !strings.Contains(s, "ADD [email]") {
		t.Errorf("expected ADD [email]: %s", s)
	}
}

func TestMSSQLDialect_CreateIndex_NoIfNotExists(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.MSSQLDialect{})
	b.CreateIndex("idx_email", "users", false, "email")

	s := b.Statements()[0]
	if strings.Contains(s, "IF NOT EXISTS") {
		t.Errorf("MSSQL should NOT use IF NOT EXISTS on index: %s", s)
	}
}

// --- GenerateSQLScript ---

func TestGenerateSQLScript_DefaultDialect(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.CreateTableOp{
			Table: "users",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
				{Name: "name", SQLType: "TEXT"},
			},
		},
		migrations.CreateIndexOp{Name: "idx_users_name", Table: "users", Columns: []string{"name"}},
	}

	sql := migrations.GenerateSQLScript(ops, nil)

	if !strings.Contains(sql, "CREATE TABLE") {
		t.Error("expected CREATE TABLE in SQL script")
	}
	if !strings.Contains(sql, "CREATE INDEX") {
		t.Error("expected CREATE INDEX in SQL script")
	}
	if !strings.Contains(sql, ";") {
		t.Error("expected semicolons")
	}
	if !strings.Contains(sql, "Auto-generated") {
		t.Error("expected header comment")
	}
}

func TestGenerateSQLScript_PostgresDialect(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.CreateTableOp{
			Table: "posts",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
				{Name: "title", SQLType: "VARCHAR(200)"},
			},
		},
	}

	sql := migrations.GenerateSQLScript(ops, migrations.PostgresDialect{})

	if !strings.Contains(sql, "SERIAL") {
		t.Error("expected SERIAL for Postgres auto-increment")
	}
}

func TestGenerateSQLScript_MSSQLDialect(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.CreateTableOp{
			Table: "items",
			Columns: []migrations.ColumnDef{
				{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
			},
		},
	}

	sql := migrations.GenerateSQLScript(ops, migrations.MSSQLDialect{})

	if !strings.Contains(sql, "IDENTITY(1,1)") {
		t.Error("expected IDENTITY(1,1) for MSSQL auto-increment")
	}
	if strings.Contains(sql, "IF NOT EXISTS") {
		t.Error("MSSQL should not use IF NOT EXISTS")
	}
}

func TestSQLScriptFileName(t *testing.T) {
	name := migrations.SQLScriptFileName("AddUsers")
	if !strings.HasSuffix(name, ".sql") {
		t.Errorf("expected .sql suffix, got %s", name)
	}
	if !strings.Contains(name, "add_users") {
		t.Errorf("expected snake_case name, got %s", name)
	}
}
