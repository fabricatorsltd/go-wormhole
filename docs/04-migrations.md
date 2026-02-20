# Code-First Migrations

`go-wormhole` uses your Go structs as the **single source of truth** for your
database schema. Add a field → generate a migration → apply it. No manual SQL.

This is the Go equivalent of Entity Framework Core's `Add-Migration` /
`Update-Database` workflow.


## Quick Start

### 1. Generate a Migration

Add a new field to your struct:

```go
type User struct {
    ID    int    `db:"primary_key; auto_increment"`
    Name  string `db:"type:varchar(100)"`
    Email string `db:"type:varchar(255); nullable"`  // ← NEW FIELD
    Age   int
}
```

Then run:

```bash
wormhole migrations add AddUserEmail
```

The engine detects the new field, computes a diff against the current schema,
and generates a timestamped `.go` file:

```
migrations/20260220120000_add_user_email.go
```

### 2. Apply to Database

```bash
WORMHOLE_DSN="postgres://localhost/myapp" wormhole database update
```

This executes all pending `Up()` methods and records each migration in the
`_wormhole_migrations_history` table.

### 3. Scaffold (Reverse Engineering)

Already have a database? Generate Go structs from it:

```bash
WORMHOLE_DSN="file:app.db" wormhole dbcontext scaffold
```

This reads the database schema and generates structs with `db:"..."` tags
in a `models/` directory. Works with PostgreSQL, MySQL, and SQLite.


## Generated Migration File

Every generated file follows this pattern:

```go
package migrations

import "github.com/mirkobrombin/go-wormhole/pkg/migrations"

func init() {
    Register(migrations.Migration{
        ID: "20260220120000_add_user_email",
        Up: func(b *migrations.SchemaBuilder) {
            b.AddColumn("users", migrations.ColumnDef{
                Name: "email", SQLType: "varchar(255)", Nullable: true,
            })
        },
        Down: func(b *migrations.SchemaBuilder) {
            b.DropColumn("users", "email")
        },
    })
}
```

- **`Up()`** applies the migration forward
- **`Down()`** reverses it
- Both are auto-populated by the differ — you can edit them manually


## The Differ Engine

The `ComputeDiff()` function is the brain of the migration system.
It compares your Go struct metadata against the current database schema
and emits a list of `MigrationOp` objects:

```go
ops := migrations.ComputeDiff(targetModels, currentSchema)
```

### Detected Changes

| Change                         | Emitted Operation     |
|-------------------------------|-----------------------|
| New struct (table not in DB)   | `CreateTableOp`       |
| New field (column not in DB)   | `AddColumnOp`         |
| Removed field                  | `DropColumnOp`        |
| Struct removed from code       | `DropTableOp`         |
| Type change (int → bigint)     | `AlterColumnOp`       |
| New index tag                  | `CreateIndexOp`       |

### Data Loss Warnings

When the differ detects destructive operations (`DropTableOp` or
`DropColumnOp`), the CLI prints a yellow warning:

```
WARNING: This migration drops column "users"."email" — potential data loss!
```


## The SchemaBuilder

The `SchemaBuilder` translates migration operations into SQL, using a
pluggable `Dialect` interface:

```go
type Dialect interface {
    QuoteIdent(s string) string          // "users" vs `users`
    AutoIncrementClause() string         // AUTOINCREMENT vs AUTO_INCREMENT
    AutoIncrementType(base string) string // INTEGER → SERIAL (Postgres)
    SupportsIfNotExists() bool
}
```

### Available Dialects

| Dialect             | Identifier Quoting | Auto-Increment      | Notes                    |
|--------------------|--------------------|---------------------|--------------------------|
| `DefaultDialect`   | `"double quotes"`  | `AUTOINCREMENT`     | SQLite compatible        |
| `PostgresDialect`  | `"double quotes"`  | `SERIAL` / `BIGSERIAL` | Type-level auto-incr  |
| `MySQLDialect`     | `` `backticks` ``  | `AUTO_INCREMENT`    | ⚠️ Implicit DDL COMMIT   |

Usage:

```go
builder := migrations.NewBuilderWith(migrations.PostgresDialect{})
```

### ⚠️ MySQL DDL Caveat

MySQL causes an **implicit COMMIT** on any DDL statement (`CREATE TABLE`,
`ALTER TABLE`, etc.). If a migration has 3 DDL commands and fails on the
second, the first **cannot be rolled back**. This is a MySQL limitation,
not a `go-wormhole` bug.


## History Table

The `_wormhole_migrations_history` table tracks which migrations have been
applied:

```sql
CREATE TABLE IF NOT EXISTS _wormhole_migrations_history (
    migration_id VARCHAR(255) PRIMARY KEY,
    applied_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

The `Runner` reads this table before executing, skips already-applied
migrations, and inserts a record after each successful `Up()`.


## Runner (Programmatic API)

For full control, use the `Runner` directly in your Go code:

```go
import "github.com/mirkobrombin/go-wormhole/pkg/migrations"

// Register migrations (typically via init() in migration files)
migrations.Register(migrations.Migration{
    ID: "20260220120000_initial",
    Up: func(b *migrations.SchemaBuilder) {
        b.CreateTable("users",
            migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
            migrations.ColumnDef{Name: "name", SQLType: "varchar(100)"},
        )
    },
    Down: func(b *migrations.SchemaBuilder) {
        b.DropTable("users")
    },
})

// Create runner and apply
runner := migrations.NewRunner(db)
err := runner.Up(ctx) // applies all pending migrations
```


## Snapshot Rebuild

To reconstruct the cumulative schema without a DB connection
(e.g. for offline diffing), use `RebuildSnapshot`:

```go
schema := migrations.RebuildSnapshot()
```

This replays all registered `Up()` methods against a dummy builder,
building the `DatabaseSchema` from the operations.


## CLI Reference

| Command                          | Description                          |
|----------------------------------|--------------------------------------|
| `wormhole migrations add <Name>` | Generate a new migration file        |
| `wormhole migrations list`       | Show pending/applied migrations      |
| `wormhole database update`       | Apply pending migrations             |
| `wormhole dbcontext scaffold`    | Reverse-engineer DB → Go structs     |

### Environment Variables

| Variable          | Default   | Description                |
|-------------------|-----------|----------------------------|
| `WORMHOLE_DSN`    | —         | Database connection string |
| `WORMHOLE_DRIVER` | `sqlite3` | SQL driver name            |
| `WORMHOLE_DIR`    | `./migrations` | Migration files dir   |
