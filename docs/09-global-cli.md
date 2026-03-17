# Installing Wormhole as a Global CLI Tool

You can install wormhole globally just like Entity Framework's `dotnet ef` tool:

```bash
go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest
```

This makes the `wormhole` command available anywhere on your system.

## 🚀 NO CGO Required

Wormhole is built with **zero CGO dependencies**, making it:
- Cross-platform compatible out of the box
- Easy to deploy in containers and restricted environments
- Simple to install without C compiler requirements
- Fast to build and distribute

We use `github.com/glebarez/sqlite` (pure Go) instead of `github.com/mattn/go-sqlite3` (CGO-based).

## Quick Start

1. **Install globally:**
   ```bash
   go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest
   ```

2. **Navigate to your Go project** that contains models with `db` tags:
   ```go
   type User struct {
       ID   int    `db:"primary_key;auto_increment"`
       Name string `db:"column:name"`
       Email string `db:"column:email;nullable"`
   }
   ```

3. **Generate a migration:**
   ```bash
   export WORMHOLE_DSN="./app.db"
   wormhole migrations add CreateUserTable
   ```

4. **Apply migrations:**
   ```bash
   wormhole database update
   ```

## Environment Variables

- `WORMHOLE_DSN`: Database connection string (required)
- `WORMHOLE_DRIVER`: SQL driver name (default: sqlite)
- `WORMHOLE_DIR`: Migrations directory (default: ./migrations)

## Supported Workflows

### Like Entity Framework

```bash
# Generate migration from model changes
wormhole migrations add AddUserAge

# Apply pending migrations
wormhole database update

# List migration status
wormhole migrations list

# Generate SQL scripts
wormhole migrations script AddUserAge postgres
```

### Integration with Your Application

You can still use the programmatic API in your application:

```go
// In your application code
ctx := context.New(provider.Default())
ctx.Set(&users).Where(dsl.Eq(&u, &u.Active, true)).Execute()
err := ctx.Save()
```

## Auto-Discovery

Wormhole automatically discovers models with `db` tags in your current directory. Make sure your structs have proper wormhole annotations:

```go
type Product struct {
    ID       int       `db:"primary_key;auto_increment"`
    Name     string    `db:"column:name"`
    Price    float64   `db:"column:price;type:decimal(10,2)"`
    Category string    `db:"column:category;nullable"`
}
```

The CLI will:
1. Scan your project for Go structs with `db` tags
2. Compare against the current database schema
3. Generate migration operations
4. Create both `.go` and `.sql` migration files
5. Apply SQL migrations directly to your database

## Migration Files

Wormhole generates two files for each migration:

- `YYYYMMDDHHMMSS_MigrationName.go` - For programmatic use
- `YYYYMMDDHHMMSS_MigrationName.sql` - For CLI execution

This dual approach ensures compatibility with both programmatic and CLI workflows.

## Benefits Over Manual Implementation

- **No CGO**: Pure Go, cross-platform compatible
- **No boilerplate**: No need to implement migration runners in your app
- **Global availability**: Install once, use in any project
- **Auto-discovery**: Automatically finds your models
- **Dual compatibility**: Works with both CLI and programmatic approaches
- **Entity Framework-like**: Familiar workflow for .NET developers