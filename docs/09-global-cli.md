# Global CLI Tool

The go-wormhole CLI provides an Entity Framework-like experience for Go developers. Unlike Entity Framework which requires providers to be installed separately, wormhole uses **your project's database drivers** via build tags.

## Installation

```bash
go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest
```

## 🚀 NO CGO Required

Wormhole CLI has **zero database driver dependencies**, making it:
- Cross-platform compatible out of the box
- Easy to deploy in containers and restricted environments  
- Simple to install without C compiler requirements
- Uses **your project's** database drivers, not its own

## Quick Start

1. **Initialize in your project:**
   ```bash
   wormhole init
   ```

2. **Import your database drivers:**
   Edit `wormhole_migrations_gen.go`:
   ```go
   // Import your database drivers here
   _ "github.com/lib/pq"                    // PostgreSQL
   _ "github.com/go-sql-driver/mysql"       // MySQL  
   _ "github.com/denisenkom/go-mssqldb"     // SQL Server
   _ "github.com/glebarez/sqlite"           // SQLite (pure Go)
   ```

3. **Define your models:**
   ```go
   type User struct {
       ID   int    `db:"primary_key;auto_increment"`
       Name string `db:"column:name"`
       Age  int    `db:"column:age"`
   }
   ```

4. **Generate migration:**
   ```bash
   wormhole migrations add CreateUser
   ```

5. **Apply migrations:**
   ```bash
   wormhole database update
   ```

## How It Works

The CLI uses build tags to compile **your project** with **your database drivers**:

1. `wormhole init` creates `wormhole_migrations_gen.go` in your project
2. This file has the build tag `//go:build wormhole_gen_migrations` 
3. When you run `wormhole migrations add`, it executes:
   ```bash
   go build -tags wormhole_gen_migrations -o temp_migrator .
   ./temp_migrator -action=add -name=CreateUser
   rm temp_migrator
   ```

This is **exactly like Entity Framework**: it uses your project's providers, not global ones.

## Environment Variables

- `WORMHOLE_DSN`: Database connection string (required)
- `WORMHOLE_DRIVER`: SQL driver name (default: sqlite)
- `WORMHOLE_DIR`: Migrations directory (default: ./migrations)

## Database Provider Configuration

### SQLite (Default)
```bash
export WORMHOLE_DRIVER=sqlite
export WORMHOLE_DSN=./myapp.db
```

### PostgreSQL
```bash
export WORMHOLE_DRIVER=postgres
export WORMHOLE_DSN="host=localhost user=postgres password=mypassword dbname=myapp sslmode=disable"
```

### MySQL
```bash
export WORMHOLE_DRIVER=mysql
export WORMHOLE_DSN="user:password@tcp(localhost:3306)/myapp?parseTime=true"
```

### SQL Server
```bash
export WORMHOLE_DRIVER=sqlserver
export WORMHOLE_DSN="server=localhost;user id=sa;password=MyPassword;database=myapp"
```

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