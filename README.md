# go-wormhole

An Entity Framework-inspired ORM / Data Mapper for Go, built on top of [go-foundation](https://github.com/mirkobrombin/go-foundation).

**Type-safe queries • Zero code generation • Dual SQL/NoSQL backends • Code-First Migrations**

```go
dsl.Register(User{})

ctx := wh.New(provider.Default())
var u User
ctx.Set(&u).Find(42)
u.Age = 35
ctx.Save() // → UPDATE "users" SET "age" = ? WHERE "id" = ?
```

## Features

- **Global CLI Tool** - Entity Framework-like CLI, runs via build flag or standalone binary
- **Auto-Discovery** — Automatically finds models with `db` tags in your project
- **Pointer-tracking DSL** — compile-time type-safe queries, no code generation
- **Change Tracker** — Unit of Work with partial UPDATE (only changed columns)
- **SQL + NoSQL** — pluggable providers for PostgreSQL, SQLite, MySQL, MongoDB, Slipstream (Bitcask), MemDoc (in-memory)
- **Cross-Engine Sync** — migrate data between different engines (e.g. MSSQL to Postgres) with identity/sequence handling
- **Engine-Specific Naming** — automatic mapping between conventions (PascalCase for MSSQL, snake_case for others)
- **Code-First Migrations** — EF Core-style differ, runner, CLI, scaffold, multi-dialect DDL
- **Resilience** — retry with backoff, circuit breaker, aggregated MultiError
- **Lifecycle Hooks** — `BeforeSave()`, `AfterInsert()` auto-discovered via reflection
- **DI-ready** — first-class `go-foundation/pkg/di` integration

## Quick Start

### 1. Define your models
```go
type User struct {
    ID   int    `db:"primary_key;auto_increment"`
    Name string `db:"column:name"`
    Age  int    `db:"column:age"`
}
```

### 2. Set up your database connection
```bash
export WORMHOLE_DSN="./myapp.db"
export WORMHOLE_DRIVER="sqlite"   # or postgres, mysql, sqlserver
```

### 3. Run migrations

If your project imports go-wormhole as a library, use the `-tags wormhole_cli` build flag
to run wormhole commands directly from your project, no extra setup needed:

```bash
go run -tags wormhole_cli . migrations add CreateUser
go run -tags wormhole_cli . database update
go run -tags wormhole_cli . migrations list
```

Or install the standalone CLI:

```bash
go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest
wormhole migrations add CreateUser
wormhole database update
```

## Documentation

| Chapter | Topic |
|---------|-------|
| [01 — Getting Started](docs/01-getting-started.md) | Install, define entities, first query in 3 steps |
| [02 — Pointer-Tracking DSL](docs/02-pointer-tracking-dsl.md) | How it works, all operators, type safety |
| [03 — Change Tracker](docs/03-change-tracker.md) | Unit of Work, snapshots, partial updates, hooks |
| [04 — Code-First Migrations](docs/04-migrations.md) | Differ, runner, CLI, scaffold, dialects |
| [05 — Providers](docs/05-providers.md) | SQL provider, Slipstream, multi-backend setup |
| [06 — Resilience](docs/06-resilience.md) | Retry, circuit breaker, MultiError |
| [07 — Architecture](docs/07-architecture.md) | Internals, data flows, design decisions |
| [08 — Relationships](docs/08-relationships.md) | 1:1, 1:N, N:M declarations, eager loading with Include |
| [09 — Global CLI Tool](docs/09-global-cli.md) | Entity Framework-like CLI experience |

## Entity Framework Comparison

| Entity Framework | go-wormhole |
|------------------|-------------|
| `dotnet ef migrations add` | `wormhole migrations add` |
| `dotnet ef database update` | `wormhole database update` |
| `dotnet ef migrations list` | `wormhole migrations list` |
| `dotnet ef migrations script` | `wormhole migrations script` |
| `dotnet ef dbcontext scaffold` | `wormhole dbcontext scaffold` |

### Database Provider Examples

```bash
# PostgreSQL
export WORMHOLE_DRIVER=postgres
export WORMHOLE_DSN="host=localhost user=postgres dbname=myapp sslmode=disable"

# MySQL
export WORMHOLE_DRIVER=mysql  
export WORMHOLE_DSN="user:password@tcp(localhost:3306)/myapp?parseTime=true"

# SQL Server
export WORMHOLE_DRIVER=sqlserver
export WORMHOLE_DSN="server=localhost;user id=sa;database=myapp"

# SQLite (default)
export WORMHOLE_DRIVER=sqlite
export WORMHOLE_DSN=./myapp.db
```

## How It Works

Building with `-tags wormhole_cli` activates the CLI inside `DbContext.New()` via a
build-tag-gated method. Execution is intercepted and the wormhole CLI runs before your
`main()` logic continues. No files are generated in your project and no code changes
are needed.

## License

MIT