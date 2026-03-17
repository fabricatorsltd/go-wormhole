# go-wormhole

An Entity Framework-inspired ORM / Data Mapper for Go, built on top of
[go-foundation](https://github.com/mirkobrombin/go-foundation).

**Type-safe queries · Zero code generation · Dual SQL/NoSQL backends · Code-First Migrations · NO CGO**

```go
dsl.Register(User{})

ctx := wh.New(provider.Default())
var u User
ctx.Set(&u).Find(42)
u.Age = 35
ctx.Save()  // → UPDATE "users" SET "age" = ? WHERE "id" = ?
```

## ⚡ Install as Global CLI Tool

Install once, use everywhere like Entity Framework:

```bash
go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest

# Use in any Go project with models
export WORMHOLE_DSN="./app.db"
wormhole migrations add CreateUserTable
wormhole database update
```

## Features

- **🚀 NO CGO** — Pure Go, cross-platform compatible, easy deployment
- **Global CLI Tool** — Install once with `go install`, use anywhere like `dotnet ef`
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

## License

MIT
