# go-wormhole

An Entity Framework-inspired ORM / Data Mapper for Go, built on top of
[go-foundation](https://github.com/mirkobrombin/go-foundation).

**Type-safe queries · Zero code generation · Dual SQL/NoSQL backends · Code-First Migrations**

```go
dsl.Register(User{})

ctx := wh.New(provider.Default())
var u User
ctx.Set(&u).Find(42)
u.Age = 35
ctx.Save()  // → UPDATE "users" SET "age" = ? WHERE "id" = ?
```

## Features

- **Pointer-tracking DSL** — compile-time type-safe queries, no code generation
- **Change Tracker** — Unit of Work with partial UPDATE (only changed columns)
- **SQL + NoSQL** — pluggable providers for PostgreSQL, SQLite, MySQL, Slipstream (Bitcask)
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

## License

MIT
