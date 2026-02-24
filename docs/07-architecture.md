# Architecture & Internals

This document explains the internal architecture of `go-wormhole` for
contributors and advanced users.


## Layer Diagram

```
┌──────────────────────────────────────────────────────┐
│                   User Code                          │
│   ctx.Set(&users).Where(dsl.Gt(&u, &u.Age, 18))     │
└───────────────────────┬──────────────────────────────┘
                        │
┌───────────────────────▼──────────────────────────────┐
│                   DbContext                          │
│   Unit of Work · Lifecycle Hooks · Retry · Breaker   │
├──────────────────────────────────────────────────────┤
│   Tracker (Identity Map + Snapshot Diff)              │
├──────────────────────────────────────────────────────┤
│   DSL (Pointer-Tracking → Predicate AST)             │
├──────────────────────────────────────────────────────┤
│   Query Builder (Provider-Neutral AST)               │
└───────────────────────┬──────────────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
┌─────────────┐  ┌────────────┐  ┌──────────┐
│ SQL Provider│  │ Slipstream │  │ Future   │
│ (Postgres,  │  │ (Bitcask)  │  │ Provider │
│  SQLite,    │  │            │  │          │
│  MySQL)     │  │            │  │          │
└─────────────┘  └────────────┘  └──────────┘
```


## Package Map

```
go-wormhole/
├── cmd/wormhole/         CLI tool (migrations, scaffold)
├── pkg/
│   ├── context/          DbContext, EntitySet, generics, DI
│   ├── dsl/              Pointer-tracking DSL (register, operators)
│   ├── migrations/       Differ, Builder, Runner, Codegen, Dialects
│   ├── model/            EntityMeta, FieldMeta, Entry, State
│   ├── provider/         Provider interface, Registry
│   ├── query/            AST types (Predicate, Sort, Query, Builder)
│   ├── schema/           Struct tag parser → EntityMeta
│   ├── slipstream/       Slipstream (NoSQL) provider
│   ├── sql/              SQL provider + compiler
│   └── tracker/          Change Tracker (Identity Map, diff)
└── docs/                 This documentation
```


## go-foundation Integration Map

`go-wormhole` builds on `go-foundation` primitives. Here's exactly where
each is used:

| go-foundation Package    | Used In                    | Purpose                                |
|--------------------------|----------------------------|----------------------------------------|
| `pkg/adapters.Registry`  | `provider/registry.go`     | Multi-backend provider registry        |
| `pkg/di.Container`       | `context/di.go`            | Dependency injection for DbContext     |
| `pkg/tags.Parser`        | `schema/parser.go`, `dsl/` | Parse `db:"..."` struct tags           |
| `pkg/safemap.ShardedMap` | `tracker/tracker.go`       | Identity Map (32 concurrent shards)    |
| `pkg/hooks.Discovery`    | `context/dbcontext.go`     | Auto-discover `BeforeSave()` etc.      |
| `pkg/hooks.Runner`       | `context/dbcontext.go`     | Execute pre/post event hooks           |
| `pkg/errors.MultiError`  | `context/dbcontext.go`     | Collect multiple validation errors     |
| `pkg/resiliency.Retry`   | `sql/provider.go`, `context/` | Retry with exponential backoff      |
| `pkg/resiliency.CB`      | `context/dbcontext.go`     | Circuit breaker for DB calls           |


## Key Design Decisions

### 1. Pointer Arithmetic over Code Generation

Traditional ORMs generate type-safe code files (`User_query.go`). We chose
`unsafe.Pointer` arithmetic instead:

- **Pro:** No build step, no stale files, instant IDE refactoring support
- **Pro:** Zero allocation in the hot path (map lookup only)
- **Con:** Requires `unsafe` package (accepted trade-off)
- **Con:** Must call `Register()` at boot (compile-time verified via panic)

### 2. Type-Erasure for Slipstream

Slipstream uses `Engine[map[string]any]` — all entities are serialized
as JSON maps. This loses type information at storage level, but the
`scanInto` function reconstructs typed structs via reflection.

Why not `Engine[User]`? Because the provider must handle **any** entity
type through a single engine instance.

### 3. Dynamic Column Scanning (SQL)

The SQL provider scans rows by **column name**, not position:

```go
cols, _ := rows.Columns()
for i, col := range cols {
    if field, ok := fieldMap[col]; ok {
        scanDest[i] = fieldAddr(field)
    } else {
        scanDest[i] = &discard  // extra column from JOIN
    }
}
```

This makes JOINs safe — extra columns are silently discarded instead of
causing a scan error.

### 4. Partial Updates Only

The Change Tracker computes `ChangedFields()` and the SQL compiler emits
`UPDATE SET` with **only** the modified columns. This prevents the
classic concurrent-write race condition where one goroutine overwrites
another's changes.

### 5. Snapshot as DB Query (not File)

Unlike EF Core's `ModelSnapshot.cs`, we query the live database to build
the current `DatabaseSchema` for diffing. This is:

- **Safer:** no desync between local files and actual DB state
- **Simpler:** no snapshot file to maintain
- **Trade-off:** requires DB connection for `migrations add`

The file-based snapshot is available via `RebuildSnapshot()` for offline
use cases.


## Data Flow: SaveChanges

```
1. DetectChanges()
   └─ For each tracked entity:
      └─ Compare current field values vs snapshot
      └─ If different → mark as Modified

2. Pending()
   └─ Return all entries with State ∈ {Added, Modified, Deleted}

3. Hook Discovery
   └─ reflect on each entity → find BeforeSave(), BeforeInsert(), etc.
   └─ Call them, collect errors in MultiError
   └─ If any error → abort (no DB call)

4. Transaction
   └─ provider.Begin(ctx)
   └─ For each entry:
       ├─ Added    → compiler.Insert(meta, entity) → tx.Exec
       ├─ Modified → compiler.Update(meta, entity, changedFields) → tx.Exec
       └─ Deleted  → compiler.Delete(meta, pkValue) → tx.Exec
   └─ tx.Commit() (or Rollback on error)

5. AcceptAll()
   └─ Re-snapshot all entities
   └─ Reset all states to Unchanged
```


## Data Flow: Query

```
1. dsl.Gt(&u, &u.Age, 18)
   └─ Pointer math: offset = &u.Age - &u
   └─ Registry lookup: offset → fieldInfo{Column: "age"}
   └─ Return Predicate{Field: "age", Op: OpGt, Value: 18}

2. Builder.Filter(pred).OrderBy("age", Desc).Limit(10).Build()
   └─ Return Query{Entity: "users", Filters: [...], Sorts: [...], Limit: 10}

3. Provider.Execute(ctx, meta, query, &dest)
   └─ SQL Compiler: SELECT "id","name","age" FROM "users" WHERE "age" > ? ...
   └─ db.QueryContext(ctx, sql, args...)
   └─ scanRows(rows, meta, &dest)

4. Tracker.Attach(each result)
   └─ Snapshot taken for future dirty detection
```


## Module Dependencies

```
go-wormhole
├── github.com/mirkobrombin/go-foundation v0.3.0    (core primitives)
├── github.com/mirkobrombin/go-slipstream v1.0.1    (NoSQL engine)
└── github.com/glebarez/sqlite v1.11.0              (test only)
```

Go version: **1.24.4+** (requires generics + `unsafe.Pointer` arithmetic).


## Test Coverage

| Package      | Tests | Coverage Area                              |
|--------------|-------|--------------------------------------------|
| `context/`   | 6     | DbContext, EntitySet, DI wiring            |
| `dsl/`       | 10    | Register, all operators, edge cases        |
| `migrations/`| 25    | Differ, builder, runner, dialects, scaffold|
| `sql/`       | 12    | Compiler (SELECT/INSERT/UPDATE/DELETE/JOIN) |
| `sql/` (E2E) | 18    | Full roundtrip with real SQLite            |
| **Total**    | **71**| All critical paths                         |
