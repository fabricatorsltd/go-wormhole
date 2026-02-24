# Getting Started

Install `go-wormhole` and write your first query in 3 steps.

## Installation

```bash
go get github.com/mirkobrombin/go-wormhole@latest
```

## Step 1 — Define Your Entity

Your Go struct **is** your database schema. Use `db:"..."` tags to control
the mapping:

```go
type User struct {
    ID    int    `db:"primary_key; auto_increment"`
    Name  string `db:"type:varchar(100)"`
    Email string `db:"type:varchar(255); nullable; index:idx_users_email"`
    Age   int
}
```

No tag? The framework defaults to snake_case (`Age` → `age`, `UserID` → `user_id`).

### Supported Tags

| Tag               | Example                  | Effect                          |
|-------------------|--------------------------|---------------------------------|
| `primary_key`     | `primary_key`            | Marks as primary key            |
| `auto_increment`  | `auto_increment`         | Auto-incrementing PK            |
| `column`          | `column:user_name`       | Override column name            |
| `type`            | `type:varchar(255)`      | Explicit SQL type               |
| `nullable`        | `nullable`               | Allow NULL values               |
| `index`           | `index:idx_email`        | Create secondary index          |
| `default`         | `default:'active'`       | SQL DEFAULT expression          |


## Step 2 — Bootstrap the Framework

At the boot of your application, register your entities with the DSL
and wire up a provider:

```go
package main

import (
    "database/sql"

    _ "github.com/lib/pq" // or github.com/glebarez/sqlite

    "github.com/mirkobrombin/go-wormhole/pkg/dsl"
    wormholesql "github.com/mirkobrombin/go-wormhole/pkg/sql"
)

func init() {
    // Pre-calculate memory offsets for the pointer-tracking DSL.
    // This is a one-time cost at startup.
    dsl.Register(User{})
    dsl.Register(Order{})
}

func main() {
    db, _ := sql.Open("sqlite", "app.db")

    // Register the SQL provider as the default backend
    wormholesql.RegisterDefault(db)
}
```

> **Why `dsl.Register`?**
> It parses struct tags and pre-computes field memory offsets.
> This powers the zero-allocation, type-safe query DSL at runtime.


## Step 3 — Query and Mutate

```go
import (
    wh "github.com/mirkobrombin/go-wormhole/pkg/context"
    "github.com/mirkobrombin/go-wormhole/pkg/dsl"
    "github.com/mirkobrombin/go-wormhole/pkg/provider"
    "github.com/mirkobrombin/go-wormhole/pkg/query"
)

func handler() {
    ctx := wh.New(provider.Default())
    defer ctx.Close()

    // Fetch by PK (auto-tracked as Unchanged)
    var u User
    ctx.Set(&u).Find(42)

    // Mutate in memory — no ORM method needed
    u.Age = 35

    // Flush: Wormhole detects the change and emits
    // UPDATE "users" SET "age" = ? WHERE "id" = ?
    ctx.Save()

    // Query with the type-safe DSL
    var users []User
    ctx.Set(&users).
        Where(dsl.Gt(&u, &u.Age, 18)).
        OrderBy("age", query.Desc).
        Limit(10).
        All()

    // Insert a new entity
    alice := &User{Name: "Alice", Age: 30}
    ctx.Add(alice)
    ctx.Save()
}
```

That's it. Three steps. No XML, no YAML, no code generation.


## What's Next?

| Topic                          | Page                                              |
|--------------------------------|---------------------------------------------------|
| The pointer-tracking DSL       | [02-pointer-tracking-dsl.md](02-pointer-tracking-dsl.md) |
| Change Tracker & Unit of Work  | [03-change-tracker.md](03-change-tracker.md)      |
| Code-First Migrations          | [04-migrations.md](04-migrations.md)              |
| SQL & NoSQL Providers          | [05-providers.md](05-providers.md)                |
| Resilience (Retry & Breaker)   | [06-resilience.md](06-resilience.md)              |
| Architecture & Internals       | [07-architecture.md](07-architecture.md)          |
