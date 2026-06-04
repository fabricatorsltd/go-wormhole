# Concurrency and DbContext Lifetime

`DbContext` is a **unit of work**, the same role it plays in Entity Framework
Core. It owns a change tracker, stages your inserts, updates, and deletes, and
flushes them together on `Save`. Like EF Core's `DbContext`, it is scoped to one
logical operation, not shared for the lifetime of the process.

The short rule: **create one `DbContext` per request (or per unit of work).
Share the provider and the database handle, not the context.**

## What is safe to share

These are built for concurrent use and should be created once at startup:

- The `*sql.DB` handle. It is a connection pool and is safe for concurrent use.
- The provider (`wsql.New(db)`). It is a thin, stateless wrapper over the pool.
- The DSL registry (`dsl.Register`). It is populated once at startup and is
  read-only afterwards.

## Why a shared DbContext breaks writes

The change tracker stores entities in a concurrent (sharded) map, so multiple
goroutines will not corrupt the map itself. The problem is one level up: the
tracker is a single shared unit of work.

`Save` detects changes across **every** tracked entity, flushes them in one
transaction, and then marks them all unchanged. Two HTTP requests sharing one
`DbContext` therefore stage into the same pending set, and the first request to
call `Save` commits and clears the second request's not-yet-finished changes.
That is silent data corruption, not a panic, which makes it the worst kind of
bug to find in production.

Reads that bypass the tracker do not have this problem (see the table below),
which is why a read-only shared context can look fine right up until the first
concurrent write.

## The per-request pattern

Build the provider once, then open a fresh context per request and let it go out
of scope when the request ends:

```go
// startup: shared, pool-backed, safe for all requests
db, _ := sql.Open("sqlite", dsn)
prov := wsql.New(db)

// per request: a fresh unit of work for this request only
func handler(w http.ResponseWriter, r *http.Request) {
    ctx := wh.New(prov)

    var u User
    if err := ctx.Set(&u).Find(idFrom(r)); err != nil { /* ... */ }
    u.LastSeen = time.Now()
    if err := ctx.Save(); err != nil { /* ... */ }
}
```

`wh.New` allocates a struct and a fresh tracker. It is cheap, so creating one per
request is the intended cost, not a thing to optimize away.

### Do not Close a per-request context

`DbContext.Close` closes the underlying provider, which closes the shared
`*sql.DB`. Calling it from a request handler would shut the pool for every other
request.

```go
ctx := wh.New(prov)
// no defer ctx.Close() here: prov and its *sql.DB are shared.
// The context and its tracker are released by GC when the request ends.
```

Reserve `Close` for a context that owns its database for its whole life, such as
a one-shot CLI script or a test, where closing the pool on exit is correct.

## API reference: what touches the tracker

Anything stateless can run on a shared singleton. Anything that touches the
tracker must be confined to a per-request context.

| API | Touches the tracker | Safe on a shared singleton |
|---|---|---|
| `Query[T](c).Exec` | no | yes |
| `Stream[T]` / `EntitySet.Stream` | no | yes |
| `Set(&slice).All()` (collections are non-tracking by default) | no | yes |
| `Set(&e).NoTracking().Find(pk)` | no | yes |
| `Set(...).Update(...)` / `.Delete()` (set-based, no load) | no | yes |
| `Upsert` | no | yes |
| `Transaction(ctx, fn)` | no | yes |
| `Add` / `Remove` / `Save` | yes | no |
| `Set(&e).Find(pk)` (tracked by default) | yes | no |
| `Set(...).AsTracking().All()` | yes | no |

## If you want a long-lived singleton anyway

Restrict it to the stateless APIs above. Set-based `Update`/`Delete`, `Upsert`,
`Transaction`, and no-tracking reads never touch the tracker, so a shared
context is fine for single-statement write paths (for example, upserting a
session row) where you do not need unit-of-work ergonomics.

## Anti-pattern: a mutex around Save

Guarding `Save` with a mutex serializes every write in the process and still
leaves the commingling problem: requests continue to stage into one shared
pending set, so a serialized `Save` can still flush another request's work. Use
a per-request context instead.
