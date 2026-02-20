# Change Tracker & Unit of Work

The Change Tracker is the memory of `go-wormhole`. It watches your entities,
detects what changed, and flushes only the minimum necessary SQL.

## The Pattern

This is the **Unit of Work** pattern, made famous by Entity Framework and
Hibernate. The idea is simple:

1. Load entities from the DB → they enter the tracker as **Unchanged**
2. Modify them in plain Go code → no ORM calls needed
3. Call `SaveChanges()` → the tracker diffs every entity against its
   snapshot and emits the minimal set of `INSERT` / `UPDATE` / `DELETE`

```go
ctx := wh.New(provider.Default())

// Step 1: Load (tracked as Unchanged)
var u User
ctx.Set(&u).Find(42)

// Step 2: Mutate (just plain Go)
u.Age = 35
u.Email = "new@email.com"

// Step 3: Flush
ctx.Save()
// → UPDATE "users" SET "age" = ?, "email" = ? WHERE "id" = ?
// Only the 2 changed columns. Not all 10.
```

## Entity States

Every tracked entity is in one of 4 states:

| State       | Meaning                        | Trigger                    |
|-------------|--------------------------------|----------------------------|
| `Unchanged` | Clean — matches the DB         | `Attach()`, after `Save()` |
| `Added`     | New — pending INSERT           | `Add()`                    |
| `Modified`  | Dirty — pending partial UPDATE | Auto-detected by diff      |
| `Deleted`   | Marked for DELETE              | `Remove()`                 |

State transitions:

```
  [new entity] ──Add()──→ Added ──SaveChanges()──→ Unchanged
  [from DB]    ──Find()──→ Unchanged ──mutate──→ Modified ──SaveChanges()──→ Unchanged
  [any state]  ──Remove()──→ Deleted ──SaveChanges()──→ (removed from tracker)
```


## How Dirty Detection Works

When an entity enters the tracker, a **snapshot** is taken — a
`map[string]any` of every field's current value:

```go
// Internal: taken at Attach/Add time
snapshot := map[string]any{
    "ID":    42,
    "Name":  "Alice",
    "Age":   30,
    "Email": "alice@old.com",
}
```

When `SaveChanges()` is called, `DetectChanges()` compares every field's
**current** value against the snapshot using `reflect.DeepEqual`:

```go
func ChangedFields(e *model.Entry) []string {
    var changed []string
    val := reflect.ValueOf(e.Entity).Elem()
    for _, f := range e.Meta.Fields {
        cur := val.FieldByName(f.FieldName).Interface()
        old := e.Snapshot[f.FieldName]
        if !reflect.DeepEqual(cur, old) {
            changed = append(changed, f.FieldName)
        }
    }
    return changed
}
```

The resulting `[]string{"Age", "Email"}` is passed to the SQL compiler,
which generates `UPDATE users SET "age" = ?, "email" = ? WHERE "id" = ?`.

**This eliminates race conditions** where two goroutines modify different
fields of the same row — each UPDATE touches only its own columns.


## Partial Updates in Action

```go
// Goroutine A                      // Goroutine B
u.Age = 35                          u.Email = "new@test.com"
ctx.Save()                          ctx.Save()

// SQL emitted:
// A: UPDATE users SET "age" = 35 WHERE id = 42
// B: UPDATE users SET "email" = 'new@test.com' WHERE id = 42
// No conflict!
```

Compare with a naive ORM that always updates all columns:

```sql
-- Both goroutines would emit:
UPDATE users SET age=35, email='old@test.com', name='Alice' WHERE id=42
UPDATE users SET age=30, email='new@test.com', name='Alice' WHERE id=42
-- Goroutine B overwrites A's age change!
```


## Identity Map

The tracker uses `go-foundation/pkg/safemap.ShardedMap` (32 shards) as
an identity map. Each entity is keyed by `{tableName}#{pkValue}`:

```
users#42  →  Entry{Entity: &u, State: Unchanged, Snapshot: {...}}
users#99  →  Entry{Entity: &v, State: Modified, Snapshot: {...}}
```

This means:
- Loading the same PK twice returns the **same pointer** — no duplicates
- 32 shards minimize lock contention under concurrent access


## Lifecycle Hooks

You can run validation or formatting logic right before an entity is
saved. Just implement the method on your struct:

```go
func (u *User) BeforeSave() error {
    if u.Name == "" {
        return fmt.Errorf("name is required")
    }
    u.Name = strings.TrimSpace(u.Name)
    return nil
}

func (u *User) AfterInsert() {
    log.Printf("User %d created", u.ID)
}
```

The `DbContext` discovers these methods automatically via
`go-foundation/pkg/hooks.Discovery` and runs them at the right time:

- `Before*` hooks run **before** the transaction opens
- If any `Before*` hook returns an error, all errors are collected in a
  `MultiError` and the save is aborted
- `After*` hooks run **after** the commit succeeds


## The Full SaveChanges Flow

```
SaveChanges(ctx)
  │
  ├─ 1. tracker.DetectChanges()          compare snapshots
  ├─ 2. tracker.Pending()                collect Added/Modified/Deleted
  ├─ 3. hooks.Discover("Before*")        find BeforeSave, BeforeInsert, etc.
  ├─ 4. call Before* hooks               collect errors in MultiError
  │     └─ if errors → return MultiError (no DB call)
  ├─ 5. provider.Begin(ctx)              open transaction
  ├─ 6. for each entry:
  │     ├─ Added    → tx.Insert(...)
  │     ├─ Modified → tx.Update(..., changedFields)
  │     └─ Deleted  → tx.Delete(...)
  ├─ 7. tx.Commit()                      (or Rollback on error)
  ├─ 8. hooks.Discover("After*")         find AfterInsert, AfterSave, etc.
  ├─ 9. call After* hooks
  └─ 10. tracker.AcceptAll()             re-snapshot, reset to Unchanged
```


## API Reference

```go
// Create a new Unit of Work session
ctx := wh.New(provider, options...)

// Track entities
ctx.Add(entity)       // mark for INSERT
ctx.Attach(entity)    // track as Unchanged
ctx.Remove(entity)    // mark for DELETE
ctx.Detach(entity)    // stop tracking

// Inspect
entry, ok := ctx.Entry(entity)
fmt.Println(entry.State)  // Added, Modified, Unchanged, Deleted

// Flush
err := ctx.Save()              // uses stored context
err := ctx.SaveChanges(ctx)    // explicit context

// Cleanup
ctx.Close()
```
