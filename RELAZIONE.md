# Relazione Completa — go-wormhole v1.0

**Progetto:** `go-wormhole` — Entity Framework-inspired ORM per Go  
**Repository:** `github.com/mirkobrombin/go-wormhole`  
**Data:** 2026-02-20  
**Stato:** ✅ Completo — 71 test PASS, 0 warning  
**LOC totali:** 6.067 (4.116 sorgente + 1.951 test)  
**File Go:** 36 (26 sorgente + 5 test + 5 file tipo/interfaccia)

---

## Indice

1. [Obiettivo del Progetto](#1-obiettivo-del-progetto)
2. [Architettura Generale](#2-architettura-generale)
3. [Fase 1 — Core Architecture](#3-fase-1--core-architecture)
4. [Fase 2 — Change Tracker (Unit of Work)](#4-fase-2--change-tracker)
5. [Fase 3 — DSL Type-Safe a Puntatori](#5-fase-3--dsl-type-safe)
6. [Fase 4 — Provider Slipstream (NoSQL)](#6-fase-4--provider-slipstream)
7. [Fase 5 — Provider SQL](#7-fase-5--provider-sql)
8. [Fase 6 — Resilienza e API Pubblica](#8-fase-6--resilienza-e-api)
9. [Epic — Code-First Migrations](#9-epic--code-first-migrations)
10. [Bug Critici Risolti](#10-bug-critici-risolti)
11. [Test Suite Completa](#11-test-suite)
12. [Cronologia Git](#12-cronologia-git)
13. [Dipendenze](#13-dipendenze)

---

## 1. Obiettivo del Progetto

`go-wormhole` è un ORM/Data Mapper di nuova generazione per Go che replica l'esperienza
di Entity Framework Core in ambiente Go. Caratteristiche principali:

- **Change Tracker** in memoria con pattern Unit of Work
- **Query DSL type-safe** senza code generation (pointer tracking via `unsafe.Pointer`)
- **Provider duali:** SQL relazionale (`database/sql`) e NoSQL (`go-slipstream` / Bitcask)
- **Code-First Migrations** con differ automatico, codegen e history table
- **Resilienza integrata:** retry con backoff, circuit breaker, MultiError
- **Zero dipendenze esterne** oltre a `go-foundation` (stdlib-only primitives)

---

## 2. Architettura Generale

```
go-wormhole/
├── cmd/wormhole/          CLI: migrations add/list, database update, dbcontext scaffold
├── pkg/
│   ├── model/             EntityState, EntityMeta, FieldMeta, Entry
│   ├── schema/            Struct-tag parser → EntityMeta (cached via sync.Map)
│   ├── query/             AST nodes, operatori, fluent QueryBuilder
│   ├── dsl/               Pointer-tracking DSL (Eq, Gt, In, Contains, …)
│   ├── provider/          Provider & Tx interfaces + adapter registry
│   ├── tracker/           Identity Map + snapshot change detector
│   ├── context/           DbContext (UoW), EntitySet, DI, generics
│   ├── sql/               Provider SQL: compiler AST→SQL, dynamic scanning
│   ├── slipstream/        Provider NoSQL: go-slipstream (Bitcask engine)
│   └── migrations/        Differ, Builder, Runner, Codegen, Scaffold, Dialects
└── go.mod
```

### Integrazione con go-foundation

| Primitiva go-foundation          | Uso in go-wormhole                                    |
|----------------------------------|-------------------------------------------------------|
| `pkg/adapters.Registry[T]`       | `provider/registry.go` — registro multi-backend       |
| `pkg/di.Container`               | `context/di.go` — dependency injection al boot        |
| `pkg/tags.Parser`                | `schema/parser.go` — parsing struct tags              |
| `pkg/safemap.ShardedMap`         | `tracker/tracker.go` — identity map concorrente       |
| `pkg/hooks.Discovery + Runner`   | `context/dbcontext.go` — lifecycle hooks BeforeSave() |
| `pkg/errors.MultiError`          | `context/dbcontext.go` — errori aggregati             |
| `pkg/resiliency.Retry`           | `sql/provider.go` + `context/dbcontext.go`            |
| `pkg/resiliency.CircuitBreaker`  | `context/dbcontext.go` — fail-fast su DB offline      |

---

## 3. Fase 1 — Core Architecture

### 3.1 Modello dei Metadati (`pkg/model/`)

**`meta.go`** — `EntityMeta` descrive la mappatura completa struct → tabella:

```go
type EntityMeta struct {
    Name       string          // entity/table name
    GoType     reflect.Type
    Fields     []FieldMeta
    PrimaryKey *FieldMeta
    fieldIndex map[string]int  // lookup O(1) per nome campo
}
```

**`FieldMeta`** contiene tutti i metadati derivati dai tag:

```go
type FieldMeta struct {
    FieldName  string
    Column     string            // storage column name
    GoType     reflect.Type
    Tags       map[string]string // parsed tag pairs
    PrimaryKey bool
    AutoIncr   bool
    Nullable   bool
    Index      string
}
```

**`state.go`** — i 4 stati del change tracking:

```go
const (
    Unchanged EntityState = iota
    Added
    Modified
    Deleted
)
```

**`entry.go`** — un'entità tracciata con il suo snapshot:

```go
type Entry struct {
    Entity   any
    Meta     *EntityMeta
    State    EntityState
    Snapshot map[string]any  // valori al momento dell'attach
}
```

### 3.2 Schema Parser (`pkg/schema/parser.go`)

Usa `go-foundation/pkg/tags` con sintassi `db:"key:value; key2"`:

```go
parser = tags.NewParser(tagName,
    tags.WithPairDelimiter(";"),
    tags.WithKVSeparator(":"),
    tags.WithValueDelimiter(","),
)
```

Tag supportati:

| Tag               | Esempio                        | Effetto                        |
|-------------------|--------------------------------|--------------------------------|
| `column`          | `column:user_name`             | Override nome colonna          |
| `type`            | `type:varchar(255)`            | Tipo SQL esplicito             |
| `primary_key`     | `primary_key`                  | Marca come PK                  |
| `auto_increment`  | `auto_increment`               | Auto-increment                 |
| `nullable`        | `nullable`                     | Colonna nullable               |
| `index`           | `index:idx_email`              | Indice secondario              |
| `default`         | `default:'active'`             | Valore default SQL             |

Il parser include `toSnake()` con gestione acronimi (`UserID` → `user_id`).

I risultati sono cachati in `sync.Map` per tipo — nessun parsing ripetuto.

### 3.3 Provider Interface (`pkg/provider/`)

```go
type Provider interface {
    Name() string
    Open(ctx context.Context, dsn string) error
    Close() error
    Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error)
    Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error
    Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error
    Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error
    Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error
    Begin(ctx context.Context) (Tx, error)
}
```

L'interfaccia `Tx` replica gli stessi metodi CRUD, operando sulla transazione.

Il `registry.go` usa `go-foundation/pkg/adapters.Registry[Provider]` per registrare
e risolvere i backend per nome:

```go
provider.Register("postgres", myPgProvider)
provider.SetDefault("postgres")
p := provider.Default()
```

### 3.4 Query AST (`pkg/query/`)

**`ast.go`** — nodi dell'albero neutrale:

```go
type Predicate struct {
    Field string  // column name
    Op    Op      // OpEq, OpGt, OpLike, OpIn, OpIsNil, …
    Value any
}

type Composite struct {
    Logic    Logic   // LogicAnd, LogicOr
    Children []Node
}
```

**`builder.go`** — composizione fluente:

```go
qb := query.NewBuilder().
    Filter(dsl.Gt(&u, &u.Age, 18)).
    Order("age", query.Desc).
    Take(10)
q := qb.Build()
```

---

## 4. Fase 2 — Change Tracker

File: `pkg/tracker/tracker.go` — 179 LOC

Il tracker implementa l'**Identity Map** usando `safemap.ShardedMap[string, *Entry]`
a 32 shard per minimizzare la contesa sui lock.

### Operazioni principali

| Metodo            | Effetto                                                |
|-------------------|--------------------------------------------------------|
| `Track(e, state)` | Registra l'entità con snapshot iniziale                |
| `Attach(e)`       | Alias per `Track(e, Unchanged)`                        |
| `Add(e)`          | Alias per `Track(e, Added)`                            |
| `Remove(e)`       | Imposta stato `Deleted` (o traccia come Deleted)        |
| `DetectChanges()` | Confronta snapshot → marca Modified se diverso         |
| `Pending()`       | Ritorna tutte le entry Added/Modified/Deleted           |
| `AcceptAll()`     | Reset a Unchanged + re-snapshot dopo SaveChanges       |

### Dirty Detection via Snapshot

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

L'array `changed` viene passato direttamente al provider SQL per generare
**UPDATE parziali** — solo le colonne effettivamente modificate.

### Entity Key Strategy

```go
func (t *Tracker) entityKey(meta *model.EntityMeta, entity any) string {
    if meta.PrimaryKey != nil {
        pk := val.FieldByName(meta.PrimaryKey.FieldName).Interface()
        if meta.PrimaryKey.AutoIncr && reflect.ValueOf(pk).IsZero() {
            return fmt.Sprintf("%s#ptr(%d)", meta.Name, val.UnsafeAddr())
        }
        return fmt.Sprintf("%s#%v", meta.Name, pk)
    }
    return fmt.Sprintf("%s#ptr(%d)", meta.Name, val.UnsafeAddr())
}
```

Per entità con PK auto-increment non ancora assegnato (ID=0), usa l'indirizzo
di memoria per evitare collisioni tra entità nuove.

---

## 5. Fase 3 — DSL Type-Safe a Puntatori

File: `pkg/dsl/` — 3 file, 236 LOC totali

### Il Concetto

Invece di usare codegen (approccio iniziale scartato), il DSL sfrutta l'aritmetica
dei puntatori per risolvere i nomi dei campi a runtime senza reflect nel hot path:

```go
u := &User{}
cond := dsl.Gt(u, &u.Age, 18)
// → Predicate{Field: "age", Op: OpGt, Value: 18}
```

### Come funziona

**Boot-time (una volta):** `dsl.Register(User{})` pre-calcola gli offset:

```go
func Register(proto any) {
    t := reflect.TypeOf(proto)
    tm := &typeMeta{byOff: make(map[uintptr]*fieldInfo)}
    for i := 0; i < t.NumField(); i++ {
        sf := t.Field(i)
        offset := sf.Offset
        tm.byOff[offset] = &fieldInfo{Name: sf.Name, Column: col}
    }
    registry.Store(t, tm)
}
```

**Runtime (hot path, zero-alloc):**

```go
func resolve[B any, F any](base *B, fieldPtr *F) *fieldInfo {
    baseAddr := uintptr(unsafe.Pointer(base))
    fieldAddr := uintptr(unsafe.Pointer(fieldPtr))
    offset := fieldAddr - baseAddr  // aritmetica puntatori
    tm := lookup(reflect.TypeOf(base).Elem())
    return tm.byOff[offset]        // O(1) map lookup
}
```

### Operatori disponibili

`Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `Like`, `Contains`, `IsNil`

Tutti usano generics duali `[B any, F any]` per type safety su struct base e tipo campo.

---

## 6. Fase 4 — Provider Slipstream (NoSQL)

File: `pkg/slipstream/provider.go` — 527 LOC

Il provider mappa le operazioni dell'ORM sull'engine Bitcask `go-slipstream`.

### Architettura

```go
type Provider struct {
    engine  *engine.Engine[map[string]any]  // type-erasure pattern
    indices map[string]struct{}             // guard per indici registrati
}
```

Le entità sono serializzate come `map[string]any` (JSON). Ogni tabella è un prefisso
nel keyspace Bitcask: `{tableName}:{pkValue}`.

### Query Resolution

1. Se il predicato è su un campo indicizzato → `Engine.GetByIndex()`
2. Altrimenti → scan lineare + filtro in memoria

### Dynamic Scanning (`scanInto`)

Gestisce sia `[]T` che `[]*T` come destinazione:

```go
if isPtr {
    elem = reflect.New(elemType.Elem())  // []*T: alloca T, restituisce *T
} else {
    elem = reflect.New(elemType)         // []T: alloca T
}
```

---

## 7. Fase 5 — Provider SQL

File: `pkg/sql/` — 634 LOC (compiler.go + provider.go)

### 7.1 Compiler AST → SQL (`compiler.go`)

Il compilatore attraversa l'albero `query.Node` in modo ricorsivo e produce
SQL parametrizzato:

```go
func (c *Compiler) compileNode(b *strings.Builder, params *[]any, node query.Node) {
    switch n := node.(type) {
    case query.Predicate:
        c.compilePredicate(b, params, n)
    case query.Composite:
        c.compileComposite(b, params, n)
    }
}
```

**Supporto multi-dialetto placeholder:**

```go
func (c *Compiler) ph(n int) string {
    if c.Numbered {
        return fmt.Sprintf("$%d", n)   // Postgres
    }
    return "?"                          // MySQL/SQLite
}
```

**Update parziali reali:**

```go
func (c *Compiler) Update(meta, values, changed, pkValue) Compiled {
    changedSet := make(map[string]struct{}, len(changed))
    for _, ch := range changed { changedSet[ch] = struct{}{} }
    for _, f := range meta.Fields {
        if _, ok := changedSet[f.FieldName]; !ok { continue }
        sets = append(sets, fmt.Sprintf("%s = %s", quoteIdent(f.Column), c.ph(idx)))
    }
}
```

Se il tracker rileva che solo `Age` è cambiato → `UPDATE users SET "age" = ? WHERE "id" = ?`.

### 7.2 Dynamic Row Scanning (`provider.go`)

La funzione `scanRows` mappa le colonne SQL ai campi della struct per **nome**,
non per posizione. Le colonne extra (da JOIN) vengono scartate in un `discard`:

```go
cols, _ := rows.Columns()
for i, col := range cols {
    if fm, ok := colToField[col]; ok {
        ptrs[i] = target.FieldByName(fm.FieldName).Addr().Interface()
    } else {
        var discard any
        ptrs[i] = &discard  // colonna non mappata (JOIN)
    }
}
rows.Scan(ptrs...)
```

### 7.3 Retry a livello Provider

```go
func (p *Provider) retryDo(ctx context.Context, fn func() error) error {
    if len(p.retry) > 0 {
        return resiliency.Retry(ctx, fn, p.retry...)
    }
    return fn()
}
```

Tutte le chiamate `ExecContext` e `QueryContext` sono avvolte in `retryDo`,
configurabile con `WithRetry(resiliency.WithAttempts(3))`.

---

## 8. Fase 6 — Resilienza e API Pubblica

### 8.1 DbContext (`pkg/context/dbcontext.go`)

Il punto d'ingresso dell'utente. Integra Tracker + Provider + Hooks:

```go
ctx := wh.New(provider.Default(),
    wh.WithReadRetry(resiliency.WithAttempts(3)),
    wh.WithCircuitBreaker(5, time.Minute),
)
```

**`SaveChanges()`** — il metodo magico:

1. `tracker.DetectChanges()` — promuove Unchanged → Modified
2. Esegue lifecycle hooks (`BeforeSave()`) su tutte le entry pending
3. Raccoglie errori in `MultiError` (non si ferma al primo)
4. Apre una `Tx` sul provider
5. Per ogni entry: `Insert` / `Update` (parziale) / `Delete`
6. `Commit` (o `Rollback` + circuit breaker trip)
7. `tracker.AcceptAll()` — re-snapshot

### 8.2 EntitySet (`pkg/context/set.go`)

API fluente per operazioni CRUD:

```go
var u User
ctx.Set(&u).Find(1)         // fetch + auto-track
u.Age = 35
ctx.Save()                   // partial update

var users []User
ctx.Set(&users).
    Where(dsl.Gt(&u, &u.Age, 18)).
    OrderBy("age", query.Desc).
    Limit(10).
    All()
```

### 8.3 Generic Functions (`pkg/context/generics.go`)

```go
user, err := wh.Find[User](ctx, 42)       // type-safe, auto-tracked
users, err := wh.Query[User](ctx, q)      // returns []User
```

---

## 9. Epic — Code-First Migrations

### 9.1 Sistema di Tipi (`types.go`)

7 operazioni DDL come interfaccia polimorfica:

```go
type MigrationOp interface { Kind() OpKind }

// CreateTableOp, DropTableOp, AddColumnOp, DropColumnOp,
// AlterColumnOp, CreateIndexOp, DropIndexOp
```

`ColumnDef` modella una colonna con tutti i vincoli (PK, auto-incr, nullable, default, tipo).

`DatabaseSchema` / `TableSchema` — snapshot del DB per il diffing.

### 9.2 SchemaBuilder (`builder.go`)

Astrae il DDL dietro un'interfaccia `Dialect`:

```go
type Dialect interface {
    QuoteIdent(s string) string
    AutoIncrementClause() string
    AutoIncrementType(baseType string) string
    SupportsIfNotExists() bool
}
```

3 dialetti implementati:

| Dialetto            | Quoting      | Auto-Increment         | Note                         |
|---------------------|-------------|------------------------|------------------------------|
| `DefaultDialect`    | `"colonna"` | `AUTOINCREMENT`        | SQLite-compatibile           |
| `PostgresDialect`   | `"colonna"` | tipo → `SERIAL`/`BIGSERIAL` | Nessuna clausola separata |
| `MySQLDialect`      | `` `colonna` `` | `AUTO_INCREMENT`    | DDL causa COMMIT implicito   |

`GoTypeToSQL()` mappa automaticamente i tipi Go → SQL:

| Go           | SQL               |
|-------------|-------------------|
| `int`       | `INTEGER`         |
| `int64`     | `BIGINT`          |
| `float32`   | `REAL`            |
| `float64`   | `DOUBLE PRECISION`|
| `bool`      | `BOOLEAN`         |
| `string`    | `TEXT`            |

### 9.3 Differ (`differ.go`)

**`ComputeDiff(targets, current)`** — confronta modelli Go vs schema corrente:

1. **Tabelle nuove** → `CreateTableOp` + `CreateIndexOp`
2. **Colonne aggiunte** → `AddColumnOp`
3. **Colonne rimosse** → `DropColumnOp`
4. **Colonne alterate** (tipo/nullable/default) → `AlterColumnOp`
5. **Tabelle obsolete** → `DropTableOp`

Il confronto tipi normalizza via `GoTypeToSQL` quando `SQLType` è vuoto.

### 9.4 History Table (`history.go`)

Tabella `_wormhole_migrations_history`:

```sql
CREATE TABLE IF NOT EXISTS "_wormhole_migrations_history" (
    "migration_id" TEXT PRIMARY KEY,
    "applied_at"   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

`RecordMigration` e `RemoveMigration` operano su `*sql.Tx` — atomici con il DDL.

### 9.5 Runner (`runner.go`)

Ogni migrazione eseguita in una singola transazione:

```
BeginTx → exec DDL statements → RecordMigration → Commit
```

Se qualsiasi step fallisce → Rollback.

`Runner.Up()` ordina per ID (timestamp) e salta le migrazioni già applicate.

`Runner.Down()` rollback l'ultima applicata.

Idempotente: `Up()` chiamato due volte non fallisce (DDL usa `IF NOT EXISTS`).

### 9.6 Code Generator (`codegen.go`)

`GenerateMigrationFile(name, ops)` produce file `.go` compilabili con `Up()` e `Down()`
pre-compilati. Il `Down()` inverte automaticamente le operazioni (ove possibile).

File naming: `{yyyyMMddHHmmss}_{snake_case_name}.go`

### 9.7 Snapshot Rebuild (`snapshot.go`)

`RebuildFromMigrations([]Migration)` ricostruisce lo schema cumulativo senza
connessione al DB — ordina per ID, chiama `Up()` su un builder fittizio, applica
le ops su uno schema in memoria.

### 9.8 Scaffold (`scaffold.go`)

`Scaffold(ctx, db)` reverse-engineers un database esistente:

1. Legge `information_schema.tables` (Postgres/MySQL) o `sqlite_master` (SQLite)
2. Per ogni tabella, legge le colonne da `information_schema.columns` o `PRAGMA table_info`
3. Genera struct Go con tag `db:"..."` pronti

Mapping SQL → Go automatico (include `sql.NullXxx` per colonne nullable).

### 9.9 CLI (`cmd/wormhole/main.go`)

```
wormhole migrations add <Name>   — genera migrazione dal diff
wormhole migrations list         — stato pending/applied
wormhole database update         — applica pending
wormhole dbcontext scaffold      — reverse-engineering da DB
```

Warning giallo ANSI su operazioni distruttive:

```
WARNING: This migration drops column "users"."age" — potential data loss!
```

---

## 10. Bug Critici Risolti

### 10.1 Deadlock AcceptAll (Fase 6)

**Problema:** `ShardedMap.Range()` acquisisce `RLock`, poi `Delete()` dentro il callback
tenta `Lock` sullo stesso shard → deadlock.

**Fix:** raccogliere le chiavi da eliminare in uno slice, eliminare dopo Range:

```go
var deleteKeys []string
t.entries.Range(func(k string, e *model.Entry) bool {
    if e.State == model.Deleted {
        deleteKeys = append(deleteKeys, k)
        return true
    }
    // ...
})
for _, k := range deleteKeys {
    t.entries.Delete(k)
}
```

### 10.2 Collisione PK Auto-Increment (Fase 6)

**Problema:** multiple entità nuove con `ID=0` generavano la stessa chiave `user#0`,
sovrascrivendosi nel tracker.

**Fix:** per PK auto-increment con valore zero, usare l'indirizzo di memoria:

```go
if meta.PrimaryKey.AutoIncr && reflect.ValueOf(pk).IsZero() {
    return fmt.Sprintf("%s#ptr(%d)", meta.Name, val.UnsafeAddr())
}
```

### 10.3 Panic scanInto su []*T (Fase 4)

**Problema:** `reflect.New(elemType)` per pointer type crea un nil pointer.

**Fix:** distinguere `[]T` da `[]*T`:

```go
if isPtr {
    elem = reflect.New(elemType.Elem())   // alloca il tipo puntato
} else {
    elem = reflect.New(elemType)
}
```

### 10.4 Index Guard Slipstream (Fase 4)

**Problema:** `GetByIndex` su indice non registrato ritornava risultati non-nil.

**Fix:** mappa `indices map[string]struct{}` per validare l'esistenza dell'indice
prima della query.

### 10.5 DSL: reflect nel hot path (Fase 3)

**Problema:** `Register()` usava `ParseType()` che salta i campi senza tag.

**Fix:** iterare `NumField()` direttamente, usare `unsafe.Pointer` nel hot path
eliminando completamente reflect dalle chiamate DSL runtime.

---

## 11. Test Suite

### Distribuzione: 71 test totali

| Package          | Test | LOC  | Descrizione                                           |
|-----------------|------|------|-------------------------------------------------------|
| `pkg/context`   | 6    | 222  | EntitySet, retry, circuit breaker, Save()             |
| `pkg/dsl`       | 10   | 139  | Tutti gli operatori, campi senza tag, builder filter  |
| `pkg/sql`       | 30   | 759  | 12 compiler + 18 E2E con SQLite reale                 |
| `pkg/migrations`| 25   | 831  | Builder, differ, history, runner, codegen, dialects, scaffold, snapshot |
| **TOTALE**      | **71** | **1.951** |                                                  |

### Test E2E notevoli (`pkg/sql/e2e_test.go`)

| Test                         | Cosa verifica                                              |
|-----------------------------|-----------------------------------------------------------|
| `TestE2E_InsertAndFind`     | Insert con auto-increment + Find by PK                    |
| `TestE2E_PartialUpdate`     | Modifica un campo → UPDATE solo quella colonna             |
| `TestE2E_DSLPointerTracking`| DSL → AST → SQL → query reale su SQLite                   |
| `TestE2E_Transaction`       | BEGIN → INSERT → COMMIT + verifica dati                    |
| `TestE2E_TransactionRollback`| BEGIN → INSERT → ROLLBACK → verifica dati assenti         |
| `TestE2E_StressBatchInsert` | 100 INSERT sequenziali senza errori                        |
| `TestE2E_DbContextModifyAndSave` | Full UoW: find → modify → SaveChanges → verify       |

### Test migrazioni notevoli (`pkg/migrations/migrations_test.go`)

| Test                         | Cosa verifica                                              |
|-----------------------------|-----------------------------------------------------------|
| `TestRunnerUpAndDown`       | 2 migrazioni, INSERT reale, rollback ultima, history check |
| `TestRunnerIdempotent`      | Doppia `Up()` senza errori                                 |
| `TestE2EDifferToRunner`     | Pipeline completa: differ v1 → run → insert → differ v2 → add column → verify |
| `TestPostgresDialectAutoIncrement` | INTEGER → SERIAL, BIGINT → BIGSERIAL             |
| `TestMySQLDialectQuoting`   | Backtick quoting + AUTO_INCREMENT                          |
| `TestRebuildFromMigrations` | Ricostruzione snapshot da migrazioni fuori ordine          |
| `TestScaffoldSQLite`        | Reverse-engineering da DB con 2 tabelle reali              |

---

## 12. Cronologia Git

```
d3222f5 first commit
b883960 fix: DSL performance + untagged fields
3b1f7b2 fix: scanInto pointer slice panic + index guard in executeQuery
a1f377a feat: dynamic column scanning + SQL compiler tests
9f59192 feat: EntitySet fluent API, read retry, circuit breaker
5444d9f feat: provider-level retry, generic Find/Query, E2E stress tests
c350603 feat: code-first migrations engine (differ, builder, runner, codegen, CLI, history table)
9215fef feat: data loss warnings in CLI, migrations docs in README, MySQL DDL caveat
c6633bf feat: Postgres/MySQL dialects, DB scaffold, snapshot rebuild
```

9 commit totali, storia pulita (squashed da ~30 commit iniziali).

---

## 13. Dipendenze

```
github.com/mirkobrombin/go-foundation  v0.3.0     Primitives stdlib-only
github.com/mirkobrombin/go-slipstream  v1.0.1     Bitcask engine
github.com/mattn/go-sqlite3            v1.14.22   SQLite driver (test)
Go                                     1.24.4+
```

Tutte le altre dipendenze sono transitive via go-foundation.

---

## Stato Finale

```
$ go test ./... -count=1
71/71 PASS ✅

$ go vet ./...
0 warnings ✅

$ go build ./...
OK ✅
```

**Il progetto è completo e pushato su `main`.**
