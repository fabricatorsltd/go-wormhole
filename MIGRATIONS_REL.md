# Relazione Tecnica: Sistema di Migrazioni Code-First

**Progetto:** go-wormhole  
**Epic:** Code-First Migrations (EF Core Parity)  
**Data:** 2026-02-20  
**Commit:** `c350603` — `feat: code-first migrations engine`  
**File coinvolti:** 8 (7 sorgenti + 1 test suite)  
**LOC aggiunte:** 1.658  
**Test:** 15/15 PASS (di cui 1 E2E full-stack con SQLite reale)

---

## 1. Panoramica Architetturale

Il sistema di migrazioni implementa il pattern **Code-First** à la Entity Framework Core.
Le struct Go sono la Single Source of Truth: il framework rileva le differenze tra il modello
nel codice e lo stato corrente del database, genera automaticamente file `.go` con i metodi
`Up()` / `Down()`, e li esegue in transazione.

Il flusso completo è:

```
  Struct Go (model.EntityMeta)
         │
         ▼
  ComputeDiff(targets, snapshot)     ← rileva le differenze
         │
         ▼ []MigrationOp
  GenerateMigrationFile(name, ops)   ← produce codice Go
         │
         ▼ file .go con Up()/Down()
  Runner.Up(ctx)                     ← esegue in transazione
         │
         ▼ DDL + INSERT in history
  _wormhole_migrations_history       ← tracking persistente
```

---

## 2. Dettaglio dei Componenti

### 2.1 Sistema di Tipi — `types.go`

Definisce il vocabolario dell'intero sottosistema.

**`ColumnDef`** modella una singola colonna con tutti i metadati necessari al DDL rendering:

```go
type ColumnDef struct {
    Name       string       // storage column name (snake_case)
    SQLType    string       // explicit SQL type, e.g. "varchar(255)"
    PrimaryKey bool
    AutoIncr   bool
    Nullable   bool
    Default    string       // literal default expression, e.g. "'active'"
    Index      string       // secondary index name (empty = none)
    GoType     reflect.Type
}
```

Il campo `GoType` è il bridge tra Go e SQL: quando `SQLType` è vuoto (l'utente non ha
specificato un tipo esplicito nel tag `db:"type:..."`), il builder lo risolve automaticamente
tramite `GoTypeToSQL()`.

**Le operazioni DDL** sono modellate come interfaccia polimorfica:

```go
type MigrationOp interface {
    Kind() OpKind
}
```

Con 7 implementazioni concrete: `CreateTableOp`, `DropTableOp`, `AddColumnOp`,
`DropColumnOp`, `AlterColumnOp`, `CreateIndexOp`, `DropIndexOp`.

**Snapshot del database** per il diffing:

```go
type DatabaseSchema struct {
    Tables map[string]*TableSchema
}

type TableSchema struct {
    Name    string
    Columns map[string]*ColumnDef
}
```

Le mappe `map[string]*ColumnDef` permettono lookup O(1) durante il confronto colonna-per-colonna.

---

### 2.2 SchemaBuilder — `builder.go`

Astrae la generazione DDL dietro un'interfaccia fluente, disaccoppiata dal dialetto SQL specifico.

**Architettura a Dialect:**

```go
type Dialect interface {
    QuoteIdent(s string) string
    AutoIncrementClause() string
    SupportsIfNotExists() bool
}
```

Il `DefaultDialect` produce SQL standard con double-quote (`"users"`) e `AUTOINCREMENT`,
compatibile con SQLite e Postgres. Per MySQL basta implementare un dialect che usa backtick
e `AUTO_INCREMENT`.

**API fluente** — il builder accumula operazioni senza eseguire nulla:

```go
b := migrations.NewBuilder()
b.CreateTable("users",
    migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
    migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
)
b.AddColumn("users", migrations.ColumnDef{Name: "email", SQLType: "VARCHAR(255)"})
b.CreateIndex("idx_email", "users", true, "email")
```

**Rendering a due livelli:**

- `b.SQL()` — restituisce tutto concatenato con `;`, utile per dump/debug.
- `b.Statements()` — restituisce uno slice di stringhe individuali, usato dal `Runner`
  per eseguire un statement alla volta dentro la transazione.

**Risoluzione automatica dei tipi Go → SQL:**

```go
func GoTypeToSQL(t reflect.Type) string {
    switch t.Kind() {
    case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
        return "INTEGER"
    case reflect.Int64:
        return "BIGINT"
    case reflect.Float32:
        return "REAL"
    case reflect.Float64:
        return "DOUBLE PRECISION"
    case reflect.Bool:
        return "BOOLEAN"
    case reflect.String:
        return "TEXT"
    default:
        return "TEXT"
    }
}
```

Se l'utente ha specificato un tipo esplicito nel tag (`db:"type:varchar(255)"`), viene usato
quello. Altrimenti il tipo Go viene mappato automaticamente.

**Rendering delle colonne** — gestisce la composizione dei vincoli:

```go
func (b *SchemaBuilder) renderColumnDef(c ColumnDef, q func(string) string) string {
    var parts []string
    parts = append(parts, q(c.Name))
    parts = append(parts, b.resolveType(c))
    if c.PrimaryKey {
        parts = append(parts, "PRIMARY KEY")
    }
    if c.AutoIncr {
        parts = append(parts, b.dialect.AutoIncrementClause())
    }
    if !c.Nullable && !c.PrimaryKey {
        parts = append(parts, "NOT NULL")
    }
    if c.Default != "" {
        parts = append(parts, "DEFAULT "+c.Default)
    }
    return strings.Join(parts, " ")
}
```

Nota: `NOT NULL` viene omesso per le primary key (implicito nello standard SQL) e per
le colonne marcate `Nullable`.

---

### 2.3 Differ — `differ.go`

Il cervello del sistema. Confronta i modelli Go (`[]*model.EntityMeta`) contro uno snapshot
del database (`DatabaseSchema`) e produce la lista di operazioni DDL necessarie.

**Algoritmo `ComputeDiff`:**

```go
func ComputeDiff(targets []*model.EntityMeta, current DatabaseSchema) []MigrationOp {
    var ops []MigrationOp
    targetNames := make(map[string]bool, len(targets))

    for _, meta := range targets {
        targetNames[meta.Name] = true
        existing, exists := current.Tables[meta.Name]

        if !exists {
            ops = append(ops, createTableFromMeta(meta))
            ops = append(ops, indexOpsForMeta(meta)...)
            continue
        }

        ops = append(ops, diffColumns(meta, existing)...)
        ops = append(ops, indexOpsForMeta(meta)...)
    }

    // Detect dropped tables (in DB but not in code)
    for name := range current.Tables {
        if !targetNames[name] {
            ops = append(ops, DropTableOp{Table: name})
        }
    }
    return ops
}
```

L'algoritmo opera in 3 fasi:

1. **Tabelle nuove** — se un `EntityMeta` non ha corrispondente in `current.Tables`,
   emette `CreateTableOp` con tutte le colonne + eventuali `CreateIndexOp`.

2. **Diff colonne** — per tabelle esistenti, confronta colonna per colonna:
   - Colonna presente nel codice ma assente nel DB → `AddColumnOp`
   - Colonna assente dal codice ma presente nel DB → `DropColumnOp`
   - Colonna presente in entrambi ma con tipo/vincoli diversi → `AlterColumnOp`

3. **Tabelle obsolete** — tabelle presenti nel DB ma senza corrispondente nel codice →
   `DropTableOp`.

**Rilevamento cambiamenti di tipo:**

```go
func columnChanged(old *ColumnDef, new *ColumnDef) bool {
    oldType := old.SQLType
    newType := new.SQLType
    if oldType == "" && old.GoType != nil {
        oldType = GoTypeToSQL(old.GoType)
    }
    if newType == "" && new.GoType != nil {
        newType = GoTypeToSQL(new.GoType)
    }
    if oldType != newType { return true }
    if old.Nullable != new.Nullable { return true }
    if old.Default != new.Default { return true }
    return false
}
```

Il confronto normalizza i tipi: se un campo non ha `SQLType` esplicito, lo risolve
tramite `GoTypeToSQL` prima del confronto. Questo evita falsi positivi quando un campo
cambia da `db:"type:INTEGER"` alla rimozione del tag (il `reflect.Type` `int` produce
comunque `"INTEGER"`).

**Bridge model → migrations:**

```go
func fieldToColumnDef(f model.FieldMeta) ColumnDef {
    sqlType := ""
    if v, ok := f.Tags["type"]; ok { sqlType = v }
    def := ""
    if v, ok := f.Tags["default"]; ok { def = v }
    return ColumnDef{
        Name: f.Column, SQLType: sqlType, PrimaryKey: f.PrimaryKey,
        AutoIncr: f.AutoIncr, Nullable: f.Nullable, Default: def,
        Index: f.Index, GoType: f.GoType,
    }
}
```

Legge `type` e `default` dalla mappa `Tags` del `FieldMeta`, che a sua volta è popolata
dal parser di struct tags (`pkg/schema/parser.go`) al boot dell'applicazione.

**Helper `MetaToSnapshot`** — converte un array di modelli in un `DatabaseSchema`,
usato per generare lo snapshot "corrente" da confrontare con la versione successiva:

```go
func MetaToSnapshot(metas []*model.EntityMeta) DatabaseSchema {
    schema := DatabaseSchema{Tables: make(map[string]*TableSchema)}
    for _, meta := range metas {
        ts := &TableSchema{Name: meta.Name, Columns: make(map[string]*ColumnDef)}
        for _, f := range meta.Fields {
            cd := fieldToColumnDef(f)
            ts.Columns[f.Column] = &cd
        }
        schema.Tables[meta.Name] = ts
    }
    return schema
}
```

---

### 2.4 History Table — `history.go`

Gestisce la tabella di sistema `_wormhole_migrations_history` che traccia quali migrazioni
sono state applicate. Equivalente di `__EFMigrationsHistory` in EF Core.

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS "_wormhole_migrations_history" (
    "migration_id" TEXT PRIMARY KEY,
    "applied_at"   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

**Funzioni esposte:**

- `EnsureHistoryTable(ctx, db)` — idempotente, crea la tabella se non esiste.
- `AppliedMigrations(ctx, db)` — restituisce `map[string]bool` delle migrazioni applicate.
- `RecordMigration(ctx, tx, id)` — inserisce un record **dentro una transazione**.
- `RemoveMigration(ctx, tx, id)` — rimuove un record (per rollback) **dentro una transazione**.

Punto chiave: `RecordMigration` e `RemoveMigration` operano su `*sql.Tx`, non su `*sql.DB`.
Questo garantisce che il DDL della migrazione e il suo record nella history table siano
atomici — se il DDL fallisce, il record non viene inserito.

---

### 2.5 Runner — `runner.go`

Il motore di esecuzione. Applica le migrazioni pending in ordine cronologico, ciascuna
nella propria transazione.

**Struttura:**

```go
type Migration struct {
    ID   string
    Up   func(b *SchemaBuilder)
    Down func(b *SchemaBuilder)
}

type Runner struct {
    db         *sql.DB
    migrations []Migration
    dialect    Dialect
}
```

Le migrazioni sono funzioni che ricevono un `*SchemaBuilder` — non SQL diretto.
Questo le rende dialect-agnostic: lo stesso file di migrazione funziona su SQLite,
Postgres e MySQL cambiando solo il `Dialect` passato al `Runner`.

**Applicazione di una singola migrazione (`applyUp`):**

```go
func (r *Runner) applyUp(ctx context.Context, m Migration) error {
    b := NewBuilderWith(r.dialect)
    m.Up(b)

    tx, err := r.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("begin: %w", err)
    }

    for _, stmt := range b.Statements() {
        if _, err := tx.ExecContext(ctx, stmt); err != nil {
            _ = tx.Rollback()
            return fmt.Errorf("exec %q: %w", stmt, err)
        }
    }

    if err := RecordMigration(ctx, tx, m.ID); err != nil {
        _ = tx.Rollback()
        return fmt.Errorf("record: %w", err)
    }

    return tx.Commit()
}
```

Il flusso è:

1. Crea un builder con il dialetto corrente.
2. Chiama `m.Up(b)` — la migrazione popola il builder con le operazioni DDL.
3. Apre una transazione.
4. Esegue ogni statement DDL individualmente dentro la TX.
5. Registra la migrazione nella history table (stessa TX).
6. Commit. Se qualsiasi step fallisce → Rollback.

**`Runner.Up(ctx)`** — applica tutte le migrazioni pending in ordine:

```go
func (r *Runner) Up(ctx context.Context) error {
    applied, _ := AppliedMigrations(ctx, r.db)

    sort.Slice(r.migrations, func(i, j int) bool {
        return r.migrations[i].ID < r.migrations[j].ID
    })

    for _, m := range r.migrations {
        if applied[m.ID] { continue }
        if err := r.applyUp(ctx, m); err != nil {
            return fmt.Errorf("migration %s: %w", m.ID, err)
        }
    }
    return nil
}
```

Le migrazioni vengono ordinate per ID (che contiene il timestamp) prima dell'esecuzione,
garantendo l'ordine cronologico indipendentemente dall'ordine di registrazione.

**`Runner.Down(ctx)`** — rollback dell'ultima migrazione applicata. Trova l'ultima
migrazione con ID presente nella history table, esegue il suo `Down()`, e rimuove
il record dalla history.

**Idempotenza:** chiamare `Up()` due volte è sicuro — le migrazioni già presenti nella
history table vengono saltate. I DDL usano `IF NOT EXISTS` / `IF EXISTS` per sicurezza
aggiuntiva.

---

### 2.6 Code Generator — `codegen.go`

Genera file `.go` contenenti migrazioni pre-compilate dal differ. Il codice generato
è Go valido e compilabile.

**`GenerateMigrationFile(name, ops)`** produce:

```go
package migrations

import "github.com/fabricatorsltd/go-wormhole/pkg/migrations"

func init() {
    Register(migrations.Migration{
        ID: "20260220175600_add_orders",
        Up: func(b *migrations.SchemaBuilder) {
            b.CreateTable("orders",
                migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
                migrations.ColumnDef{Name: "total", SQLType: "REAL"},
            )
        },
        Down: func(b *migrations.SchemaBuilder) {
            b.DropTable("orders")
        },
    })
}
```

Il metodo `Down()` viene generato automaticamente invertendo le operazioni:

| Up                | Down generato                                |
|-------------------|----------------------------------------------|
| `CreateTable`     | `DropTable`                                  |
| `AddColumn`       | `DropColumn`                                 |
| `CreateIndex`     | `DropIndex`                                  |
| `DropTable`       | `// TODO: recreate table` (non reversibile)  |
| `DropColumn`      | `// TODO: re-add column` (non reversibile)   |
| `AlterColumn`     | `// TODO: revert column` (non reversibile)   |

Le operazioni distruttive generano un commento `TODO` nel Down — il developer deve
completarle manualmente se vuole supportare il rollback.

**Naming convention:** il file viene nominato `{timestamp}_{snake_case_name}.go`,
es. `20260220175600_add_orders.go`. Il migration ID segue lo stesso pattern.

---

### 2.7 CLI — `cmd/wormhole/main.go`

Entry point per la riga di comando. Comandi implementati:

```
wormhole migrations add <Name>   — genera un nuovo file di migrazione
wormhole migrations list         — elenca migrazioni (pending/applied)
wormhole database update         — applica migrazioni pending
```

**Variabili d'ambiente:**

| Variabile        | Default      | Descrizione                      |
|------------------|-------------|----------------------------------|
| `WORMHOLE_DSN`   | (required)  | Connection string del database   |
| `WORMHOLE_DRIVER`| `sqlite`    | Driver `database/sql`            |
| `WORMHOLE_DIR`   | `./migrations` | Directory dei file `.go`      |

Il comando `add` esegue `ComputeDiff` → `GenerateMigrationFile` → scrive il file su disco.
Il comando `list` legge la directory e la history table, mostrando lo stato di ogni migrazione.

---

## 3. Test Suite

15 test distribuiti su 6 categorie:

### Builder (3 test)

| Test                       | Cosa verifica                                            |
|---------------------------|----------------------------------------------------------|
| `TestBuilderCreateTable`  | DDL CREATE TABLE con PK, AUTOINCREMENT, quoting          |
| `TestBuilderAddDropColumn`| ALTER TABLE ADD/DROP COLUMN                              |
| `TestBuilderCreateIndex`  | CREATE UNIQUE INDEX con IF NOT EXISTS                    |

### Differ (5 test)

| Test                  | Cosa verifica                                                 |
|----------------------|---------------------------------------------------------------|
| `TestDifferNewTable` | Tabella assente nel DB → `CreateTableOp`                      |
| `TestDifferAddColumn`| Campo nuovo nella struct → `AddColumnOp`                      |
| `TestDifferDropColumn`| Campo rimosso dalla struct → `DropColumnOp`                  |
| `TestDifferDropTable`| Tabella nel DB ma non nel codice → `DropTableOp`             |
| `TestMetaToSnapshot` | Conversione `EntityMeta` → `DatabaseSchema` per snapshotting |

### History (2 test)

| Test                         | Cosa verifica                                        |
|-----------------------------|------------------------------------------------------|
| `TestHistoryEnsureAndRecord`| Creazione idempotente della tabella + INSERT/SELECT  |
| `TestHistoryRemove`         | DELETE del record per rollback                       |

### Runner (2 test)

| Test                   | Cosa verifica                                              |
|-----------------------|------------------------------------------------------------|
| `TestRunnerUpAndDown` | Applica 2 migrazioni, verifica dati reali, rollback ultima |
| `TestRunnerIdempotent`| Doppia chiamata a `Up()` non causa errori                  |

### Codegen (1 test)

| Test                        | Cosa verifica                                         |
|----------------------------|-------------------------------------------------------|
| `TestGenerateMigrationFile`| Codice generato contiene Up/Down corretti, snake_case |

### GoTypeToSQL (1 test)

| Test             | Cosa verifica                                          |
|-----------------|--------------------------------------------------------|
| `TestGoTypeToSQL`| Mapping `int`→INTEGER, `int64`→BIGINT, `string`→TEXT  |

### E2E Full-Stack (1 test)

| Test                    | Cosa verifica                                              |
|------------------------|------------------------------------------------------------|
| `TestE2EDifferToRunner`| Pipeline completa: differ → builder → runner → SQLite reale. Crea tabella, inserisce dati, aggiunge colonna via diff v2, verifica UPDATE sulla nuova colonna. |

Tutti i test usano SQLite `:memory:` con `SetMaxOpenConns(1)` per evitare il problema
delle connessioni multiple su database in-memory.

---

## 4. Integrazione con l'Ecosistema go-wormhole

Il sistema di migrazioni si integra con i componenti esistenti:

- **`model.EntityMeta`** — il differ legge i metadati delle struct prodotti dal parser
  (`pkg/schema/parser.go`) che a sua volta usa `go-foundation/pkg/tags`.

- **`model.FieldMeta.Tags`** — i tag `type`, `default`, `primary_key`, `auto_increment`,
  `nullable`, `index` vengono tutti consumati da `fieldToColumnDef()`.

- **SQL Provider** — il `Runner` usa `database/sql` direttamente (`*sql.DB` + `*sql.Tx`),
  lo stesso layer usato dal Provider SQL dell'ORM.

- **Dialect** — il sistema è pronto per il multi-dialetto. Implementando l'interfaccia
  `Dialect` (3 metodi) si supporta qualsiasi RDBMS.

---

## 5. Stato Finale della Suite

```
$ go test ./... -count=1

pkg/context       6 PASS   ✅
pkg/dsl          10 PASS   ✅
pkg/migrations   15 PASS   ✅
pkg/sql          30 PASS   ✅
─────────────────────────
TOTALE           61 PASS
```

```
$ go vet ./...
(nessun warning)
```

---

## 6. Lavoro Futuro

- **Scaffold da DB** (`wormhole dbcontext scaffold <DSN>`) — reverse engineering
  da `information_schema` per generare struct Go con tag `db:"..."` da database esistenti.

- **Dialect Postgres** — implementazione concreta che usa `$1, $2` per i placeholder
  e `SERIAL` / `BIGSERIAL` per auto-increment.

- **Ricostruzione snapshot cumulativo** — attualmente lo snapshot "corrente" è vuoto
  (greenfield). In futuro il CLI dovrà parsare i file di migrazione esistenti per
  ricostruire lo stato cumulativo del DB senza doversi connettere.
