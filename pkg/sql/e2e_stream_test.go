package sql_test

import (
	stdctx "context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type sRow struct {
	ID     int    `db:"column:id;primary_key;auto_increment"`
	Tenant string `db:"column:tenant"`
	Name   string `db:"column:name"`
}

func openStreamDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "s_row" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "tenant" TEXT NOT NULL, "name" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "s_row" ("tenant","name") VALUES ('A','a1'),('B','b1'),('A','a2'),('A','a3'),('B','b2')`); err != nil {
		t.Fatal(err)
	}
	return db
}

// Streaming yields every matching row, in order, one at a time.
func TestE2E_Stream_AllRows(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var got []string
	for v, err := range ctx.Set(&sRow{}).OrderBy("id", query.Asc).Stream(stdctx.Background()) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		got = append(got, v.(*sRow).Name)
	}
	want := []string{"a1", "b1", "a2", "a3", "b2"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("streamed names: got %v, want %v", got, want)
	}
}

// The generic Stream[T] helper yields *T directly.
func TestE2E_Stream_Typed(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	n := 0
	for u, err := range wctx.Stream[sRow](stdctx.Background(), ctx.Set(&sRow{}).OrderBy("id", query.Asc)) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		if u.ID == 0 {
			t.Errorf("expected populated entity, got zero ID")
		}
		n++
	}
	if n != 5 {
		t.Errorf("typed stream count: got %d, want 5", n)
	}
}

// Registered query filters apply to streaming exactly as they do to All.
func TestE2E_Stream_RespectsFilters(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	d := &sRow{}
	ctx.AddQueryFilter(&sRow{}, dsl.Eq(d, &d.Tenant, "A"))

	n := 0
	for v, err := range ctx.Set(&sRow{}).Stream(stdctx.Background()) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		if v.(*sRow).Tenant != "A" {
			t.Errorf("filter bypassed: streamed tenant %q, want A", v.(*sRow).Tenant)
		}
		n++
	}
	if n != 3 {
		t.Errorf("filtered stream count: got %d, want 3 (tenant A)", n)
	}
}

// Breaking out of the range loop must close the rows and release the
// connection (MaxOpenConns is 1, so a leak would show as a busy connection).
func TestE2E_Stream_EarlyBreakClosesRows(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	seen := 0
	for v, err := range ctx.Set(&sRow{}).OrderBy("id", query.Asc).Stream(stdctx.Background()) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		_ = v
		seen++
		if seen == 2 {
			break
		}
	}
	if seen != 2 {
		t.Fatalf("expected to break after 2, saw %d", seen)
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Errorf("connection leaked after early break: InUse = %d, want 0", inUse)
	}
	// The connection is reusable: a follow-up query succeeds.
	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "s_row"`).Scan(&total); err != nil {
		t.Fatalf("follow-up query after break failed (leaked conn?): %v", err)
	}
	if total != 5 {
		t.Errorf("follow-up count: got %d, want 5", total)
	}
}

// A panic in the consumer loop body must still close the rows: defer in
// ExecuteStream runs as the panic unwinds through the yield call site.
func TestE2E_Stream_PanicInLoopClosesRows(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	func() {
		defer func() { _ = recover() }()
		for v, err := range ctx.Set(&sRow{}).OrderBy("id", query.Asc).Stream(stdctx.Background()) {
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			_ = v
			panic("boom")
		}
	}()

	if inUse := db.Stats().InUse; inUse != 0 {
		t.Errorf("connection leaked after panic: InUse = %d, want 0", inUse)
	}
	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "s_row"`).Scan(&total); err != nil {
		t.Fatalf("follow-up query after panic failed (leaked conn?): %v", err)
	}
}

// Breaking out of a generic Stream[T] loop exercises the wrapper's stop path and
// must also release the connection.
func TestE2E_Stream_TypedEarlyBreak(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	seen := 0
	for u, err := range wctx.Stream[sRow](stdctx.Background(), ctx.Set(&sRow{}).OrderBy("id", query.Asc)) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		_ = u
		seen++
		break
	}
	if seen != 1 {
		t.Fatalf("expected break after 1, saw %d", seen)
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Errorf("connection leaked after typed early break: InUse = %d", inUse)
	}
}

type sBad struct {
	ID  int `db:"column:id;primary_key"`
	Num int `db:"column:num"`
}

// A scan error mid-stream surfaces through the error slot exactly once and ends
// the iteration (no double-yield, no infinite loop).
func TestE2E_Stream_MidStreamError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	// num is TEXT so a non-numeric value fails to scan into the int field.
	if _, err := db.Exec(`CREATE TABLE "s_bad" ("id" INTEGER PRIMARY KEY, "num" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "s_bad" ("id","num") VALUES (1,'10'),(2,'oops')`); err != nil {
		t.Fatal(err)
	}
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	values, errs := 0, 0
	for _, err := range ctx.Set(&sBad{}).OrderBy("id", query.Asc).Stream(stdctx.Background()) {
		if err != nil {
			errs++
			continue
		}
		values++
	}
	if values != 1 {
		t.Errorf("good rows before error: got %d, want 1", values)
	}
	if errs != 1 {
		t.Errorf("error should surface exactly once: got %d", errs)
	}
	if db.Stats().InUse != 0 {
		t.Errorf("connection leaked after stream error: InUse = %d", db.Stats().InUse)
	}
}

// Stream rejects query shapes it cannot serve row by row.
func TestE2E_Stream_RejectsUnsupportedShapes(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	cases := map[string]*wctx.EntitySet{
		"Include":   ctx.Set(&sRow{}).Include("Whatever"),
		"GroupBy":   ctx.Set(&sRow{}).GroupBy("tenant"),
		"Aggregate": ctx.Set(&sRow{}).Aggregate(query.AggCount, "*", "n"),
	}
	for name, set := range cases {
		got := 0
		var gotErr error
		for _, err := range set.Stream(stdctx.Background()) {
			got++
			gotErr = err
			break
		}
		if gotErr == nil {
			t.Errorf("%s: Stream should error, got nil", name)
		}
	}
}

// Streamed entities are not change-tracked, so a mutation is not persisted.
func TestE2E_Stream_NotTracked(t *testing.T) {
	db := openStreamDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	for v, err := range ctx.Set(&sRow{}).Stream(stdctx.Background()) {
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		v.(*sRow).Name = "mutated"
	}
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "s_row" WHERE "name"='mutated'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("streamed entities must not be tracked/persisted, found %d mutated rows", n)
	}
}

func init() {
	dsl.Register(sRow{})
	schema.Parse(&sBad{})
}
