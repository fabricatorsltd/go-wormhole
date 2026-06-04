package sql_test

import (
	"database/sql"
	stderrors "errors"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type qfDoc struct {
	ID       int          `db:"column:id;primary_key;auto_increment"`
	TenantID string       `db:"column:tenant_id"`
	Title    string       `db:"column:title"`
	Comments []*qfComment `db:"fk:doc_id"`
}

type qfComment struct {
	ID       int    `db:"column:id;primary_key;auto_increment"`
	DocID    int    `db:"column:doc_id"`
	TenantID string `db:"column:tenant_id"`
	Body     string `db:"column:body"`
}

func init() {
	dsl.Register(qfDoc{})
	dsl.Register(qfComment{})
}

func openQFDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "qf_doc" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "tenant_id" TEXT NOT NULL, "title" TEXT NOT NULL)`,
		`CREATE TABLE "qf_comment" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "doc_id" INTEGER NOT NULL, "tenant_id" TEXT NOT NULL, "body" TEXT NOT NULL)`,
		`INSERT INTO "qf_doc" ("id","tenant_id","title") VALUES (1,'A','docA1'),(2,'A','docA2'),(3,'B','docB1')`,
		// comment 2 belongs to doc 1 but tenant B: the cross-tenant leak Include must not surface.
		`INSERT INTO "qf_comment" ("id","doc_id","tenant_id","body") VALUES (1,1,'A','a-comment'),(2,1,'B','b-leak'),(3,3,'B','b-comment')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// tenantACtx returns a context scoped to tenant A via query filters.
func tenantACtx(db *sql.DB) *wctx.DbContext {
	ctx := wctx.New(wsql.New(db))
	d := &qfDoc{}
	c := &qfComment{}
	ctx.AddQueryFilter(&qfDoc{}, dsl.Eq(d, &d.TenantID, "A"))
	ctx.AddQueryFilter(&qfComment{}, dsl.Eq(c, &c.TenantID, "A"))
	return ctx
}

func TestE2E_QueryFilter_AllScoped(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	var docs []*qfDoc
	if err := ctx.Set(&docs).All(); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d docs, want 2 (tenant A only)", len(docs))
	}
	for _, d := range docs {
		if d.TenantID != "A" {
			t.Errorf("leaked tenant %s", d.TenantID)
		}
	}
}

func TestE2E_QueryFilter_FindBlocksCrossTenant(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	// Own tenant: found.
	var ok qfDoc
	if err := ctx.Set(&ok).Find(1); err != nil {
		t.Fatalf("find own doc: %v", err)
	}
	if ok.Title != "docA1" {
		t.Errorf("got %q, want docA1", ok.Title)
	}

	// Cross tenant: must be not-found, not a leak.
	var leak qfDoc
	err := ctx.Set(&leak).Find(3)
	if !stderrors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant Find: want sql.ErrNoRows, got %v", err)
	}
}

func TestE2E_QueryFilter_IncludeChildrenScoped(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	d := &qfDoc{}
	var docs []*qfDoc
	if err := ctx.Set(&docs).Where(dsl.Eq(d, &d.ID, 1)).Include("Comments").All(); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("got %d docs, want 1", len(docs))
	}
	// Doc 1 has two comments in the DB, one of them tenant B. The child filter
	// must exclude the tenant-B comment.
	cs := docs[0].Comments
	if len(cs) != 1 || cs[0].Body != "a-comment" {
		t.Fatalf("Include children not tenant-scoped: got %d comments %+v", len(cs), cs)
	}
}

func TestE2E_QueryFilter_IgnoreFilters(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	var docs []*qfDoc
	if err := ctx.Set(&docs).IgnoreFilters().All(); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 3 {
		t.Fatalf("IgnoreFilters: got %d docs, want 3 (all tenants)", len(docs))
	}
}

func TestE2E_QueryFilter_BulkUpdateAndDeleteScoped(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	d := &qfDoc{}
	n, err := ctx.Set(&qfDoc{}).Update(dsl.Set(d, &d.Title, "X"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("bulk update affected %d, want 2 (tenant A only)", n)
	}
	// Tenant B doc must be untouched.
	var bTitle string
	if err := db.QueryRow(`SELECT "title" FROM "qf_doc" WHERE "id" = 3`).Scan(&bTitle); err != nil {
		t.Fatal(err)
	}
	if bTitle != "docB1" {
		t.Errorf("tenant B doc was modified: %q", bTitle)
	}

	del, err := ctx.Set(&qfDoc{}).Delete()
	if err != nil {
		t.Fatal(err)
	}
	if del != 2 {
		t.Fatalf("bulk delete removed %d, want 2 (tenant A only)", del)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "qf_doc"`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Errorf("remaining docs: got %d, want 1 (tenant B)", remaining)
	}
}

type qfCount struct {
	N int `db:"column:n"`
}

// A From() override onto a registered, filtered table must still be scoped:
// the filter follows the queried table, not the DTO type.
func TestE2E_QueryFilter_FromOverrideScoped(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := tenantACtx(db)
	defer ctx.Close()

	var rows []qfCount
	if err := ctx.Set(&rows).From("qf_doc").Aggregate(query.AggCount, "*", "n").All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].N != 2 {
		t.Fatalf("From-override aggregate not tenant-scoped: got %+v, want count 2", rows)
	}
}

func TestE2E_QueryFilter_MultipleFiltersAnd(t *testing.T) {
	db := openQFDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	d := &qfDoc{}
	ctx.AddQueryFilter(&qfDoc{}, dsl.Eq(d, &d.TenantID, "A"))
	ctx.AddQueryFilter(&qfDoc{}, dsl.Neq(d, &d.Title, "docA2"))

	var docs []*qfDoc
	if err := ctx.Set(&docs).All(); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].Title != "docA1" {
		t.Fatalf("both filters must AND: got %d docs %+v", len(docs), docs)
	}
}
