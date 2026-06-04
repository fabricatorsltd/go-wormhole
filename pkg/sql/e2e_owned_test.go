package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// ownedAddr is a value object with no table of its own.
type ownedAddr struct {
	Street string `db:"column:street"`
	City   string `db:"column:city"`
}

// ownedCustomer flattens its address into address_* columns on its own table.
type ownedCustomer struct {
	ID      int       `db:"column:id;primary_key"`
	Name    string    `db:"column:name"`
	Address ownedAddr `db:"owned"`
}

func init() {
	dsl.Register(ownedCustomer{})
}

func openOwnedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "owned_customer" (
		"id"             INTEGER PRIMARY KEY,
		"name"           TEXT NOT NULL,
		"address_street" TEXT NOT NULL,
		"address_city"   TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// The parser flattens the owned struct into prefixed columns with a reflect path.
func TestOwned_ParserFlattening(t *testing.T) {
	meta := schema.Parse(&ownedCustomer{})
	cols := map[string]bool{}
	for i := range meta.Fields {
		cols[meta.Fields[i].Column] = true
	}
	for _, want := range []string{"id", "name", "address_street", "address_city"} {
		if !cols[want] {
			t.Errorf("missing flattened column %q (have %v)", want, cols)
		}
	}
	street := meta.FieldByColumn("address_street")
	if street == nil || !street.Owned() || len(street.Path) != 2 {
		t.Fatalf("address_street should be an owned field with a 2-element path, got %+v", street)
	}
}

// An owned value object round-trips: it is written to and read from the
// flattened columns.
func TestE2E_Owned_RoundTrip(t *testing.T) {
	db := openOwnedDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(&ownedCustomer{ID: 1, Name: "Ada", Address: ownedAddr{Street: "1 Loop", City: "Cupertino"}})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Stored in the flattened columns.
	var street, city string
	if err := db.QueryRow(`SELECT address_street, address_city FROM owned_customer WHERE id=1`).Scan(&street, &city); err != nil {
		t.Fatal(err)
	}
	if street != "1 Loop" || city != "Cupertino" {
		t.Fatalf("flattened columns: got %q/%q", street, city)
	}

	// Read back through the ORM.
	var got ownedCustomer
	if err := ctx.Set(&got).Find(1); err != nil {
		t.Fatal(err)
	}
	if got.Address.Street != "1 Loop" || got.Address.City != "Cupertino" {
		t.Fatalf("owned round-trip failed: %+v", got.Address)
	}
}

// Mutating one nested field emits a partial UPDATE that touches only its column.
func TestE2E_Owned_PartialUpdate(t *testing.T) {
	db := openOwnedDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(&ownedCustomer{ID: 1, Name: "Ada", Address: ownedAddr{Street: "1 Loop", City: "Cupertino"}})
	if err := ctx.Save(); err != nil {
		t.Fatal(err)
	}

	var c ownedCustomer
	if err := ctx.Set(&c).Find(1); err != nil {
		t.Fatal(err)
	}
	c.Address.City = "San Jose"

	// The UPDATE must carry only the changed nested column, not the whole owned
	// type: asserting the stored street is unchanged would also pass for a full
	// rewrite, so inspect the pending SQL directly.
	pending, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Operation != "UPDATE" {
		t.Fatalf("expected one pending UPDATE, got %+v", pending)
	}
	if !strings.Contains(pending[0].SQL, "address_city") || strings.Contains(pending[0].SQL, "address_street") {
		t.Errorf("partial UPDATE should set only address_city: %s", pending[0].SQL)
	}

	if err := ctx.Save(); err != nil {
		t.Fatalf("save update: %v", err)
	}

	var street, city string
	if err := db.QueryRow(`SELECT address_street, address_city FROM owned_customer WHERE id=1`).Scan(&street, &city); err != nil {
		t.Fatal(err)
	}
	if city != "San Jose" {
		t.Errorf("nested field not updated: city=%q", city)
	}
	if street != "1 Loop" {
		t.Errorf("untouched nested field changed: street=%q", street)
	}
}

// A DSL predicate on a nested field resolves to the flattened column.
func TestE2E_Owned_PredicateOnNestedField(t *testing.T) {
	db := openOwnedDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(
		&ownedCustomer{ID: 1, Name: "Ada", Address: ownedAddr{Street: "1 Loop", City: "Cupertino"}},
		&ownedCustomer{ID: 2, Name: "Ben", Address: ownedAddr{Street: "5 Market", City: "San Jose"}},
	)
	if err := ctx.Save(); err != nil {
		t.Fatal(err)
	}

	c := &ownedCustomer{}
	var found []ownedCustomer
	if err := ctx.Set(&found).Where(dsl.Eq(c, &c.Address.City, "San Jose")).All(); err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Name != "Ben" {
		t.Fatalf("predicate on nested field: got %+v", found)
	}
}
