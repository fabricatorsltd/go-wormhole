package schema_test

import (
	"database/sql"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type ownedMoney struct {
	Amount   int    `db:"column:amount"`
	Currency string `db:"column:currency"`
}

type ownedInvoice struct {
	ID    int        `db:"column:id;primary_key"`
	Total ownedMoney `db:"owned;prefix:total_"`
}

// A prefix: override controls the flattened column names.
func TestOwned_PrefixOverride(t *testing.T) {
	meta := schema.Parse(&ownedInvoice{})
	if f := meta.FieldByColumn("total_amount"); f == nil || !f.Owned() {
		t.Errorf("expected owned column total_amount, fields: %+v", meta.Fields)
	}
	if f := meta.FieldByColumn("total_currency"); f == nil {
		t.Errorf("expected owned column total_currency")
	}
}

type scannerModel struct {
	ID   int            `db:"column:id;primary_key"`
	Note sql.NullString `db:"column:note"`
}

// A struct that speaks the database/sql scalar protocol (sql.NullString) maps to
// a single column and must not be mistaken for an owned type.
func TestOwned_ScannerTypeNotFlattened(t *testing.T) {
	meta := schema.Parse(&scannerModel{})
	f := meta.FieldByColumn("note")
	if f == nil {
		t.Fatal("sql.NullString field should map to a single column")
	}
	if f.Owned() {
		t.Error("sql.NullString must not be treated as an owned type")
	}
}

type mismappedStructModel struct {
	ID   int        `db:"column:id;primary_key"`
	Junk ownedMoney `db:"column:junk"` // mapped as a scalar column, but neither owned nor json
}

// A struct field mapped as a plain column without owned/json is a model error
// caught at parse time rather than failing opaquely at the driver later.
func TestOwned_MismappedStructPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic for a struct field mapped without owned/json")
		}
	}()
	schema.Parse(&mismappedStructModel{})
}
