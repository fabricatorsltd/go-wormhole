package dsl_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
)

type coDevice struct {
	ID     int    `db:"column:id;primary_key"`
	Status string `db:"column:status"`
}

type coSlot struct {
	ID     int    `db:"column:id;primary_key;table:device_slot"`
	Status string `db:"column:status"`
}

func init() {
	dsl.Register(coDevice{})
	dsl.Register(coSlot{})
}

// Col carries the operand's table so a COALESCE column is qualified, taking the
// table override into account (coSlot maps to device_slot).
func TestCol_CarriesTable(t *testing.T) {
	d, s := &coDevice{}, &coSlot{}
	expr := dsl.Coalesce(dsl.Col(d, &d.Status), dsl.Col(s, &s.Status), dsl.Lit(""))

	if len(expr.Args) != 3 {
		t.Fatalf("args: %d", len(expr.Args))
	}
	if expr.Args[0].Table != "co_device" || expr.Args[0].Column != "status" {
		t.Errorf("device arg: got %q.%q", expr.Args[0].Table, expr.Args[0].Column)
	}
	if expr.Args[1].Table != "device_slot" || expr.Args[1].Column != "status" {
		t.Errorf("slot arg: got %q.%q (table override should apply)", expr.Args[1].Table, expr.Args[1].Column)
	}
	if expr.Args[2].Table != "" || expr.Args[2].Column != "" {
		t.Errorf("literal arg should carry no table/column: %+v", expr.Args[2])
	}
}
