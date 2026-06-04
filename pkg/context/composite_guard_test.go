package context_test

import (
	"strings"
	"testing"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
)

type guardLine struct {
	A int `db:"column:a;primary_key"`
	B int `db:"column:b;primary_key"`
	V int `db:"column:v"`
}

// A provider that does not implement provider.CompositeKeyer (the mock) must
// reject a composite-PK entity rather than silently keying on the first column.
func TestCompositeKey_RejectedOnUnsupportedProvider(t *testing.T) {
	ctx := wctx.New(&mockProvider{})

	var l guardLine
	err := ctx.Set(&l).Find(1, 2)
	if err == nil || !strings.Contains(err.Error(), "composite primary key") {
		t.Fatalf("Find: want composite-key rejection, got %v", err)
	}

	ctx.Add(&guardLine{A: 1, B: 2, V: 3})
	if err := ctx.Save(); err == nil || !strings.Contains(err.Error(), "composite primary key") {
		t.Fatalf("Save: want composite-key rejection, got %v", err)
	}
}
