package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type obdRow struct {
	ID        int       `db:"column:id;primary_key"`
	Embedding []float32 `db:"column:embedding;type:vector(3);vector"`
}

func init() { dsl.Register(obdRow{}) }

// EntitySet.OrderByDistance must reach the compiled SQL. buildQuery's sort copy
// previously handled only Field and Case sorts, silently dropping Distance (and
// Coalesce); this guards that regression on the EntitySet path.
func TestEntitySet_OrderByDistance_ReachesSQL(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := wctx.New(wsql.New(db, wsql.WithNumberedParams())) // postgres dialect

	r := &obdRow{}
	out, _, err := ctx.Set(&[]obdRow{}).
		OrderByDistance(dsl.L2Distance(r, &r.Embedding, []float32{1, 2, 3}), query.Asc).
		ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"embedding" <-> $1::vector`) {
		t.Errorf("OrderByDistance did not reach SQL: %s", out)
	}
}
