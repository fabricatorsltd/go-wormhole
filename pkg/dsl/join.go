package dsl

import "github.com/fabricatorsltd/go-wormhole/pkg/query"

// JoinEq builds a join condition predicate "tableA.colA = tableB.colB".
//
// Both sides are pointer-tracked, so the column names AND parent tables are
// resolved at compile time:
//
//	jo := &JobOffer{}; pos := &POS{}
//	b.Join("pos", dsl.JoinEq(jo, &jo.OwnerId, pos, &pos.Id))
//
// The returned Condition stores the left-side column (Field/Table) and the
// right-side column reference inside Value as a query.ColumnRef so the SQL
// compiler can render it without a parameter placeholder.
func JoinEq[A any, B any, FA any, FB any](baseA *A, fieldA *FA, baseB *B, fieldB *FB) Condition {
	fa, ta := resolveWithTypeMap(baseA, fieldA)
	fb, tb := resolveWithTypeMap(baseB, fieldB)
	return Condition{
		Field: fa.Column,
		Table: ta.table,
		Op:    query.OpEq,
		Value: query.ColumnRef{Field: fb.Column, Table: tb.table},
	}
}
