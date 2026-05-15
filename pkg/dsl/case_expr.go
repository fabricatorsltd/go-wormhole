package dsl

import "github.com/fabricatorsltd/go-wormhole/pkg/query"

// CaseBuilder fluently constructs a CASE WHEN … THEN … ELSE … END expression
// usable inside ORDER BY for sort-priority patterns.
//
// Example:
//
//	jo := &JobOffer{}
//	expr := dsl.Case().
//	    When(dsl.Gt(jo, &jo.OnTopUntil, time.Now()), 0).
//	    Else(1)
//	q := db.Set(&offers).OrderByCase(expr, query.Asc)
type CaseBuilder struct {
	expr query.CaseExpr
}

// Case starts a new fluent CASE expression.
func Case() *CaseBuilder {
	return &CaseBuilder{}
}

// When adds a WHEN <pred> THEN <value> branch. pred is a Condition produced
// by Eq/Gt/IsNotNil/etc. and can be combined via And/Or (added later).
func (b *CaseBuilder) When(pred Condition, then any) *CaseBuilder {
	b.expr.Branches = append(b.expr.Branches, query.CaseBranch{
		When: pred,
		Then: then,
	})
	return b
}

// WhenAll adds a WHEN branch whose condition is the AND of multiple predicates.
// Equivalent to wrapping the predicates in a single ANDed composite node.
func (b *CaseBuilder) WhenAll(then any, preds ...Condition) *CaseBuilder {
	children := make([]query.Node, len(preds))
	for i, p := range preds {
		children[i] = p
	}
	b.expr.Branches = append(b.expr.Branches, query.CaseBranch{
		When: query.Composite{Logic: query.LogicAnd, Children: children},
		Then: then,
	})
	return b
}

// Else sets the default value when no branch matches. Optional.
func (b *CaseBuilder) Else(value any) query.CaseExpr {
	b.expr.Else = value
	return b.expr
}

// Build returns the underlying CaseExpr without an ELSE clause.
func (b *CaseBuilder) Build() query.CaseExpr {
	return b.expr
}
