package query

// CaseExpr is a CASE WHEN … THEN … [WHEN … THEN …] ELSE … END expression.
//
// Currently only usable inside ORDER BY for sort-priority cases such as
// "rows with on_top=true first, then everything else". Both Then and Else
// values are emitted as parameterized literals, so types are preserved
// across drivers.
//
// Each Branch.When may be a Predicate or a Composite (And/Or tree).
type CaseExpr struct {
	Branches []CaseBranch
	Else     any
}

// CaseBranch is one WHEN … THEN … of a CASE expression.
type CaseBranch struct {
	When Node // Predicate or Composite
	Then any  // emitted as parameter literal
}
