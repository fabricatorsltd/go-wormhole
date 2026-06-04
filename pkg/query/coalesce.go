package query

// CoalesceArg is one operand of a COALESCE expression: a column reference when
// Column is set, otherwise a literal Value emitted as a bound parameter.
//
// Table optionally qualifies a column operand so COALESCE works in a joined
// query where the column name is ambiguous across tables. It is empty for an
// unqualified column (the back-compatible default). dsl.Col fills it in from the
// operand's entity automatically.
type CoalesceArg struct {
	Table  string
	Column string
	Value  any
}

// CoalesceExpr is a COALESCE(arg1, arg2, ...) expression that returns the first
// non-null operand. It is usable in SELECT projections, WHERE predicates, and
// ORDER BY, the same places as a CASE expression. Portable across SQL dialects.
type CoalesceExpr struct {
	Args []CoalesceArg
}

// CoalesceSelect is a COALESCE expression projected into the SELECT list under
// an alias; the result is scanned into the destination field mapped to that
// column name.
type CoalesceSelect struct {
	Expr  CoalesceExpr
	Alias string
}
