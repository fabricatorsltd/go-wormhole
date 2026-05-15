package query

// JoinType selects the SQL JOIN flavour to emit.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
)

func (j JoinType) Keyword() string {
	switch j {
	case JoinLeft:
		return "LEFT JOIN"
	case JoinRight:
		return "RIGHT JOIN"
	case JoinFull:
		return "FULL JOIN"
	default:
		return "JOIN"
	}
}

// JoinSpec is one JOIN clause attached to a Query.
// Entity is the joined table name. On is the join predicate (typically a
// Predicate produced by dsl.JoinEq, but may be a Composite for compound joins).
type JoinSpec struct {
	Type   JoinType
	Entity string
	On     Node
}

// ColumnRef references a column on a specific table. Used as a Predicate.Value
// when the right-hand side of a comparison is another column (not a literal).
// The SQL compiler emits "table"."column" instead of a parameter placeholder.
type ColumnRef struct {
	Table string // optional
	Field string
}
