package query

// Op represents a comparison operator in the AST.
type Op int

const (
	OpEq    Op = iota // =
	OpNeq             // !=
	OpGt              // >
	OpGte             // >=
	OpLt              // <
	OpLte             // <=
	OpIn              // IN
	OpLike            // LIKE / contains
	OpIsNil           // IS NULL
)

func (o Op) String() string {
	names := [...]string{"=", "!=", ">", ">=", "<", "<=", "IN", "LIKE", "IS NULL"}
	if int(o) < len(names) {
		return names[o]
	}
	return "?"
}

// LogicOp combines predicates.
type LogicOp int

const (
	LogicAnd LogicOp = iota
	LogicOr
)

// SortDir is the direction of an ORDER BY clause.
type SortDir int

const (
	Asc SortDir = iota
	Desc
)
