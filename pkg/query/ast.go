package query

// Node is a single element in the query AST.
// The tree is provider-agnostic: each Provider translates it
// into the native query language (SQL, key-value lookups, …).
type Node interface {
	nodeTag() // sealed marker
}

// Predicate is a leaf comparison node (e.g. Age > 18).
//
// Table is an optional table qualifier used when the query has joins so the
// compiler can emit "table"."column". Non-SQL backends ignore it.
type Predicate struct {
	Field string
	Op    Op
	Value any
	Table string    // optional: table qualifier for joined queries
	Case  *CaseExpr // optional: a CASE expression in place of Field on the left side
	// JSONPath, when set, makes the left side a JSON extraction of Field at this
	// dotted path (e.g. "address.city") rather than the bare column. SQL only.
	JSONPath string
}

func (Predicate) nodeTag() {}

// Composite joins two or more predicates with AND/OR.
type Composite struct {
	Logic    LogicOp
	Children []Node
}

func (Composite) nodeTag() {}

// Subquery is a predicate whose right side is a nested SELECT. For OpIn/OpNotIn
// it renders "Field IN (SELECT col FROM ... )" and Query must project exactly
// one column; for OpExists/OpNotExists it renders "[NOT] EXISTS (SELECT 1 FROM
// ... )" and Field is unused. The inner Query carries its own table and WHERE,
// which may reference the outer table (a correlated subquery) via a column-ref
// predicate.
type Subquery struct {
	Field string // outer column (OpIn/OpNotIn); unused for EXISTS
	Op    Op
	Query Query
}

func (Subquery) nodeTag() {}

// SetOpKind is a set operation that combines two SELECTs.
type SetOpKind int

const (
	SetUnion     SetOpKind = iota // UNION (distinct)
	SetUnionAll                   // UNION ALL
	SetIntersect                  // INTERSECT
	SetExcept                     // EXCEPT
)

// Keyword returns the SQL keyword for the set operation.
func (k SetOpKind) Keyword() string {
	switch k {
	case SetUnionAll:
		return "UNION ALL"
	case SetIntersect:
		return "INTERSECT"
	case SetExcept:
		return "EXCEPT"
	default:
		return "UNION"
	}
}

// SetOp combines the carrying query with a second query via a set operation.
// The operand's ORDER BY / LIMIT are ignored (a compound term cannot carry
// them); sort and limit the combined result on the outer query.
type SetOp struct {
	Kind  SetOpKind
	Query Query
}

// Sort represents a single ORDER BY clause.
//
// When Case is non-nil it takes precedence: the compiler emits the CASE
// expression in place of a column reference. Field is otherwise the column
// to sort on.
type Sort struct {
	Field string
	Dir   SortDir
	Case  *CaseExpr // optional CASE WHEN … THEN … expression
}

// AggFunc is the type of an aggregate function.
type AggFunc int

const (
	AggCount AggFunc = iota // COUNT
	AggSum                  // SUM
	AggAvg                  // AVG
	AggMin                  // MIN
	AggMax                  // MAX
)

// Aggregate represents an aggregate expression in a SELECT clause
// (e.g. COUNT(*) AS total, SUM(amount) AS revenue).
// Field may be "*" or empty for COUNT(*); otherwise it names the column.
// Alias is the AS label used to match the result column to a destination field.
type Aggregate struct {
	Func  AggFunc
	Field string // column name or "*"/"" for COUNT(*)
	Alias string // AS alias
}

// Assignment is a single column assignment in a bulk UPDATE ... SET clause
// (e.g. status = 'active'). Field is the storage column name. There is no table
// qualifier: the SET clause of an UPDATE never qualifies its target columns.
type Assignment struct {
	Field string
	Value any
}

// Query is the root AST produced by the fluent QueryBuilder.
type Query struct {
	EntityName  string
	Where       Node
	OrderBy     []Sort
	Limit       int
	Offset      int
	Includes    []string     // eager-loaded relations
	GroupBy     []string     // GROUP BY field names
	Having      Node         // HAVING condition tree
	Aggregates  []Aggregate  // aggregate expressions (COUNT, SUM, ...)
	Joins       []JoinSpec   // additional tables joined via JOIN clauses
	Distinct    bool         // emit SELECT DISTINCT
	Columns     []string     // projected columns (field or column names); empty selects all
	SetOps      []SetOp      // UNION / INTERSECT / EXCEPT operands combined with this query
	CaseSelects []CaseSelect // CASE expressions added to the SELECT list, each aliased
}

// CaseSelect is a CASE expression projected into the SELECT list under an alias.
// The result is scanned into the destination field mapped to that column name.
type CaseSelect struct {
	Expr  CaseExpr
	Alias string
}
