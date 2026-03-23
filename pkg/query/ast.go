package query

// Node is a single element in the query AST.
// The tree is provider-agnostic: each Provider translates it
// into the native query language (SQL, key-value lookups, …).
type Node interface {
	nodeTag() // sealed marker
}

// Predicate is a leaf comparison node (e.g. Age > 18).
type Predicate struct {
	Field string
	Op    Op
	Value any
}

func (Predicate) nodeTag() {}

// Composite joins two or more predicates with AND/OR.
type Composite struct {
	Logic    LogicOp
	Children []Node
}

func (Composite) nodeTag() {}

// Sort represents a single ORDER BY clause.
type Sort struct {
	Field string
	Dir   SortDir
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

// Query is the root AST produced by the fluent QueryBuilder.
type Query struct {
	EntityName string
	Where      Node
	OrderBy    []Sort
	Limit      int
	Offset     int
	Includes   []string   // eager-loaded relations
	GroupBy    []string   // GROUP BY field names
	Having     Node       // HAVING condition tree
	Aggregates []Aggregate // aggregate expressions (COUNT, SUM, …)
}
