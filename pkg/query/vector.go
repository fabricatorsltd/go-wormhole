package query

// VectorOp is a pgvector distance operator used in a nearest-neighbor ORDER BY.
type VectorOp int

const (
	// VectorL2 is Euclidean (L2) distance, the `<->` operator.
	VectorL2 VectorOp = iota
	// VectorCosine is cosine distance, the `<=>` operator.
	VectorCosine
	// VectorInner is negative inner product, the `<#>` operator.
	VectorInner
)

// VectorDistance is a distance expression between a stored vector column and a
// query vector, usable as an ORDER BY term for nearest-neighbor search. It is
// PostgreSQL + pgvector only; other backends reject it.
type VectorDistance struct {
	Field  string // entity field or column holding the stored vector
	Op     VectorOp
	Vector []float32 // the query vector
}
