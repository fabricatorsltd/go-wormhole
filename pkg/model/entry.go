package model

// Entry wraps a tracked entity with its state and snapshot.
type Entry struct {
	Entity   any
	Meta     *EntityMeta
	State    EntityState
	Snapshot map[string]any // field values at load time
}
