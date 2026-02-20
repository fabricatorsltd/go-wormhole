package model

// EntityState represents the tracking state of an entity within the DbContext.
type EntityState int

const (
	Detached  EntityState = iota // not tracked
	Unchanged                    // loaded, no changes
	Added                        // new, pending insert
	Modified                     // changed since snapshot
	Deleted                      // marked for deletion
)

func (s EntityState) String() string {
	switch s {
	case Detached:
		return "Detached"
	case Unchanged:
		return "Unchanged"
	case Added:
		return "Added"
	case Modified:
		return "Modified"
	case Deleted:
		return "Deleted"
	default:
		return "Unknown"
	}
}
