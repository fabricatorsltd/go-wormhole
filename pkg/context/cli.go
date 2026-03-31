//go:build !wormhole_cli

package context

// runCLIIfEnabled is a no-op when the wormhole_cli build tag is not set.
func (c *DbContext) runCLIIfEnabled() {}
