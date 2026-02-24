package slipstream_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider/conformance"
	"github.com/fabricatorsltd/go-wormhole/pkg/slipstream"
)

func TestConformance_Slipstream(t *testing.T) {
	conformance.RunNoSQLProvider(t, "slipstream", func(t *testing.T) provider.Provider {
		p, err := slipstream.New(t.TempDir())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := p.Open(t.Context(), ""); err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	})
}
