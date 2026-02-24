package memdoc_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider/conformance"
)

func TestConformance_MemDoc(t *testing.T) {
	conformance.RunNoSQLProvider(t, "memdoc", func(t *testing.T) provider.Provider {
		p := memdoc.New()
		if err := p.Open(t.Context(), ""); err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	})
}
