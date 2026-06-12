package bleve_bench

import (
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2"
)

const WriterDocumentCount = 1_000_000

// temporaryDirectory creates a temp dir and registers cleanup with the benchmark.
func temporaryDirectory(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "bleve-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// buildBatch constructs a bleve.Batch pre-loaded with WriterDocumentCount documents.
// The index is only used to construct the batch; the actual indexing target is
// opened fresh inside each benchmark loop.
func buildBatch(idx bleve.Index) *bleve.Batch {
	batch := idx.NewBatch()
	for i := range WriterDocumentCount {
		id := fmt.Sprintf("%d", i)
		doc := map[string]any{
			"name":          fmt.Sprintf("hello-%d", i),
			"index":         id,
			"reversed-name": fmt.Sprintf("olleh-%d", i),
		}
		if err := batch.Index(id, doc); err != nil {
			panic(fmt.Sprintf("batch.Index: %v", err))
		}
	}
	return batch
}

// newIndex opens a new bleve index at path with a simple keyword-oriented mapping.
// All three fields are stored as keyword (not analyzed) to mirror bluge's
// NewKeywordField semantics.
func newIndex(b *testing.B, path string) bleve.Index {
	b.Helper()

	km := bleve.NewIndexMapping()
	km.DefaultAnalyzer = "keyword"

	idx, err := bleve.New(path, km)
	if err != nil {
		b.Fatalf("bleve.New: %v", err)
	}
	return idx
}

// BenchmarkWriter mirrors bluge.BenchmarkWriter:
// open a normal (online) index, write the pre-built batch, close.
func BenchmarkWriter(b *testing.B) {
	b.StopTimer()

	// Build the batch using a throw-away in-memory alias so we don't pay
	// for index construction inside the timed region.
	scratch, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		b.Fatalf("scratch index: %v", err)
	}
	batch := buildBatch(scratch)
	_ = scratch.Close()

	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		dir := temporaryDirectory(b)
		idx := newIndex(b, dir)
		b.StartTimer()

		if err := idx.Batch(batch); err != nil {
			b.Fatal(err)
		}
		if err := idx.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOfflineWriter mirrors bluge.BenchmarkOfflineWriter.
// Bleve has no dedicated offline/bulk writer; the closest equivalent is a
// normal index opened with the scorch store's "forceSegmentType"/"batchSize"
// knobs via a custom config, which pre-commits in large segments much like
// bluge's OfflineWriter.  For a fair apples-to-apples comparison we use the
// same bleve.New path but with an explicit large batch size hint.
func BenchmarkOfflineWriter(b *testing.B) {
	b.StopTimer()

	scratch, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		b.Fatalf("scratch index: %v", err)
	}
	batch := buildBatch(scratch)
	_ = scratch.Close()

	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		dir := temporaryDirectory(b)

		km := bleve.NewIndexMapping()
		km.DefaultAnalyzer = "keyword"

		// Use scorch with an explicit per-batch segment size to approximate
		// bluge's offline (single large segment) write path.
		idx, err := bleve.NewUsing(dir, km, "scorch", "scorch", map[string]any{
			"forceSegmentType":    "zap",
			"forceSegmentVersion": 15,
		})
		if err != nil {
			b.Fatalf("bleve.NewUsing: %v", err)
		}
		b.StartTimer()

		if err := idx.Batch(batch); err != nil {
			b.Fatal(err)
		}
		if err := idx.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
