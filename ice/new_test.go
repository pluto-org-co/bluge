//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"path/filepath"
	"testing"

	"github.com/pluto-org-co/bluge/documents"
	"github.com/pluto-org-co/bluge/segment"
)

func TestBuild(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	sb, err := buildTestSegment()
	if err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(path, "segment.ice")
	err = persistToFile(sb, segPath)
	if err != nil {
		t.Fatal(err)
	}
}

func buildTestSegment() (*Segment, error) {
	doc := documents.NewDocument("a").
		AddField(documents.NewTextField("name", "wow").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("desc", "some thing").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "cold").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "dark").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

	doc.Analyze() // ← try calling this explicitly first
	results := []segment.Document{
		doc,
	}

	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode)
	return seg.(*Segment), err
}

func buildTestSegmentMulti() (*Segment, error) {
	results := buildTestAnalysisResultsMulti()

	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode)
	return seg.(*Segment), err
}

func buildTestSegmentMultiWithChunkFactor(chunkFactor uint32) (*Segment, error) {
	results := buildTestAnalysisResultsMulti()

	seg, _, err := newWithChunkMode(results, encodeNorm, chunkFactor)
	return seg.(*Segment), err
}

func buildTestSegmentMultiWithDifferentFields(includeDocA, includeDocB bool) (*Segment, error) {
	results := buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB)

	seg, _, err := newWithChunkMode(results, encodeNorm, defaultChunkMode)
	return seg.(*Segment), err
}

func buildTestAnalysisResultsMulti() []segment.Document {
	doc := documents.NewDocument("a").
		AddField(documents.NewTextField("name", "wow").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("desc", "some thing").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "cold").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

	doc2 := documents.NewDocument("b").
		AddField(documents.NewTextField("name", "who").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("desc", "some thing").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "cold").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "dark").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

	doc.Analyze()  // ← try calling this explicitly first
	doc2.Analyze() // ← try calling this explicitly first
	results := []segment.Document{
		doc, doc2,
	}

	return results
}

func buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB bool) []segment.Document {
	var results []segment.Document

	if includeDocA {
		doc := documents.NewDocument("a").
			AddField(documents.NewTextField("name", "mat").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewTextField("dept", "ABC").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewTextField("manages.id", "XYZ").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewTextField("manages.count", "1").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

		doc.Analyze() // ← try calling this explicitly first
		results = append(results, doc)
	}

	if includeDocB {
		doc := documents.NewDocument("b").
			AddField(documents.NewTextField("name", "mat").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewTextField("dept", "ABC dept").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewTextField("reportsTo.id", "ABC").
				Aggregatable().
				Sortable().
				HighlightMatches().
				SearchTermPositions().
				StoreValue()).
			AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

		doc.Analyze() // ← try calling this explicitly first
		results = append(results, doc)
	}

	return results
}

func buildTestSegmentWithDefaultFieldMapping(chunkFactor uint32) (
	*Segment, []string, error) {
	doc := documents.NewDocument("a").
		AddField(documents.NewTextField("name", "wow").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("desc", "some thing").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewTextField("tag", "cold").
			Aggregatable().
			Sortable().
			HighlightMatches().
			SearchTermPositions().
			StoreValue()).
		AddField(documents.NewCompositeFieldExcluding("_all", []string{"_id"}))

	doc.Analyze() // ← try calling this explicitly first

	var fields []string
	fields = append(fields, "_id", "name", "desc", "tag")

	results := []segment.Document{
		doc,
	}

	sb, _, err := newWithChunkMode(results, encodeNorm, chunkFactor)

	return sb.(*Segment), fields, err
}
