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

package searcher

import (
	"math"

	"github.com/pluto-org-co/bluge/documents"
	"github.com/pluto-org-co/bluge/search"
	"github.com/pluto-org-co/bluge/search/similarity"
)

var baseTestIndexReaderDirect *stubIndexReader
var baseTestIndexReader search.Reader

func init() {
	baseTestIndexReaderDirect = newStubIndexReader()
	for _, doc := range baseTestIndexDocs {
		baseTestIndexReaderDirect.add(doc)
	}
	baseTestIndexReader = baseTestIndexReaderDirect
}

var testSearchOptions = search.SearcherOptions{
	SimilarityForField: func(field string) search.Similarity {
		return similarity.NewBM25Similarity()
	},
	Explain: true,
}

func makeDoc(id string, fields ...*documents.Field) *documents.Document {
	doc := documents.NewDocument(id)
	for _, f := range fields {
		doc.AddField(f)
	}
	doc.Analyze()
	return doc
}

var baseTestIndexDocs = []*documents.Document{
	// must have 4/4 beer
	func() *documents.Document {
		doc := documents.NewDocument("1").
			AddField(documents.NewTextField("name", "marty").
				Aggregatable()).
			AddField(documents.NewTextField("desc", "beer beer beer beer").
				SearchTermPositions().
				Aggregatable()).
			AddField(documents.NewTextField("street", "couchbase way").
				Aggregatable())
		doc.Analyze()
		return doc
	}(),
	// must have 1/4 beer
	func() *documents.Document {
		doc := documents.NewDocument("2").
			AddField(documents.NewTextField("name", "steve").
				Aggregatable()).
			AddField(documents.NewTextField("desc", "angst beer couch database").
				SearchTermPositions().
				Aggregatable()).
			AddField(documents.NewTextField("street", "couchbase way").
				Aggregatable()).
			AddField(documents.NewTextField("title", "mister").
				Aggregatable())
		doc.Analyze()
		return doc
	}(),
	// must have 1/4 beer
	func() *documents.Document {
		doc := documents.NewDocument("3").
			AddField(documents.NewTextField("name", "dustin").
				Aggregatable()).
			AddField(documents.NewTextField("desc", "apple beer column dank").
				SearchTermPositions().
				Aggregatable()).
			AddField(documents.NewTextField("title", "mister").
				Aggregatable())
		doc.Analyze()
		return doc
	}(),
	// must have 65/65 beer
	func() *documents.Document {
		doc := documents.NewDocument("4").
			AddField(documents.NewTextField("name", "ravi").
				Aggregatable()).
			AddField(documents.NewTextField("desc", "beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer").
				SearchTermPositions().
				Aggregatable())
		doc.Analyze()
		return doc
	}(),
	// must have 0/x beer
	func() *documents.Document {
		doc := documents.NewDocument("5").
			AddField(documents.NewTextField("name", "bobert").
				Aggregatable()).
			AddField(documents.NewTextField("desc", "water").
				SearchTermPositions().
				Aggregatable()).
			AddField(documents.NewTextField("title", "mister").
				Aggregatable())
		doc.Analyze()
		return doc
	}(),
}

func scoresCloseEnough(a, b float64) bool {
	return math.Abs(a-b) < 0.001
}
