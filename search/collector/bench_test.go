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

package collector

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/stretchr/testify/assert"

	"github.com/blugelabs/bluge/search"
)

type createCollector func() search.Collector

func benchHelper(numOfMatches int, cc createCollector, b *testing.B) {
	assertions := assert.New(b)

	var random = rand.New(rand.NewChaCha8([32]byte{2, 0, 2, 6, 'A', 'n', 't', 'o', 'n', 'i', 'o'}))

	matches := make([]*search.DocumentMatch, 0, numOfMatches)
	for i := range numOfMatches {
		matches = append(matches, &search.DocumentMatch{
			Number: uint64(i),
			Score:  random.Float64(),
		})
	}

	b.ResetTimer()

	for b.Loop() {
		searcher := &stubSearcher{
			matches: matches,
		}
		collector := cc()
		aggs := search.Aggregations{
			"count":     aggregations.CountMatches(),
			"max_score": aggregations.Max(search.DocumentScore()),
		}
		dmi, err := collector.Collect(context.Background(), aggs, searcher)
		if !assertions.Nil(err, "failed to collect from searcher") {
			return
		}

		for result, err := dmi.Next(); result != nil && err == nil; result, err = dmi.Next() {
		}
		if !assertions.Nil(err, "failed to iterate results") {
			return
		}
	}
}
