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

package tokenizer

import (
	"github.com/blevesearch/segment"

	"github.com/blugelabs/bluge/analysis"
)

const maxEstimatedRemainingSegments = 1000
const maxRvCapacity = 256

type UnicodeTokenizer struct{}

func NewUnicodeTokenizer() *UnicodeTokenizer {
	return &UnicodeTokenizer{}
}

func (rt *UnicodeTokenizer) Tokenize(input []byte) analysis.TokenStream {
	rvx := make([]analysis.TokenStream, 0, 10) // When rv gets full, append to rvx.
	rv := make(analysis.TokenStream, 0, 1)

	ta := []analysis.Token(nil)
	taNext := 0

	segmenter := segment.NewWordSegmenterDirect(input)
	start := 0

	guessRemaining := func(end int) int {
		avgSegmentLen := max(1, end/(len(rv)+1))

		remainingLen := len(input) - end

		return remainingLen / avgSegmentLen
	}

	for segmenter.Segment() {
		segmentBytes := segmenter.Bytes()
		end := start + len(segmentBytes)
		if segmenter.Type() != segment.None {
			if taNext >= len(ta) {
				remainingSegments := max(1, min(maxEstimatedRemainingSegments, guessRemaining(end)))

				ta = make([]analysis.Token, remainingSegments)
				taNext = 0
			}

			token := &ta[taNext]
			taNext++

			token.Term = segmentBytes
			token.Start = start
			token.End = end
			token.PositionIncr = 1
			token.Type = convertType(segmenter.Type())

			if len(rv) >= cap(rv) { // When rv is full, save it into rvx.
				rvx = append(rvx, rv)

				rvCap := min(maxRvCapacity, cap(rv)*2)

				rv = make(analysis.TokenStream, 0, rvCap) // Next rv cap is bigger.
			}

			rv = append(rv, token)
		}
		start = end
	}

	if len(rvx) > 0 {
		n := len(rv)
		for _, r := range rvx {
			n += len(r)
		}
		rall := make(analysis.TokenStream, 0, n)
		for _, r := range rvx {
			rall = append(rall, r...)
		}
		rv = append(rall, rv...)
	}

	return rv
}

func convertType(segmentWordType int) analysis.TokenType {
	switch segmentWordType {
	case segment.Ideo:
		return analysis.Ideographic
	case segment.Kana:
		return analysis.Ideographic
	case segment.Number:
		return analysis.Numeric
	default:
		return analysis.AlphaNumeric
	}
}
