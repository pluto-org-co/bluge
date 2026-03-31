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
	"testing"
	"unicode"

	"github.com/blugelabs/bluge/analysis"
	"github.com/stretchr/testify/assert"
)

func TestCharacterTokenizer(t *testing.T) {
	type Test struct {
		Name      string
		Input     []byte
		Tokenizer analysis.Tokenizer
		Expect    analysis.TokenStream
	}
	tests := []Test{
		{
			Name:      "Hello World",
			Input:     []byte("Hello World."),
			Tokenizer: NewCharacterTokenizer(unicode.IsLetter),
			Expect: analysis.TokenStream{
				{
					Start:        0,
					End:          5,
					Term:         []byte("Hello"),
					PositionIncr: 1,
					Type:         analysis.AlphaNumeric,
				},
				{
					Start:        6,
					End:          11,
					Term:         []byte("World"),
					PositionIncr: 1,
					Type:         analysis.AlphaNumeric,
				},
			},
		},
		{
			Name:      "Mail",
			Input:     []byte("dominique@mcdiabetes.com"),
			Tokenizer: NewCharacterTokenizer(unicode.IsLetter),
			Expect: analysis.TokenStream{
				{
					Start:        0,
					End:          9,
					Term:         []byte("dominique"),
					PositionIncr: 1,
					Type:         analysis.AlphaNumeric,
				},
				{
					Start:        10,
					End:          20,
					Term:         []byte("mcdiabetes"),
					PositionIncr: 1,
					Type:         analysis.AlphaNumeric,
				},
				{
					Start:        21,
					End:          24,
					Term:         []byte("com"),
					PositionIncr: 1,
					Type:         analysis.AlphaNumeric,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			assertions := assert.New(t)

			actual := test.Tokenizer.Tokenize(test.Input)
			if !assertions.Equal(test.Expect, actual, "expecting different value") {
				return
			}
		})
	}
}
