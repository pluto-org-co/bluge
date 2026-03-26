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

package analysis_test

import (
	"reflect"
	"testing"

	"github.com/blugelabs/bluge/analysis"
	"github.com/zeebo/xxh3"
)

func TestTokenFrequency(t *testing.T) {
	tokens := analysis.TokenStream{
		&analysis.Token{
			Term:         []byte("water"),
			PositionIncr: 1,
			Start:        0,
			End:          5,
		},
		&analysis.Token{
			Term:         []byte("water"),
			PositionIncr: 1,
			Start:        6,
			End:          11,
		},
	}
	expectedResult := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
			Freq: 2,
		},
	}
	result, _ := analysis.TokenFrequency(tokens, true, 0)
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestTokenFrequenciesMergeAll(t *testing.T) {
	tf1 := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
		},
	}
	tf2 := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
		},
	}
	expectedResult := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
				{
					FieldVal:    "tf2",
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					FieldVal:    "tf2",
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
		},
	}
	tf1.MergeAll("tf2", tf2)
	if !reflect.DeepEqual(tf1, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, tf1)
	}
}

func TestTokenFrequenciesMergeAllLeftEmpty(t *testing.T) {
	tf1 := analysis.TokenFrequencies{}
	tf2 := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
		},
	}
	expectedResult := analysis.TokenFrequencies{
		xxh3.HashString("water"): &analysis.TokenFreq{
			TermVal: []byte("water"),
			Locations: []*analysis.TokenLocation{
				{
					FieldVal:    "tf2",
					PositionVal: 1,
					StartVal:    0,
					EndVal:      5,
				},
				{
					FieldVal:    "tf2",
					PositionVal: 2,
					StartVal:    6,
					EndVal:      11,
				},
			},
		},
	}
	tf1.MergeAll("tf2", tf2)
	if !reflect.DeepEqual(tf1, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, tf1)
	}
}
