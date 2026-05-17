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

package analysis

import (
	"reflect"

	"github.com/zeebo/xxh3"
)

var reflectStaticSizeTokenLocation int
var reflectStaticSizeTokenFreq int

func init() {
	var tl TokenLocation
	reflectStaticSizeTokenLocation = int(reflect.TypeOf(tl).Size())
	var tf TokenFreq
	reflectStaticSizeTokenFreq = int(reflect.TypeOf(tf).Size())
}

// TokenLocation represents one occurrence of a term at a particular location in
// a field. Start, End and Position have the same meaning as in analysis.Token.
// Field and ArrayPositions identify the field value in the source document.
// See document.Field for details.
type TokenLocation struct {
	FieldVal    string
	StartVal    int
	EndVal      int
	PositionVal int
}

func (tl *TokenLocation) Field() string {
	return tl.FieldVal
}

func (tl *TokenLocation) Pos() int {
	return tl.PositionVal
}

func (tl *TokenLocation) Start() int {
	return tl.StartVal
}

func (tl *TokenLocation) End() int {
	return tl.EndVal
}

func (tl *TokenLocation) Size() int {
	return reflectStaticSizeTokenLocation
}

// TokenFreq represents all the occurrences of a term in all fields of a
// document.
type TokenFreq struct {
	Field     string
	TermVal   []byte
	Locations []*TokenLocation
	Frequency uint64
}

func (tf *TokenFreq) Size() int {
	rv := reflectStaticSizeTokenFreq
	rv += len(tf.TermVal)
	for _, loc := range tf.Locations {
		rv += loc.Size()
	}
	return rv
}

// TokenFrequencies maps document terms to their combined frequencies from all
// fields.
type TokenFrequencies map[uint64]*TokenFreq

func (tfs TokenFrequencies) Size() int {
	rv := int(sizeOfMap)
	rv += len(tfs) * int(sizeOfString+sizeOfPtr)
	for _, v := range tfs {
		rv += len(v.Field)
		rv += v.Size()
	}
	return rv
}

func (tfs TokenFrequencies) MergeAll(remoteField string, other TokenFrequencies) {
	// walk the new token frequencies
	for tfk, tf := range other {
		tfs.mergeOne(remoteField, tfk, tf)
	}
}

func (tfs TokenFrequencies) mergeOne(otherFieldName string, tfk uint64, tf *TokenFreq) {
	// set the remoteField value in incoming token freqs
	for _, l := range tf.Locations {
		l.FieldVal = otherFieldName
	}
	existingTf, exists := tfs[tfk]
	if exists {
		existingTf.Locations = append(existingTf.Locations, tf.Locations...)
		existingTf.Frequency += tf.Frequency
	} else {
		tfs[tfk] = &TokenFreq{
			Field:     tf.Field,
			TermVal:   tf.TermVal,
			Frequency: tf.Frequency,
			Locations: make([]*TokenLocation, len(tf.Locations)),
		}
		copy(tfs[tfk].Locations, tf.Locations)
	}
}

func (tfs TokenFrequencies) MergeOneBytes(remoteField string, tfk []byte, tf *TokenFreq) {
	// set the remoteField value in incoming token freqs
	for _, l := range tf.Locations {
		l.FieldVal = remoteField
	}

	tfsKey := xxh3.Hash(tfk)
	existingTf, exists := tfs[tfsKey]
	if exists {
		existingTf.Locations = append(existingTf.Locations, tf.Locations...)
		existingTf.Frequency += tf.Frequency
	} else {
		tfs[tfsKey] = &TokenFreq{
			Field:     tf.Field,
			TermVal:   tf.TermVal,
			Frequency: tf.Frequency,
			Locations: make([]*TokenLocation, len(tf.Locations)),
		}
		copy(tfs[tfsKey].Locations, tf.Locations)
	}
}

func TokenFrequency(tokens TokenStream, includeTermVectors bool, startOffset int) (
	tokenFreqs TokenFrequencies, position int) {
	tokenFreqs = make(TokenFrequencies, len(tokens))

	if includeTermVectors {
		tls := make([]TokenLocation, len(tokens))
		tlNext := 0

		position = startOffset
		for _, token := range tokens {
			position += token.PositionIncr
			tls[tlNext] = TokenLocation{
				StartVal:    token.Start,
				EndVal:      token.End,
				PositionVal: position,
			}

			tokenFreqsKey := xxh3.Hash(token.Term)
			curr, ok := tokenFreqs[tokenFreqsKey]
			if ok {
				curr.Locations = append(curr.Locations, &tls[tlNext])
				curr.Frequency++
			} else {
				tokenFreqs[tokenFreqsKey] = &TokenFreq{
					TermVal:   token.Term,
					Locations: []*TokenLocation{&tls[tlNext]},
					Frequency: 1,
				}
			}

			tlNext++
		}
	} else {
		for _, token := range tokens {
			tokenFreqsKey := xxh3.Hash(token.Term)
			curr, exists := tokenFreqs[tokenFreqsKey]
			if exists {
				curr.Frequency++
			} else {
				tokenFreqs[tokenFreqsKey] = &TokenFreq{
					TermVal:   token.Term,
					Frequency: 1,
				}
			}
		}
	}

	return tokenFreqs, position
}
