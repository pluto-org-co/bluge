//  Copyright (c) 2020 The Bluge Authors.
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
	"github.com/pluto-org-co/bluge/numeric"
)

func addShiftTokens(terms []numeric.PrefixCoded, original int64, shiftBy uint) []numeric.PrefixCoded {
	shift := shiftBy
	for shift < 64 {
		shiftEncoded, err := numeric.NewPrefixCodedInt64(original, shift)
		if err != nil {
			break
		}
		terms = append(terms, shiftEncoded)
		shift += shiftBy
	}
	return terms
}
