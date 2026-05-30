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

package ice

import (
	"github.com/zeebo/xxh3"
)

type CollectionStats struct {
	TotalDocumentCount    uint64
	DocumentCount         uint64
	SumTotalTermFrequency uint64
}

func (c *CollectionStats) Merge(other *CollectionStats) {
	c.TotalDocumentCount += other.TotalDocumentCount
	c.DocumentCount += other.DocumentCount
	c.SumTotalTermFrequency += other.SumTotalTermFrequency
}

func (s *Segment) CollectionStats(field string) (stats CollectionStats) {
	if fieldIDPlus1 := s.fieldsMap[xxh3.HashString(field)]; fieldIDPlus1 > 0 {
		return CollectionStats{
			TotalDocumentCount:    s.footer.numDocs,
			DocumentCount:         s.fieldDocs[fieldIDPlus1-1],
			SumTotalTermFrequency: s.fieldFreqs[fieldIDPlus1-1],
		}
	}
	return CollectionStats{}
}
