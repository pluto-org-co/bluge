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

package index

import (
	"fmt"
)

func checkDocIDForNumber(indexReader *Snapshot, number uint64, docID string) error {
	var ok bool
	err := indexReader.VisitStoredFields(number, func(field string, value []byte) bool {
		if field == "_id" {
			if string(value) == docID {
				ok = true
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("did not find _id match")
	}
	return nil
}

func findNumberByID(indexReader *Snapshot, docID string) (uint64, error) {
	return findNumberByUniqueFieldTerm(indexReader, "_id", docID)
}

func findNumberByUniqueFieldTerm(indexReader *Snapshot, field, val string) (uint64, error) {
	tfr, err := indexReader.PostingsIterator([]byte(val), field, false, false, false)
	if err != nil {
		return 0, fmt.Errorf("error building tfr for %s = '%s'", field, val)
	}
	if tfr.Count() != 1 {
		return 0, fmt.Errorf("search by _id did not return exactly one hit, got %d", tfr.Count())
	}
	tfd, err := tfr.Next()
	if err != nil {
		return 0, fmt.Errorf("error getting term field doc: %v", err)
	}
	return tfd.Number(), nil
}
