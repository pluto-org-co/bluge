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

package bluge

import (
	"fmt"

	"github.com/blugelabs/bluge/index"
)

type OfflineWriter struct {
	idxWriter *index.WriterOffline
}

func OpenOfflineWriter(config Config) (writer *OfflineWriter, err error) {
	rv := &OfflineWriter{}

	rv.idxWriter, err = index.OpenOfflineWriter(config.indexConfig)
	if err != nil {
		return nil, fmt.Errorf("error opening index: %w", err)
	}

	return rv, nil
}

func (w *OfflineWriter) Batch(batch *index.Batch) (err error) {
	return w.idxWriter.Batch(batch)
}

func (w *OfflineWriter) Close() (err error) {
	return w.idxWriter.Close()
}
