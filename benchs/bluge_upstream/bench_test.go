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

package bluge_upstream

import (
	"fmt"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/pluto-org-co/bluge/testsuite"
	"github.com/stretchr/testify/assert"
)

const WriterDocumentCount = 1_000_000

func BenchmarkWriter(b *testing.B) {
	assertions := assert.New(b)

	b.StopTimer()
	batch := index.NewBatch()
	for index := range WriterDocumentCount {
		doc := bluge.NewDocument(fmt.Sprintf("%d", index)).
			AddField(bluge.NewKeywordField("name", fmt.Sprintf("hello-%d", index))).
			AddField(bluge.NewKeywordField("index", fmt.Sprintf("%d", index))).
			AddField(bluge.NewKeywordField("reversed-name", fmt.Sprintf("olleh-%d", index)))
		batch.Insert(doc)
	}
	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		tmpIndexPath := testsuite.TemporaryDirectory(b)

		config := bluge.DefaultConfig(tmpIndexPath)
		writer, err := bluge.OpenWriter(config)
		if !assertions.Nil(err, "failed to open offline writer") {
			return
		}

		b.StartTimer()
		err = writer.Batch(batch)
		if err != nil {
			b.Fatal(err)
		}

		err = writer.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
