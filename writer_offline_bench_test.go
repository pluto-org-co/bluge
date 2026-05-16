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
	"testing"

	"github.com/pluto-org-co/bluge/documents"
	"github.com/pluto-org-co/bluge/index"
	"github.com/pluto-org-co/bluge/testsuite"
	"github.com/stretchr/testify/assert"
)

func BenchmarkOfflineWriter(b *testing.B) {
	assertions := assert.New(b)

	b.StopTimer()
	const docCount = 1_000_000
	batch := index.NewBatch()
	for index := range docCount {
		doc := documents.NewDocument(fmt.Sprintf("%d", index)).
			AddField(documents.NewKeywordField("name", fmt.Sprintf("hello-%d", index))).
			AddField(documents.NewKeywordField("index", fmt.Sprintf("%d", index))).
			AddField(documents.NewKeywordField("reversed-name", fmt.Sprintf("olleh-%d", index)))
		batch.Insert(doc)
	}
	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		tmpIndexPath := testsuite.TemporaryDirectory(b)

		config := DefaultConfig(tmpIndexPath)
		writer, err := OpenOfflineWriter(config)
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

func BenchmarkOfflineWriterWithDefinitions(b *testing.B) {
	assertions := assert.New(b)

	b.StopTimer()
	const docCount = 1_000_000
	batch := index.NewBatch()
	for index := range docCount {
		info, fields := documents.FieldsFromDefinitions(
			documents.NewKeywordFieldDefinition("name", fmt.Sprintf("hello-%d", index)),
			documents.NewKeywordFieldDefinition("index", fmt.Sprintf("%d", index)),
			documents.NewKeywordFieldDefinition("reversed-name", fmt.Sprintf("olleh-%d", index)),
		)
		doc := documents.NewDocumentWithFields(
			fmt.Sprintf("%d", index),
			info, fields...,
		)
		batch.Insert(doc)
	}
	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		tmpIndexPath := testsuite.TemporaryDirectory(b)

		config := DefaultConfig(tmpIndexPath)
		writer, err := OpenOfflineWriter(config)
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

func BenchmarkOfflineWriterWithDefinitionsManagedId(b *testing.B) {
	assertions := assert.New(b)

	b.StopTimer()
	const docCount = 1_000_000
	batch := index.NewBatch()
	for index := range docCount {
		info, fields := documents.FieldsFromDefinitionsWithId(
			fmt.Sprintf("%d", index),
			documents.NewKeywordFieldDefinition("name", fmt.Sprintf("hello-%d", index)),
			documents.NewKeywordFieldDefinition("index", fmt.Sprintf("%d", index)),
			documents.NewKeywordFieldDefinition("reversed-name", fmt.Sprintf("olleh-%d", index)),
		)
		doc := documents.NewDocumentWithFieldsManagedId(info, fields...)
		batch.Insert(doc)
	}
	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		b.StopTimer()
		tmpIndexPath := testsuite.TemporaryDirectory(b)

		config := DefaultConfig(tmpIndexPath)
		writer, err := OpenOfflineWriter(config)
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
