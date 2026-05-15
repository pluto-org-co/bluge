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

	"github.com/pluto-org-co/bluge/index"
	"github.com/pluto-org-co/bluge/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestOfflineWriter(t *testing.T) {
	assertions := assert.New(t)
	tmpIndexPath := testsuite.TemporaryDirectory(t)

	config := DefaultConfig(tmpIndexPath)
	writer, err := OpenOfflineWriter(config)
	if !assertions.Nil(err, "failed to open writer") {
		return
	}

	const docCount uint64 = 1_000_000

	batch := index.NewBatch()
	for index := range docCount {
		doc := NewDocument(fmt.Sprintf("%d", index)).
			AddField(NewKeywordField("name", fmt.Sprintf("hello-%d", index))).
			AddField(NewKeywordField("index", fmt.Sprintf("%d", index))).
			AddField(NewKeywordField("reversed-name", fmt.Sprintf("olleh-%d", index)))
		batch.Insert(doc)
	}

	err = writer.Batch(batch)
	if !assertions.Nil(err, "failed to write batch") {
		return
	}

	err = writer.Close()
	if !assertions.Nil(err, "failed close writer") {
		return
	}

	indexReader, err := OpenReader(config)
	if !assertions.Nil(err, "failed to open reader") {
		return
	}
	t.Cleanup(func() { indexReader.Close() })

	idxDocCount, err := indexReader.Count()
	if !assertions.Nil(err, "failed to get index count") {
		return
	}
	if !assertions.Equal(docCount, idxDocCount, "expecting exact amount of documents") {
		return
	}

	req := NewAllMatches(NewMatchAllQuery())
	res, err := indexReader.Search(t.Context(), req)
	if !assertions.Nil(err, "failed to search") {
		return
	}

	var searchCount uint64
	for {
		doc, err := res.Next()
		if !assertions.Nil(err, "failed to iter to next value") {
			return
		}
		if doc == nil {
			break
		}
		searchCount++
	}

	if !assertions.Equal(docCount, searchCount, "expecting same amount of search results") {
		return
	}
}

func TestOfflineWriterWithDefinitions(t *testing.T) {
	assertions := assert.New(t)
	tmpIndexPath := testsuite.TemporaryDirectory(t)

	config := DefaultConfig(tmpIndexPath)
	writer, err := OpenOfflineWriter(config)
	if !assertions.Nil(err, "failed to open writer") {
		return
	}

	const docCount uint64 = 1_000_000

	batch := index.NewBatch()
	for index := range docCount {
		info, fields := FieldsFromDefinitions(
			NewKeywordFieldDefinition("name", fmt.Sprintf("hello-%d", index)),
			NewKeywordFieldDefinition("index", fmt.Sprintf("%d", index)),
			NewKeywordFieldDefinition("reversed-name", fmt.Sprintf("olleh-%d", index)),
		)
		doc := NewDocumentWithFields(fmt.Sprintf("%d", index), info, fields...)
		batch.Insert(doc)
	}

	err = writer.Batch(batch)
	if !assertions.Nil(err, "failed to write batch") {
		return
	}

	err = writer.Close()
	if !assertions.Nil(err, "failed close writer") {
		return
	}

	indexReader, err := OpenReader(config)
	if !assertions.Nil(err, "failed to open reader") {
		return
	}
	t.Cleanup(func() { indexReader.Close() })

	idxDocCount, err := indexReader.Count()
	if !assertions.Nil(err, "failed to get index count") {
		return
	}
	if !assertions.Equal(docCount, idxDocCount, "expecting exact amount of documents") {
		return
	}

	req := NewAllMatches(NewMatchAllQuery())
	res, err := indexReader.Search(t.Context(), req)
	if !assertions.Nil(err, "failed to search") {
		return
	}

	var searchCount uint64
	for {
		doc, err := res.Next()
		if !assertions.Nil(err, "failed to iter to next value") {
			return
		}
		if doc == nil {
			break
		}
		searchCount++
	}

	if !assertions.Equal(docCount, searchCount, "expecting same amount of search results") {
		return
	}
}

func TestOfflineWriterWithDefinitionsManagedId(t *testing.T) {
	assertions := assert.New(t)
	tmpIndexPath := testsuite.TemporaryDirectory(t)

	config := DefaultConfig(tmpIndexPath)
	writer, err := OpenOfflineWriter(config)
	if !assertions.Nil(err, "failed to open writer") {
		return
	}

	const docCount uint64 = 1_000_000

	batch := index.NewBatch()
	for index := range docCount {
		info, fields := FieldsFromDefinitionsWithId(
			fmt.Sprintf("%d", index),
			NewKeywordFieldDefinition("name", fmt.Sprintf("hello-%d", index)),
			NewKeywordFieldDefinition("index", fmt.Sprintf("%d", index)),
			NewKeywordFieldDefinition("reversed-name", fmt.Sprintf("olleh-%d", index)),
		)
		doc := NewDocumentWithFieldsManagedId(info, fields...)
		batch.Insert(doc)
	}

	err = writer.Batch(batch)
	if !assertions.Nil(err, "failed to write batch") {
		return
	}

	err = writer.Close()
	if !assertions.Nil(err, "failed close writer") {
		return
	}

	indexReader, err := OpenReader(config)
	if !assertions.Nil(err, "failed to open reader") {
		return
	}
	t.Cleanup(func() { indexReader.Close() })

	idxDocCount, err := indexReader.Count()
	if !assertions.Nil(err, "failed to get index count") {
		return
	}
	if !assertions.Equal(docCount, idxDocCount, "expecting exact amount of documents") {
		return
	}

	req := NewAllMatches(NewMatchAllQuery())
	res, err := indexReader.Search(t.Context(), req)
	if !assertions.Nil(err, "failed to search") {
		return
	}

	var searchCount uint64
	for {
		doc, err := res.Next()
		if !assertions.Nil(err, "failed to iter to next value") {
			return
		}
		if doc == nil {
			break
		}
		searchCount++
	}

	if !assertions.Equal(docCount, searchCount, "expecting same amount of search results") {
		return
	}
}
