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

package index

import (
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/pluto-org-co/bluge/documents"
	"github.com/pluto-org-co/bluge/segment"
)

func CreateConfig(name string) (config Config, cleanup func() error) {
	path, err := ioutil.TempDir("", "bluge-index-test"+name)
	if err != nil {
		panic(err)
	}
	rv := DefaultConfig(path).
		WithPersisterNapTimeMSec(1).
		WithNormCalc(func(_ string, numTerms int) float32 {
			return math.Float32frombits(uint32(numTerms))
		}).
		WithVirtualField(documents.NewKeywordField("", ""))
	return rv, func() error { return os.RemoveAll(path) }
}

func TestIndexOpenReopen(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexOpenReopenWithInsert(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = OpenWriter(cfg)
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}

	doc = documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test2").
			Aggregatable())
	doc.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsert(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsert")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenDelete(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertThenDelete")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc2 := documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc2.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("2"), doc2)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	b3 := NewBatch()
	b3.Delete(documents.Identifier("1"))
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	b4 := NewBatch()
	b4.Delete(documents.Identifier("2"))
	err = idx.Batch(b4)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenUpdate(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertThenUpdate")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var expectedCount uint64
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test fail").
			Aggregatable())
	doc.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	doc = documents.NewDocument("1").
		AddField(documents.NewTextField("name", "fail").
			Aggregatable())
	doc.Analyze()
	b3 := NewBatch()
	b3.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertMultiple")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = documents.NewDocument("3").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b3 := NewBatch()
	b3.Update(documents.Identifier("3"), doc)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertWithStore(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertWithStore")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			StoreValue().
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	var storedFieldCount int
	err = indexReader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		storedFieldCount++
		if field == "name" {
			if string(value) != "test" {
				t.Errorf("expected name to be 'test', got '%s'", string(value))
			}
		} else if field == "_id" {
			if string(value) != "1" {
				t.Errorf("expected _id to be 1, got '%s'", string(value))
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if storedFieldCount != 2 {
		t.Errorf("expected 2 stored fields, got %d", storedFieldCount)
	}
}

func TestIndexBatch(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexBatch")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test2").
			Aggregatable())
	doc.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	batch := NewBatch()
	doc = documents.NewDocument("3").
		AddField(documents.NewTextField("name", "test3").
			Aggregatable())
	doc.Analyze()
	batch.Update(documents.Identifier("3"), doc)
	doc = documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test2updated").
			Aggregatable())
	doc.Analyze()
	batch.Update(documents.Identifier("2"), doc)
	batch.Delete(documents.Identifier("1"))

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	numSegments := len(indexReader.segment)
	if numSegments <= 0 {
		t.Errorf("expected some segments, got: %d", numSegments)
	}

	docCount, err := indexReader.Count()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	pi, err := indexReader.postingsIteratorAll("")
	if err != nil {
		t.Fatal(err)
	}

	var docIDs []string
	var posting segment.Posting
	posting, err = pi.Next()
	for err == nil && posting != nil {
		err = indexReader.VisitStoredFields(posting.Number(), func(field string, value []byte) bool {
			if field == "_id" {
				docIDs = append(docIDs, string(value))
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		posting, err = pi.Next()
	}
	if err != nil {
		t.Fatalf("error getting postings")
	}

	slices.Sort(docIDs)
	expectedIDs := []string{"2", "3"}
	if !reflect.DeepEqual(expectedIDs, docIDs) {
		t.Errorf("expected ids: %v, got ids: %v", expectedIDs, docIDs)
	}
}

func TestIndexBatchWithCallbacks(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexBatchWithCallbacks")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Fatal(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	batch := NewBatch()
	doc := documents.NewDocument("3").
		AddField(documents.NewTextField("name", "test3").
			Aggregatable())
	doc.Analyze()
	batch.Update(documents.Identifier("3"), doc)
	batch.SetPersistedCallback(func(e error) {
		wg.Done()
	})

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestIndexUpdateComposites(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexUpdateComposites")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = documents.NewDocument("1").
		AddField(documents.NewTextField("name", "testupdated").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewTextField("title", "misterupdated").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	var fieldCount int
	err = indexReader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		fieldCount++
		if field == "name" {
			if string(value) != "testupdated" {
				t.Errorf("expected field content 'testupdated', got '%s'", string(value))
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if fieldCount != 3 {
		t.Errorf("expected 3 stored fields, got %d", fieldCount)
	}
}

func TestIndexTermReaderCompositeFields(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexTermReaderCompositeFields")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum, err := findNumberByUniqueFieldTerm(indexReader, "_all", "mister")
	if err != nil {
		t.Fatal(err)
	}

	err = checkDocIDForNumber(indexReader, docNum, "1")
	if err != nil {
		t.Fatal(err)
	}

	pi, err := indexReader.PostingsIterator([]byte("mister"), "_all", true, true, true)
	if err != nil {
		t.Fatal(err)
	}

	var docIDs []string
	var posting segment.Posting
	posting, err = pi.Next()
	for err == nil && posting != nil {
		err = indexReader.VisitStoredFields(posting.Number(), func(field string, value []byte) bool {
			if field == "_id" {
				docIDs = append(docIDs, string(value))
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		posting, err = pi.Next()
	}
	if err != nil {
		t.Fatalf("error getting postings")
	}

	slices.Sort(docIDs)
	expectedIDs := []string{"1"}
	if !reflect.DeepEqual(expectedIDs, docIDs) {
		t.Errorf("expected ids: %v, got ids: %v", expectedIDs, docIDs)
	}
}

func TestIndexDocumentVisitFieldTerms(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTerms")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister").
			StoreValue().
			SearchTermPositions().
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	fieldTerms := make(map[string][]string)

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	dvReader, err := indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNum1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
}

func TestConcurrentUpdate(t *testing.T) {
	cfg, cleanup := CreateConfig("TestConcurrentUpdate")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			doc := documents.NewDocument("1").
				AddField(documents.NewTextField(strconv.Itoa(i), strconv.Itoa(i)).
					StoreValue().
					Aggregatable())
			doc.Analyze()
			b := NewBatch()
			b.Update(documents.Identifier("1"), doc)
			err2 := idx.Batch(b)
			if err2 != nil {
				t.Errorf("Error updating index: %v", err2)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	r, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(r, "1")
	if err != nil {
		t.Fatal(err)
	}

	var fieldCount int
	err = r.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		fieldCount++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if fieldCount != 2 {
		t.Errorf("expected 2 fields, got %d", fieldCount)
	}
}

func TestLargeField(t *testing.T) {
	cfg, cleanup := CreateConfig("TestLargeField")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var largeFieldValue []byte
	for len(largeFieldValue) < 4096 {
		largeFieldValue = append(largeFieldValue, bleveWikiArticle1K...)
	}

	d := documents.NewDocument("large").
		AddField(documents.NewTextField("desc", string(largeFieldValue)).
			StoreValue().
			Aggregatable())
	d.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), d)
	err = idx.Batch(b)
	if err != nil {
		t.Fatal(err)
	}
}

var bleveWikiArticle1K = []byte(`Boiling liquid expanding vapor explosion
From Wikipedia, the free encyclopedia
See also: Boiler explosion and Steam explosion

Flames subsequent to a flammable liquid BLEVE from a tanker. BLEVEs do not necessarily involve fire.

This article's tone or style may not reflect the encyclopedic tone used on Wikipedia. See Wikipedia's guide to writing better articles for suggestions. (July 2013)
A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.[1]
Contents  [hide]
1 Mechanism
1.1 Water example
1.2 BLEVEs without chemical reactions
2 Fires
3 Incidents
4 Safety measures
5 See also
6 References
7 External links
Mechanism[edit]

This section needs additional citations for verification. Please help improve this article by adding citations to reliable sources. Unsourced material may be challenged and removed. (July 2013)
There are three characteristics of liquids which are relevant to the discussion of a BLEVE:`)

func TestIndexDocumentVisitFieldTermsWithMultipleDocs(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleDocs")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister").
			StoreValue().
			SearchTermPositions().
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms := make(map[string][]string)
	docNumber1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err := indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc2 := documents.NewDocument("2").
		AddField(documents.NewTextField("name", "test2").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister2").
			StoreValue().
			SearchTermPositions().
			Aggregatable())
	doc2.Analyze()
	b2 := NewBatch()
	b2.Update(documents.Identifier("2"), doc2)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms = make(map[string][]string)
	docNumber2, err := findNumberByID(indexReader, "2")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber2, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name":  {"test2"},
		"title": {"mister2"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc3 := documents.NewDocument("3").
		AddField(documents.NewTextField("name3", "test3").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("title3", "mister3").
			StoreValue().
			SearchTermPositions().
			Aggregatable())
	doc3.Analyze()
	b3 := NewBatch()
	b3.Update(documents.Identifier("3"), doc3)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms = make(map[string][]string)
	docNumber3, err := findNumberByID(indexReader, "3")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name3", "title3"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber3, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name3":  {"test3"},
		"title3": {"mister3"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}

	fieldTerms = make(map[string][]string)
	docNumber1, err = findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexDocumentVisitFieldTermsWithMultipleFieldOptions(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleFieldOptions")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "test").
			Aggregatable()).
		AddField(documents.NewTextField("title", "mister").
			Aggregatable()).
		AddField(documents.NewTextField("designation", "engineer").
			StoreValue().
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewTextField("dept", "bleve").
			StoreValue().
			SearchTermPositions().
			Aggregatable())
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms := make(map[string][]string)
	docNumber1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err := indexReader.DocumentValueReader([]string{"name", "designation", "dept"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":        {"test"},
		"designation": {"engineer"},
		"dept":        {"bleve"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAllFieldWithDifferentTermVectorsEnabled(t *testing.T) {
	cfg, cleanup := CreateConfig("TestAllFieldWithDifferentTermVectorsEnabled")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("keyword", "something").
			Aggregatable()).
		AddField(documents.NewTextField("text", "A sentence that includes something within.").
			SearchTermPositions().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	b := NewBatch()
	b.Update(documents.Identifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
}

func TestIndexSeekBackwardsStats(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := NewBatch()
	doc := documents.NewDocument("1").
		AddField(documents.NewTextField("name", "cat").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	batch.Update(documents.Identifier("1"), doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	batch.Reset()
	doc = documents.NewDocument("2").
		AddField(documents.NewTextField("name", "cat").
			StoreValue().
			Aggregatable()).
		AddField(documents.NewCompositeFieldExcluding("_all", nil))
	doc.Analyze()
	batch.Update(documents.Identifier("2"), doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	tfr, err := reader.PostingsIterator([]byte("cat"), "name", false, false, false)
	if err != nil {
		t.Fatalf("error getting term field readyer for name/cat: %v", err)
	}

	tfdFirst, err := tfr.Next()
	if err != nil {
		t.Fatalf("error getting first tfd: %v", err)
	}

	_, err = tfr.Next()
	if err != nil {
		t.Fatalf("error getting second tfd: %v", err)
	}

	_, err = tfr.Advance(tfdFirst.Number())
	if err != nil {
		t.Fatalf("error adancing backwards: %v", err)
	}

	err = tfr.Close()
	if err != nil {
		t.Fatalf("error closing term field reader: %v", err)
	}

	if idx.stats.TotTermSearchersStarted != idx.stats.TotTermSearchersFinished {
		t.Errorf("expected term searchers started %d to equal term searchers finished %d",
			idx.stats.TotTermSearchersStarted,
			idx.stats.TotTermSearchersFinished)
	}
}
