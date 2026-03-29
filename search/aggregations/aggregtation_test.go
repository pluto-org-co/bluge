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

package aggregations

import (
	"math"
	"strconv"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/zeebo/xxh3"

	"github.com/blugelabs/bluge/numeric"
	"github.com/blugelabs/bluge/search"
)

func TestAggregations(t *testing.T) {
	global := buildTestAggregations()
	fieldsNeeded := global.Fields()
	assertFieldsSeen(t, []string{"age", "name", "type"}, fieldsNeeded)

	bucket := search.NewBucket(xxh3.HashString("global"), global)
	testDocs := buildTestDocs()
	for _, doc := range testDocs {
		err := doc.LoadDocumentValues(search.NewSearchContext(0, 0), global.Fields())
		if err != nil {
			t.Fatal(err)
		}
		bucket.Consume(doc)
	}
	bucket.Finish()

	expect := buildTestBucketExpectations()
	expect.Assert(t, bucket, "")
}

func TestAggregationMerge(t *testing.T) {
	global := buildTestAggregations()
	fieldsNeeded := global.Fields()
	assertFieldsSeen(t, []string{"age", "name", "type"}, fieldsNeeded)

	testDocs := buildTestDocs()

	// process the first 5 docs in shard1
	shard1 := search.NewBucket(xxh3.HashString("shard1"), global)
	for _, doc := range testDocs[0:5] {
		err := doc.LoadDocumentValues(search.NewSearchContext(0, 0), global.Fields())
		if err != nil {
			t.Fatal(err)
		}
		shard1.Consume(doc)
	}
	shard1.Finish()

	// process the next 5 docs in shard2
	shard2 := search.NewBucket(xxh3.HashString("shard2"), global)
	for _, doc := range testDocs[5:] {
		err := doc.LoadDocumentValues(search.NewSearchContext(0, 0), global.Fields())
		if err != nil {
			t.Fatal(err)
		}
		shard2.Consume(doc)
	}
	shard2.Finish()

	// merge shard2 into shard1
	shard1.Merge(shard2)

	expect := buildTestBucketExpectations()
	expect.Assert(t, shard1, "")
}

type matchReader struct {
	docVals map[string][]byte
}

func (mr *matchReader) DocumentValueReader(fields []string) (segment.DocumentValueReader, error) {
	return mr, nil
}

func (mr *matchReader) VisitDocumentValues(number uint64, visitor segment.DocumentValueVisitor) error {
	for k, v := range mr.docVals {
		visitor(k, v)
	}
	return nil
}

func (mr *matchReader) VisitStoredFields(number uint64, visitor segment.StoredFieldVisitor) error {
	return nil
}

func newDocumentMatch(number uint64, score float64, docVals map[string][]byte) *search.DocumentMatch {
	rv := &search.DocumentMatch{
		Number: number,
		Score:  score,
	}
	rv.SetReader(&matchReader{docVals: docVals})
	return rv
}

func buildTestDocs() []*search.DocumentMatch {
	return []*search.DocumentMatch{
		newDocumentMatch(0, 1.0,
			map[string][]byte{
				"name": []byte("barbara"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(1), 0),
			}),
		newDocumentMatch(1, 1.2,
			map[string][]byte{
				"name": []byte("john"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(25), 0),
			}),

		newDocumentMatch(2, 1.01,
			map[string][]byte{
				"name": []byte("barbara"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(16), 0),
			}),

		newDocumentMatch(3, 1.5,
			map[string][]byte{
				"name": []byte("dale"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(32), 0),
			}),
		newDocumentMatch(4, 1.6,
			map[string][]byte{
				"name": []byte("judy"),
				"type": []byte("contractor"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(48), 0),
			}),
		newDocumentMatch(5, 1.2,
			map[string][]byte{
				"name": []byte("donna"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(63), 0),
			}),

		newDocumentMatch(6, 1.2,
			map[string][]byte{
				"name": []byte("john"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(4), 0),
			}),

		newDocumentMatch(7, 1.14,
			map[string][]byte{
				"name": []byte("gary"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(95), 0),
			}),

		newDocumentMatch(8, 1.1,
			map[string][]byte{
				"name": []byte("john"),
				"type": []byte("contractor"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(39), 0),
			}),

		newDocumentMatch(9, 1.22,
			map[string][]byte{
				"name": []byte("carol"),
				"type": []byte("employee"),
				"age":  numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(11), 0),
			}),
	}
}

func assertFieldsSeen(t *testing.T, expectedFields, actualFields []string) {
	expectFields := map[string]bool{}
	for _, expectField := range expectedFields {
		expectFields[expectField] = false
	}
	for _, field := range actualFields {
		expectFields[field] = true
	}
	for field, seen := range expectFields {
		if !seen {
			t.Errorf("expected to see field '%s', did not", field)
		}
	}
}

type bucketExpectation struct {
	metrics  map[uint64]float64
	children map[uint64]map[uint64]*bucketExpectation
}

func (b bucketExpectation) Assert(t *testing.T, bucket *search.Bucket, path string) {
	for hash, agg := range bucket.Aggregations() {
		switch c := agg.(type) {
		case search.MetricCalculator:
			if expectMetricValue, ok := b.metrics[hash]; ok {
				if c.Value() != expectMetricValue {
					t.Errorf("expected value %f got %f for '%s'", expectMetricValue, c.Value(), path+"."+strconv.FormatUint(hash, 10))
				}
			} else {
				t.Errorf("unexpected metric %d in path '%s'", hash, path)
			}
			delete(b.metrics, hash)
		case search.BucketCalculator:
			if expectedBuckets, ok := b.children[hash]; ok {
				buckets := c.Buckets()
				if len(expectedBuckets) != len(buckets) {
					t.Errorf("expected %d buckets, got %d, at '%s'", len(expectedBuckets), len(buckets), path+"."+strconv.FormatUint(hash, 10))
				}
				for _, bucket := range buckets {
					if expectedBucket, ok := expectedBuckets[bucket.Hash()]; ok {
						expectedBucket.Assert(t, bucket, path+strconv.FormatUint(hash, 10)+"."+strconv.FormatUint(bucket.Hash(), 10))
					} else {
						t.Errorf("unexpected bucket %d in path '%s'", bucket.Hash(), path+"."+strconv.FormatUint(hash, 10))
					}
				}
			} else {
				t.Errorf("unexpected bucket aggregation %d in path '%s'", hash, path)
			}
			delete(b.children, hash)
		}
	}
	for metricName := range b.metrics {
		t.Errorf("expected a metric named %d at path '%s', was missing", metricName, path)
	}
	for aggName := range b.children {
		t.Errorf("expected an aggregation named: %d at path '%s', was missing", aggName, path)
	}
}

func buildTestAggregations() search.Aggregations {
	global := make(search.Aggregations)

	child := NamedRange("children", 0, 18)
	adult := NamedRange("adults", 18, math.Inf(1))

	byAge := Ranges(search.Field("age")).
		AddRange(child).
		AddRange(adult).
		AddAggregation(xxh3.HashString("min_age"), Min(search.Field("age"))).
		AddAggregation(xxh3.HashString("max_age"), Max(search.Field("age")))

	global.AddString("byAge", byAge)

	global.AddString("max_score", Max(search.DocumentScore()))
	global.AddString("doc_count", CountMatches())
	global.AddString("min_age", Min(search.Field("age")))
	global.AddString("max_age", Max(search.Field("age")))
	global.AddString("avg_age", Avg(search.Field("age")))

	global.AddString("quantiles", Quantiles(search.Field("age")))

	termsAgg := NewTermsAggregation(search.Field("name"), 2)
	global.AddString("byName", termsAgg)

	typesAgg := NewTermsAggregation(search.Field("type"), 2)
	global.AddString("byType", typesAgg)

	return global
}

func buildTestBucketExpectations() *bucketExpectation {
	return &bucketExpectation{
		metrics: map[uint64]float64{
			xxh3.HashString("doc_count"): 10.0,
			xxh3.HashString("max_score"): 1.6,
			xxh3.HashString("min_age"):   1.0,
			xxh3.HashString("max_age"):   95,
			xxh3.HashString("avg_age"):   33.4,
		},
		children: map[uint64]map[uint64]*bucketExpectation{
			xxh3.HashString("byType"): {
				xxh3.HashString("employee"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"): 8.0,
					},
				},
				xxh3.HashString("contractor"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"): 2.0,
					},
				},
			},
			xxh3.HashString("byName"): {
				xxh3.HashString("john"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"): 3.0,
					},
				},
				xxh3.HashString("barbara"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"): 2.0,
					},
				},
			},
			xxh3.HashString("byAge"): {
				xxh3.HashString("children"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"):   4.0,
						xxh3.HashString("min_age"): 1.0,
						xxh3.HashString("max_age"): 16.0,
					},
				},
				xxh3.HashString("adults"): &bucketExpectation{
					metrics: map[uint64]float64{
						xxh3.HashString("count"):   6.0,
						xxh3.HashString("min_age"): 25.0,
						xxh3.HashString("max_age"): 95.0,
					},
				},
			},
		},
	}
}
