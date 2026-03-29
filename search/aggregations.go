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

package search

import (
	"time"

	"github.com/zeebo/xxh3"
)

var (
	CountHash    = xxh3.HashString("count")
	MaxScoreHash = xxh3.HashString("max_score")
	UpdatedHash  = xxh3.HashString("updated")
	DurationHash = xxh3.HashString("duration")
	RatingsHash  = xxh3.HashString("ratings")
	TypesHash    = xxh3.HashString("types")
)

type Aggregation interface {
	Fields() []string
	Calculator() Calculator
}

type Aggregations map[uint64]Aggregation

func (a Aggregations) Add(hash uint64, aggregation Aggregation) {
	a[hash] = aggregation
}

func (a Aggregations) AddString(name string, aggregation Aggregation) {
	a[xxh3.HashString(name)] = aggregation
}

func (a Aggregations) Fields() []string {
	var rv []string
	for _, aggregation := range a {
		rv = append(rv, aggregation.Fields()...)
	}
	return rv
}

type Calculator interface {
	Consume(*DocumentMatch)
	Finish()
	Merge(Calculator)
}

type MetricCalculator interface {
	Calculator
	Value() float64
}

type DurationCalculator interface {
	Calculator
	Duration() time.Duration
}

type BucketCalculator interface {
	Calculator
	Buckets() []*Bucket
}

type Bucket struct {
	hash         uint64
	aggregations map[uint64]Calculator
}

func NewBucket(hash uint64, aggregations map[uint64]Aggregation) *Bucket {
	rv := &Bucket{
		hash:         hash,
		aggregations: make(map[uint64]Calculator),
	}
	for hash, agg := range aggregations {
		rv.aggregations[hash] = agg.Calculator()
	}
	return rv
}

func (b *Bucket) Merge(other *Bucket) {
	for otherAggName, otherCalculator := range other.aggregations {
		if thisCalculator, ok := b.aggregations[otherAggName]; ok {
			thisCalculator.Merge(otherCalculator)
		} else {
			b.aggregations[otherAggName] = otherCalculator
		}
	}
}

func (b *Bucket) Hash() uint64 {
	return b.hash
}

func (b *Bucket) Consume(d *DocumentMatch) {
	for _, aggCalc := range b.aggregations {
		aggCalc.Consume(d)
	}
}

func (b *Bucket) Finish() {
	for _, aggCalc := range b.aggregations {
		aggCalc.Finish()
	}
}

func (b *Bucket) Aggregations() map[uint64]Calculator {
	return b.aggregations
}

func (b *Bucket) Count() uint64 {
	if countAgg, ok := b.aggregations[CountHash]; ok {
		if countCalc, ok := countAgg.(MetricCalculator); ok {
			return uint64(countCalc.Value())
		}
	}
	return 0
}

func (b *Bucket) Duration() time.Duration {
	if durationAgg, ok := b.aggregations[DurationHash]; ok {
		if durationCalc, ok := durationAgg.(DurationCalculator); ok {
			return durationCalc.Duration()
		}
	}
	return 0
}

func (b *Bucket) Metric(hash uint64) float64 {
	if agg, ok := b.aggregations[hash]; ok {
		if calc, ok := agg.(MetricCalculator); ok {
			return calc.Value()
		}
	}
	return 0
}

func (b *Bucket) Buckets(hash uint64) []*Bucket {
	if agg, ok := b.aggregations[hash]; ok {
		if calc, ok := agg.(BucketCalculator); ok {
			return calc.Buckets()
		}
	}
	return nil
}

func (b *Bucket) Aggregation(hash uint64) Calculator {
	return b.aggregations[hash]
}
