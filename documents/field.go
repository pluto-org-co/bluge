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

package documents

import (
	"time"

	"github.com/pluto-org-co/bluge/analysis/analyzer"

	"github.com/pluto-org-co/bluge/analysis"
	"github.com/pluto-org-co/bluge/numeric"
	"github.com/pluto-org-co/bluge/numeric/geo"
)

type FieldOptions int64

const (
	Index FieldOptions = 1 << iota
	Store
	SearchTermPositions
	HighlightMatches
	Sortable
	Aggregatable
)

func (o FieldOptions) Index() bool {
	return o&Index != 0
}

func (o FieldOptions) Store() bool {
	return o&Store != 0
}

func (o FieldOptions) IncludeLocations() bool {
	return o&SearchTermPositions != 0 || o&HighlightMatches != 0
}

func (o FieldOptions) IndexDocValues() bool {
	return o&Sortable != 0 || o&Aggregatable != 0
}

type FieldKind uint8

const (
	FieldKindTerm FieldKind = iota
	FieldKindComposite
)

type Field struct {
	FieldOptions
	NameString                string
	RawBytes                  []byte
	NumPlainTextBytesValue    int
	AnalyzedLengthValue       int
	AnalyzedTokenFreqs        analysis.TokenFrequencies
	Analyzer                  Analyzer
	PositionIncrementGapValue int
	// Composite fields
	IncludeFields  map[string]struct{}
	ExcludeFields  map[string]struct{}
	DefaultInclude bool
	Kind           FieldKind
}

func (b *Field) PositionIncrementGap() int {
	if b.Kind == FieldKindComposite {
		return 0
	}
	return b.PositionIncrementGapValue
}

func (b *Field) SetPositionIncrementGap(positionIncrementGap int) *Field {
	b.PositionIncrementGapValue = positionIncrementGap
	return b
}

func (b *Field) Name() string {
	return b.NameString
}

func (b *Field) AnalyzedLength() int {
	return b.AnalyzedLengthValue
}

func (b *Field) AnalyzedTokenFrequencies() analysis.TokenFrequencies {
	return b.AnalyzedTokenFreqs
}

func (b *Field) Value() []byte {
	return b.RawBytes
}

func (b *Field) NumPlainTextBytes() int {
	return b.NumPlainTextBytesValue
}

func (b *Field) StoreValue() *Field {
	b.FieldOptions |= Store
	return b
}

func (b *Field) Sortable() *Field {
	b.FieldOptions |= Sortable
	return b
}

func (b *Field) Aggregatable() *Field {
	b.FieldOptions |= Aggregatable
	return b
}

func (b *Field) SearchTermPositions() *Field {
	b.FieldOptions |= SearchTermPositions
	return b
}

func (b *Field) HighlightMatches() *Field {
	b.FieldOptions |= HighlightMatches
	return b
}

func (b *Field) Length() int {
	return b.AnalyzedLengthValue
}

func (b *Field) baseAnalayze(typ analysis.TokenType) analysis.TokenStream {
	var tokens analysis.TokenStream
	tokens = append(tokens, &analysis.Token{
		Start:        0,
		End:          len(b.RawBytes),
		Term:         b.RawBytes,
		PositionIncr: 1,
		Type:         typ,
	})
	return tokens
}

func (b *Field) WithAnalyzer(fieldAnalyzer Analyzer) *Field {
	b.Analyzer = fieldAnalyzer
	return b
}

func (b *Field) Analyze(startOffset int) (lastPos int) {
	switch b.Kind {
	case FieldKindComposite:
		return 0
	default:
		var tokens analysis.TokenStream
		if b.Analyzer != nil {
			bytesToAnalyze := b.Value()
			if b.Store() {
				// need to copy
				bytesCopied := make([]byte, len(bytesToAnalyze))
				copy(bytesCopied, bytesToAnalyze)
				bytesToAnalyze = bytesCopied
			}
			tokens = b.Analyzer.Analyze(bytesToAnalyze)
		} else {
			tokens = b.baseAnalayze(analysis.AlphaNumeric)
		}
		b.AnalyzedLengthValue = len(tokens) // number of tokens in this doc field
		b.AnalyzedTokenFreqs, lastPos = analysis.TokenFrequency(tokens, b.IncludeLocations(), startOffset)
		return lastPos
	}

}

const defaultTextIndexingOptions = Index

type Analyzer interface {
	Analyze(input []byte) analysis.TokenStream
}

var standardAnalyzer = analyzer.NewStandardAnalyzer()

func NewKeywordField(name, value string) (field *Field) {
	field = new(Field)
	newTextField(field, name, []byte(value), nil, 0)
	return field
}

func NewKeywordFieldBytes(name string, value []byte) (field *Field) {
	field = new(Field)
	newTextField(field, name, value, nil, 0)
	return field
}

func NewTextField(name, value string) (field *Field) {
	field = new(Field)
	newTextField(field, name, []byte(value), standardAnalyzer, 0)
	return field
}

func NewTextFieldBytes(name string, value []byte) (field *Field) {
	field = new(Field)
	newTextField(field, name, value, standardAnalyzer, 0)
	return field
}

func newTextField(dst *Field, name string, value []byte, fieldAnalyzer Analyzer, options FieldOptions) {
	dst.FieldOptions = defaultTextIndexingOptions | options
	dst.NameString = name
	dst.RawBytes = value
	dst.NumPlainTextBytesValue = len(value)
	dst.Analyzer = fieldAnalyzer
	dst.PositionIncrementGapValue = 100
	dst.Kind = FieldKindTerm
}

const defaultNumericIndexingOptions = Index | Sortable | Aggregatable

const defaultNumericPrecisionStep uint = 4

func addShiftTokens(tokens analysis.TokenStream, original int64, shiftBy uint, typ analysis.TokenType) analysis.TokenStream {
	shift := shiftBy
	for shift < 64 {
		shiftEncoded, err := numeric.NewPrefixCodedInt64(original, shift)
		if err != nil {
			break
		}
		token := analysis.Token{
			Start:        0,
			End:          len(shiftEncoded),
			Term:         shiftEncoded,
			PositionIncr: 0,
			Type:         typ,
		}
		tokens = append(tokens, &token)
		shift += shiftBy
	}
	return tokens
}

type numericAnalyzer struct {
	tokenType analysis.TokenType
	shiftBy   uint
}

var (
	DefaultNumericAnalyzer = &numericAnalyzer{
		tokenType: analysis.Numeric,
		shiftBy:   defaultNumericPrecisionStep,
	}
	GeoAnalyzer = &numericAnalyzer{
		tokenType: analysis.Numeric,
		shiftBy:   GeoPrecisionStep,
	}
	DateAnalyzer = &numericAnalyzer{
		tokenType: analysis.DateTime,
		shiftBy:   defaultDateTimePrecisionStep,
	}
)

func (n *numericAnalyzer) Analyze(input []byte) analysis.TokenStream {
	tokens := analysis.TokenStream{
		&analysis.Token{
			Start:        0,
			End:          len(input),
			Term:         input,
			PositionIncr: 1,
			Type:         n.tokenType,
		},
	}
	original, err := numeric.PrefixCoded(input).Int64()
	if err == nil {
		tokens = addShiftTokens(tokens, original, n.shiftBy, n.tokenType)
	}
	return tokens
}

func NewNumericField(name string, number float64) (field *Field) {
	field = new(Field)
	newNumericFieldWithIndexingOptions(field, name, numeric.Float64ToInt64(number))
	return field
}

func newNumericFieldWithIndexingOptions(dst *Field, name string, number int64) {
	*dst = Field{
		FieldOptions:           defaultNumericIndexingOptions,
		NameString:             name,
		RawBytes:               numeric.MustNewPrefixCodedInt64(number, 0),
		NumPlainTextBytesValue: 8,
		Analyzer: &numericAnalyzer{
			tokenType: analysis.Numeric,
			shiftBy:   defaultNumericPrecisionStep,
		},
		PositionIncrementGapValue: 100,
		Kind:                      FieldKindTerm,
	}
}

func DecodeNumericFloat64(value []byte) (float64, error) {
	i64, err := numeric.PrefixCoded(value).Int64()
	if err != nil {
		return 0, err
	}
	return numeric.Int64ToFloat64(i64), nil
}

const defaultDateTimeIndexingOptions = Index | Sortable | Aggregatable

const defaultDateTimePrecisionStep uint = 4

func NewDateTimeField(name string, dt time.Time) *Field {
	dtInt64 := dt.UnixNano()
	prefixCoded := numeric.MustNewPrefixCodedInt64(dtInt64, 0)
	return &Field{
		FieldOptions:           defaultDateTimeIndexingOptions,
		NameString:             name,
		RawBytes:               prefixCoded,
		NumPlainTextBytesValue: 8,
		Analyzer: &numericAnalyzer{
			tokenType: analysis.DateTime,
			shiftBy:   defaultDateTimePrecisionStep,
		},
		PositionIncrementGapValue: 100,
		Kind:                      FieldKindTerm,
	}
}

func DecodeDateTime(value []byte) (time.Time, error) {
	i64, err := numeric.PrefixCoded(value).Int64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, i64).UTC(), nil
}

var GeoPrecisionStep uint = 9

func NewGeoPointField(name string, lon, lat float64) *Field {
	mHash := geo.MortonHash(lon, lat)
	prefixCoded := numeric.MustNewPrefixCodedInt64(int64(mHash), 0)
	return &Field{
		FieldOptions:           defaultNumericIndexingOptions,
		NameString:             name,
		RawBytes:               prefixCoded,
		NumPlainTextBytesValue: 8,
		Analyzer: &numericAnalyzer{
			tokenType: analysis.Numeric,
			shiftBy:   GeoPrecisionStep,
		},
		PositionIncrementGapValue: 100,
		Kind:                      FieldKindTerm,
	}
}

func DecodeGeoLonLat(value []byte) (lon, lat float64, err error) {
	i64, err := numeric.PrefixCoded(value).Int64()
	if err != nil {
		return 0, 0, err
	}
	return geo.MortonUnhashLon(uint64(i64)), geo.MortonUnhashLat(uint64(i64)), nil
}

const defaultCompositeIndexingOptions = Index

func NewCompositeFieldIncluding(name string, including []string) *Field {
	return newCompositeFieldWithIndexingOptions(name, false, including,
		nil, defaultCompositeIndexingOptions)
}

func NewCompositeFieldExcluding(name string, excluding []string) *Field {
	return newCompositeFieldWithIndexingOptions(name, true, nil,
		excluding, defaultCompositeIndexingOptions)
}

func NewCompositeField(name string, defaultInclude bool, include, exclude []string) *Field {
	return newCompositeFieldWithIndexingOptions(name, defaultInclude, include, exclude, defaultCompositeIndexingOptions)
}

func newCompositeFieldWithIndexingOptions(name string, defaultInclude bool, include, exclude []string, options FieldOptions) *Field {
	rv := &Field{
		FieldOptions:       options,
		NameString:         name,
		AnalyzedTokenFreqs: make(analysis.TokenFrequencies),
		DefaultInclude:     defaultInclude,
		IncludeFields:      make(map[string]struct{}, len(include)),
		ExcludeFields:      make(map[string]struct{}, len(exclude)),
		Kind:               FieldKindComposite,
	}

	for _, i := range include {
		rv.IncludeFields[i] = struct{}{}
	}
	for _, e := range exclude {
		rv.ExcludeFields[e] = struct{}{}
	}

	return rv
}

func (c *Field) includesField(field string) bool {
	shouldInclude := c.DefaultInclude
	_, fieldShouldBeIncluded := c.IncludeFields[field]
	if fieldShouldBeIncluded {
		shouldInclude = true
	}
	_, fieldShouldBeExcluded := c.ExcludeFields[field]
	if fieldShouldBeExcluded {
		shouldInclude = false
	}
	return shouldInclude
}

func (c *Field) Consume(field *Field) {
	if c.Kind != FieldKindComposite {
		return
	}
	if c.includesField(field.Name()) {
		c.AnalyzedLengthValue += field.Length()
		c.AnalyzedTokenFreqs.MergeAll(field.Name(), field.AnalyzedTokenFrequencies())
	}
}

func NewStoredOnlyField(name string, value []byte) *Field {
	return &Field{
		Kind:                   FieldKindTerm,
		FieldOptions:           Store,
		NameString:             name,
		RawBytes:               value,
		NumPlainTextBytesValue: len(value),
	}
}
