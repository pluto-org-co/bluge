package bluge

import (
	"time"
	"unsafe"

	"github.com/pluto-org-co/bluge/analysis"
	"github.com/pluto-org-co/bluge/numeric"
	"github.com/pluto-org-co/bluge/numeric/geo"
)

type Information struct {
	Buffer []byte
}

type FieldDefinitionKind uint8

const (
	FieldDefinitionKindText FieldDefinitionKind = iota
	FieldDefinitionKindKeyword
	FieldDefinitionKindNumeric
	FieldDefinitionKindGeo
	FieldDefinitionKindDate
)

type FieldDefinition struct {
	Name     string
	Value    []byte
	Analizer Analyzer
	Kind     FieldDefinitionKind
}

func (d *FieldDefinition) Size() (size int) {
	return len(d.Name) + len(d.Value)
}

func NewKeywordFieldDefinition[T ~string | ~[]byte](name string, value T) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: []byte(value), Analizer: nil, Kind: FieldDefinitionKindKeyword}
}

func NewTextFieldDefinition[T ~string | ~[]byte](name string, value T) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: []byte(value), Analizer: standardAnalyzer, Kind: FieldDefinitionKindText}
}

func NewKeywordFieldDefinitionWithAnalyzer[T ~string | ~[]byte](name string, value T, analyzer Analyzer) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: []byte(value), Analizer: analyzer, Kind: FieldDefinitionKindKeyword}
}

func NewTextFieldDefinitionWithAnalyzer[T ~string | ~[]byte](name string, value T, analyzer Analyzer) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: []byte(value), Analizer: analyzer, Kind: FieldDefinitionKindText}
}

func NewFloat64FieldDefinition(name string, number float64) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(number), 0), Kind: FieldDefinitionKindNumeric}
}

func NewInt64FieldDefinition(name string, number int64) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: numeric.MustNewPrefixCodedInt64(number, 0), Kind: FieldDefinitionKindNumeric}
}

func NewUint64FieldDefinition(name string, number uint64) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: numeric.MustNewPrefixCodedInt64(int64(number), 0), Kind: FieldDefinitionKindNumeric}
}

func NewGeoFieldDefinition(name string, lon, lat float64) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: numeric.MustNewPrefixCodedInt64(int64(geo.MortonHash(lon, lat)), 0), Analizer: nil, Kind: FieldDefinitionKindGeo}
}

func NewTimeFieldDefinition(name string, value *time.Time) (def *FieldDefinition) {
	return &FieldDefinition{Name: name, Value: numeric.MustNewPrefixCodedInt64(value.UnixNano(), 0), Analizer: nil, Kind: FieldDefinitionKindDate}
}

// Generates a slice of fields based on the definitions taking into consideration CPU functioning
// Less fragmentation by allocating everything near each other
func FieldsFromDefinitions(defs ...*FieldDefinition) (info *Information, fields []*Field) {
	fieldValues := make([]Field, len(defs))
	fields = make([]*Field, len(defs))

	var bufferSize int
	for index, def := range defs {
		fields[index] = &fieldValues[index]

		bufferSize += def.Size()
	}

	buffer := make([]byte, bufferSize)
	workingBuffer := buffer[:0]
	for index, def := range defs {
		// Store name
		workingBuffer = append(workingBuffer, def.Name...)
		nameString := unsafe.String(&workingBuffer[0], len(def.Name))
		workingBuffer = workingBuffer[len(def.Name):]

		// Store value
		workingBuffer = append(workingBuffer, def.Value...)
		value := workingBuffer[:len(def.Value)]
		workingBuffer = workingBuffer[len(def.Value):]

		switch def.Kind {
		case FieldDefinitionKindText, FieldDefinitionKindKeyword:
			newTextField(fields[index], nameString, value, def.Analizer)
		case FieldDefinitionKindNumeric:
			*fields[index] = Field{
				FieldOptions:      defaultNumericIndexingOptions,
				name:              nameString,
				value:             value,
				numPlainTextBytes: 8,
				analyzer: &numericAnalyzer{
					tokenType: analysis.Numeric,
					shiftBy:   defaultNumericPrecisionStep,
				},
				positionIncrementGap: 100,
				kind:                 FieldKindTerm,
			}
		case FieldDefinitionKindGeo:
			*fields[index] = Field{
				FieldOptions:      defaultNumericIndexingOptions,
				name:              nameString,
				value:             value,
				numPlainTextBytes: 8,
				analyzer: &numericAnalyzer{
					tokenType: analysis.Numeric,
					shiftBy:   geoPrecisionStep,
				},
				positionIncrementGap: 100,
				kind:                 FieldKindTerm,
			}
		case FieldDefinitionKindDate:
			*fields[index] = Field{
				FieldOptions:      defaultDateTimeIndexingOptions,
				name:              nameString,
				value:             value,
				numPlainTextBytes: 8,
				analyzer: &numericAnalyzer{
					tokenType: analysis.DateTime,
					shiftBy:   defaultDateTimePrecisionStep,
				},
				positionIncrementGap: 100,
				kind:                 FieldKindTerm,
			}
		}

	}
	return &Information{Buffer: buffer}, fields
}
