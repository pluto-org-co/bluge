package documents

import (
	"time"
	"unsafe"

	"github.com/pluto-org-co/bluge/numeric"
	"github.com/pluto-org-co/bluge/numeric/geo"
)

type Information struct {
	HasComposites bool
	Buffer        []byte
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

		bufferSize += len(def.Name) + len(def.Value)
	}

	var hasComposites bool
	var currentPosition int
	buffer := make([]byte, bufferSize)

	for index, def := range defs {
		// Store name
		delta := copy(buffer[currentPosition:], def.Name)
		nameString := unsafe.String(&buffer[currentPosition], len(def.Name))
		currentPosition += delta

		// Store value
		delta = copy(buffer[currentPosition:], def.Value)
		value := buffer[currentPosition : currentPosition+delta]
		currentPosition += delta

		switch def.Kind {
		case FieldDefinitionKindText, FieldDefinitionKindKeyword:
			fields[index].FieldOptions = defaultTextIndexingOptions
			fields[index].NameString = nameString
			fields[index].RawBytes = value
			fields[index].NumPlainTextBytesValue = len(value)
			fields[index].Analyzer = def.Analizer
			fields[index].PositionIncrementGapValue = 100
			fields[index].Kind = FieldKindTerm
		case FieldDefinitionKindNumeric:
			fields[index].FieldOptions = defaultNumericIndexingOptions
			fields[index].NameString = nameString
			fields[index].RawBytes = value
			fields[index].NumPlainTextBytesValue = 8
			fields[index].Analyzer = DefaultNumericAnalyzer
			fields[index].PositionIncrementGapValue = 100
			fields[index].Kind = FieldKindTerm
		case FieldDefinitionKindGeo:
			fields[index].FieldOptions = defaultNumericIndexingOptions
			fields[index].NameString = nameString
			fields[index].RawBytes = value
			fields[index].NumPlainTextBytesValue = 8
			fields[index].Analyzer = GeoAnalyzer
			fields[index].PositionIncrementGapValue = 100
			fields[index].Kind = FieldKindTerm
		case FieldDefinitionKindDate:
			fields[index].FieldOptions = defaultDateTimeIndexingOptions
			fields[index].NameString = nameString
			fields[index].RawBytes = value
			fields[index].NumPlainTextBytesValue = 8
			fields[index].Analyzer = DateAnalyzer
			fields[index].PositionIncrementGapValue = 100
			fields[index].Kind = FieldKindTerm
		default:
			// How we should handle it?
			// Is it composite?
		}

	}
	return &Information{Buffer: buffer, HasComposites: hasComposites}, fields
}

// Generates a slice of fields based on the definitions taking into consideration CPU functioning
// Less fragmentation by allocating everything near each other
func FieldsFromDefinitionsWithId[T ~string | ~[]byte](id T, defs ...*FieldDefinition) (info *Information, fields []*Field) {
	fieldValues := make([]Field, 1+len(defs))
	fields = make([]*Field, 1+len(defs))

	// Id is the last element
	fields[0] = &fieldValues[0]

	var bufferSize int = len(id)
	for index, def := range defs {
		fields[1+index] = &fieldValues[1+index]

		bufferSize += len(def.Name) + len(def.Value)
	}

	var currentPosition int
	buffer := make([]byte, bufferSize)

	// Prepare the id
	delta := copy(buffer[currentPosition:], id)
	fields[0].FieldOptions = defaultTextIndexingOptions | Sortable | Store
	fields[0].NameString = IdFieldName
	fields[0].RawBytes = buffer[currentPosition : currentPosition+delta]
	fields[0].NumPlainTextBytesValue = len(id)
	fields[0].PositionIncrementGapValue = 100
	fields[0].Kind = FieldKindTerm
	currentPosition += delta

	for index, def := range defs {
		// Store name
		delta := copy(buffer[currentPosition:], def.Name)
		nameString := unsafe.String(&buffer[currentPosition], len(def.Name))
		currentPosition += delta

		// Store value
		delta = copy(buffer[currentPosition:], def.Value)
		value := buffer[currentPosition : currentPosition+delta]
		currentPosition += delta

		switch def.Kind {
		case FieldDefinitionKindText, FieldDefinitionKindKeyword:
			fields[1+index].FieldOptions = defaultTextIndexingOptions
			fields[1+index].NameString = nameString
			fields[1+index].RawBytes = value
			fields[1+index].NumPlainTextBytesValue = len(value)
			fields[1+index].Analyzer = def.Analizer
			fields[1+index].PositionIncrementGapValue = 100
			fields[1+index].Kind = FieldKindTerm
		case FieldDefinitionKindNumeric:
			fields[1+index].FieldOptions = defaultNumericIndexingOptions
			fields[1+index].NameString = nameString
			fields[1+index].RawBytes = value
			fields[1+index].NumPlainTextBytesValue = 8
			fields[1+index].Analyzer = DefaultNumericAnalyzer
			fields[1+index].PositionIncrementGapValue = 100
			fields[1+index].Kind = FieldKindTerm
		case FieldDefinitionKindGeo:
			fields[1+index].FieldOptions = defaultNumericIndexingOptions
			fields[1+index].NameString = nameString
			fields[1+index].RawBytes = value
			fields[1+index].NumPlainTextBytesValue = 8
			fields[1+index].Analyzer = GeoAnalyzer
			fields[1+index].PositionIncrementGapValue = 100
			fields[1+index].Kind = FieldKindTerm
		case FieldDefinitionKindDate:
			fields[1+index].FieldOptions = defaultDateTimeIndexingOptions
			fields[1+index].NameString = nameString
			fields[1+index].RawBytes = value
			fields[1+index].NumPlainTextBytesValue = 8
			fields[1+index].Analyzer = DateAnalyzer
			fields[1+index].PositionIncrementGapValue = 100
			fields[1+index].Kind = FieldKindTerm
		}
	}
	return &Information{Buffer: buffer}, fields
}
