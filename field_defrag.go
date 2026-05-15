package bluge

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
			fields[index].name = nameString
			fields[index].value = value
			fields[index].numPlainTextBytes = len(value)
			fields[index].analyzer = def.Analizer
			fields[index].positionIncrementGap = 100
			fields[index].kind = FieldKindTerm
		case FieldDefinitionKindNumeric:
			fields[index].FieldOptions = defaultNumericIndexingOptions
			fields[index].name = nameString
			fields[index].value = value
			fields[index].numPlainTextBytes = 8
			fields[index].analyzer = DefaultNumericAnalyzer
			fields[index].positionIncrementGap = 100
			fields[index].kind = FieldKindTerm
		case FieldDefinitionKindGeo:
			fields[index].FieldOptions = defaultNumericIndexingOptions
			fields[index].name = nameString
			fields[index].value = value
			fields[index].numPlainTextBytes = 8
			fields[index].analyzer = GeoAnalyzer
			fields[index].positionIncrementGap = 100
			fields[index].kind = FieldKindTerm
		case FieldDefinitionKindDate:
			fields[index].FieldOptions = defaultDateTimeIndexingOptions
			fields[index].name = nameString
			fields[index].value = value
			fields[index].numPlainTextBytes = 8
			fields[index].analyzer = DateAnalyzer
			fields[index].positionIncrementGap = 100
			fields[index].kind = FieldKindTerm
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
	fields[0].name = IdFieldName
	fields[0].value = buffer[currentPosition : currentPosition+delta]
	fields[0].numPlainTextBytes = len(id)
	fields[0].positionIncrementGap = 100
	fields[0].kind = FieldKindTerm
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
			fields[1+index].name = nameString
			fields[1+index].value = value
			fields[1+index].numPlainTextBytes = len(value)
			fields[1+index].analyzer = def.Analizer
			fields[1+index].positionIncrementGap = 100
			fields[1+index].kind = FieldKindTerm
		case FieldDefinitionKindNumeric:
			fields[1+index].FieldOptions = defaultNumericIndexingOptions
			fields[1+index].name = nameString
			fields[1+index].value = value
			fields[1+index].numPlainTextBytes = 8
			fields[1+index].analyzer = DefaultNumericAnalyzer
			fields[1+index].positionIncrementGap = 100
			fields[1+index].kind = FieldKindTerm
		case FieldDefinitionKindGeo:
			fields[1+index].FieldOptions = defaultNumericIndexingOptions
			fields[1+index].name = nameString
			fields[1+index].value = value
			fields[1+index].numPlainTextBytes = 8
			fields[1+index].analyzer = GeoAnalyzer
			fields[1+index].positionIncrementGap = 100
			fields[1+index].kind = FieldKindTerm
		case FieldDefinitionKindDate:
			fields[1+index].FieldOptions = defaultDateTimeIndexingOptions
			fields[1+index].name = nameString
			fields[1+index].value = value
			fields[1+index].numPlainTextBytes = 8
			fields[1+index].analyzer = DateAnalyzer
			fields[1+index].positionIncrementGap = 100
			fields[1+index].kind = FieldKindTerm
		}
	}
	return &Information{Buffer: buffer}, fields
}
