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

package bluge

import (
	"github.com/pluto-org-co/bluge/segment"
)

type Document struct {
	hasComposites bool
	info          *Information
	Fields        []*Field
}

func NewDocument(id string) *Document {
	return &Document{
		Fields: []*Field{NewKeywordField(IdFieldName, id).StoreValue().Sortable()},
	}
}

func NewDocumentWithFields(id string, info *Information, fields ...*Field) (doc *Document) {
	doc = &Document{
		info:   info,
		Fields: make([]*Field, 0, 1+len(fields)),
	}

	doc.Fields = append(doc.Fields, NewKeywordField(IdFieldName, id).StoreValue().Sortable())
	for _, field := range fields {
		if !doc.hasComposites && field.kind == FieldKindComposite {
			doc.hasComposites = true
		}
	}
	return doc
}

func (d Document) Size() int {
	sizeInBytes := sizeOfSlice

	for _, entry := range d.Fields {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// ID is an experimental helper method
// to simplify common use cases
func (d Document) ID() segment.Term {
	return Identifier(d.Fields[0].Value())
}

func (d *Document) AddField(f *Field) *Document {
	if !d.hasComposites && f.kind == FieldKindComposite {
		d.hasComposites = true
	}
	d.Fields = append(d.Fields, f)
	return d
}

func (d Document) Analyze() {
	fieldOffsets := map[string]int{}
	for _, field := range d.Fields {
		if !field.Index() {
			continue
		}
		fieldOffset := fieldOffsets[field.Name()]
		if fieldOffset > 0 {
			fieldOffset += field.PositionIncrementGap()
		}
		lastPos := field.Analyze(fieldOffset)
		fieldOffsets[field.Name()] = lastPos

		if d.hasComposites {
			// see if any of the composite fields need this
			for _, otherField := range d.Fields {
				if otherField.kind != FieldKindComposite || otherField == field {
					// never include yourself
					continue
				}
				otherField.Consume(field)
			}
		}
	}
}

func (d Document) EachField(vf segment.VisitField) {
	for _, field := range d.Fields {
		vf(field)
	}
}
