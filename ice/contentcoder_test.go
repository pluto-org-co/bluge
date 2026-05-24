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

package ice

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkedContentCoder(t *testing.T) {
	type Test struct {
		Name      string
		MaxDocNum uint64
		ChunkSize uint64
		DocNums   []uint64
		Values    [][]byte
		Expect    []byte
	}
	tests := []*Test{
		{
			Name:      "Single word 'bluge'",
			MaxDocNum: 0,
			ChunkSize: 1,
			DocNums:   []uint64{0},
			Values:    [][]byte{[]byte("bluge")},
			Expect: []byte{0x1, 0x0, 0x5, 0x62, 0x6c, 0x75, 0x67, 0x65,
				0x8,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			Name:      "Double worded",
			MaxDocNum: 1,
			ChunkSize: 1,
			DocNums:   []uint64{0, 1},
			Values: [][]byte{
				[]byte("upside"),
				[]byte("scorch"),
			},
			Expect: []byte{0x1, 0x0, 0x6, 0x75, 0x70, 0x73, 0x69, 0x64,
				0x65, 0x1, 0x1, 0x6, 0x73, 0x63, 0x6f, 0x72,
				0x63, 0x68, 0x9, 0x12,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			assertions := assert.New(t)

			var buffer bytes.Buffer
			cic := newChunkedContentCoder(test.ChunkSize, test.MaxDocNum, &buffer, false)
			for i, docNum := range test.DocNums {
				err := cic.Add(docNum, test.Values[i])
				if !assertions.Nil(err, "error adding to intcoder") {
					return
				}
			}

			cic.Close()
			_, err := cic.Write()
			if !assertions.Nil(err, "failed to write cic") {
				return
			}

			if !assertions.Equal(test.Expect, buffer.Bytes(), "expecting a different value") {
				return
			}
		})

	}
}

func TestChunkedContentCoders(t *testing.T) {
	maxDocNum := uint64(5)
	chunkSize := uint64(1)
	docNums := []uint64{0, 1, 2, 3, 4, 5}
	vals := [][]byte{
		[]byte("scorch"),
		[]byte("does"),
		[]byte("better"),
		[]byte("than"),
		[]byte("upside"),
		[]byte("down"),
	}

	var actual1, actual2 bytes.Buffer
	// chunkedContentCoder that writes out at the end
	cic1 := newChunkedContentCoder(chunkSize, maxDocNum, &actual1, false)
	// chunkedContentCoder that writes out in chunks
	cic2 := newChunkedContentCoder(chunkSize, maxDocNum, &actual2, true)

	for i, docNum := range docNums {
		err := cic1.Add(docNum, vals[i])
		if err != nil {
			t.Fatalf("error adding to intcoder: %v", err)
		}
		err = cic2.Add(docNum, vals[i])
		if err != nil {
			t.Fatalf("error adding to intcoder: %v", err)
		}
	}
	_ = cic1.Close()
	_ = cic2.Close()

	_, err := cic1.Write()
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	_, err = cic2.Write()
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	if !bytes.Equal(actual1.Bytes(), actual2.Bytes()) {
		t.Errorf("%s != %s", actual1.String(), actual2.String())
	}
}
