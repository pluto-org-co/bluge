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

package analysis

import (
	"bufio"
	"bytes"
	"os"

	"github.com/zeebo/xxh3"
)

type TokenMap map[uint64]struct{}

func NewTokenMap() TokenMap {
	return make(TokenMap)
}

// LoadFile reads in a list of tokens from a text file,
// one per line.
// Comments are supported using `#` or `|`
func (t TokenMap) LoadFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	t.LoadBytes(data)
	return nil
}

// LoadBytes reads in a list of tokens from memory,
// one per line.
// Comments are supported using `#` or `|`
func (t TokenMap) LoadBytes(data []byte) {
	bytesReader := bytes.NewReader(data)

	scanner := bufio.NewScanner(bytesReader)
	for scanner.Scan() {
		line := scanner.Bytes()

		t.LoadLine(line)
	}

	err := scanner.Err()
	// if the err was EOF we still need to process the last value
	if err != nil {
		panic(err)
	}
}

func (t TokenMap) LoadLine(line []byte) {
	// find the start of a comment, if any
	startComment := bytes.IndexAny(line, "#|")
	if startComment >= 0 {
		line = line[:startComment]
	}

	for token := range bytes.FieldsSeq(line) {
		t.Add(token)
	}
}

func (t TokenMap) Has(token []byte) (has bool) {
	_, has = t[xxh3.Hash(token)]
	return has
}

func (t TokenMap) HasRunes(token []rune) (has bool) {
	asBytes := []byte(string(token))

	_, has = t[xxh3.Hash(asBytes)]
	return has
}

func (t TokenMap) Add(token []byte) {
	t[xxh3.Hash(token)] = struct{}{}
}

func (t TokenMap) AddString(token string) {
	t[xxh3.HashString(token)] = struct{}{}
}
