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
	"io"
	"sync"

	"github.com/pluto-org-co/bluge/documents"
	"github.com/pluto-org-co/bluge/ice"
	"github.com/pluto-org-co/bluge/segment"
)

func (s *Writer) newSegment(results []*documents.Document) (*segmentWrapper, uint64, error) {
	seg, count, err := ice.New(results, s.config.NormCalc)
	return &segmentWrapper{
		Segment:    seg,
		refCounter: noOpRefCounter{},
	}, count, err
}

type segmentWrapper struct {
	segment.Segment
	refCounter
	persisted bool
}

func (s segmentWrapper) Persisted() bool {
	return s.persisted
}

func (s segmentWrapper) Close() error {
	return s.DecRef()
}

type refCounter interface {
	AddRef()
	DecRef() error
}

type noOpRefCounter struct{}

func (noOpRefCounter) AddRef()       {}
func (noOpRefCounter) DecRef() error { return nil }

type closeOnLastRefCounter struct {
	closer io.Closer
	m      sync.Mutex
	refs   int64
}

func (c *closeOnLastRefCounter) AddRef() {
	c.m.Lock()
	c.refs++
	c.m.Unlock()
}

func (c *closeOnLastRefCounter) DecRef() error {
	c.m.Lock()
	c.refs--
	var err error
	if c.refs == 0 && c.closer != nil {
		err = c.closer.Close()
	}
	c.m.Unlock()
	return err
}
