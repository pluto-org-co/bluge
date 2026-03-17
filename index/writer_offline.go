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

package index

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"

	segment "github.com/blugelabs/bluge_segment_api"
)

type WriterOffline struct {
	m         sync.Mutex
	config    Config
	directory Directory
	segPlugin *SegmentPlugin
	segCount  atomic.Uint64
	segIDs    []uint64

	mergeMax int
}

func OpenOfflineWriter(config Config) (writer *WriterOffline, err error) {
	writer = &WriterOffline{
		config:    config,
		directory: config.DirectoryFunc(),
		segPlugin: nil,
		mergeMax:  10,
	}

	err = writer.directory.Setup(false)
	if err != nil {
		return nil, fmt.Errorf("error setting up directory: %w", err)
	}

	writer.segPlugin, err = loadSegmentPlugin(config.supportedSegmentPlugins, config.SegmentType, config.SegmentVersion)
	if err != nil {
		return nil, fmt.Errorf("error loading segment plugin: %v", err)
	}

	return writer, nil
}

func (s *WriterOffline) Batch(batch *Batch) (err error) {
	if len(batch.documents) == 0 {
		return nil
	}

	for _, doc := range batch.documents {
		if doc != nil {
			doc.Analyze()
		}
	}

	newSegment, _, err := s.segPlugin.New(batch.documents, s.config.NormCalc)
	if err != nil {
		return err
	}

	newId := s.segCount.Add(1)
	// There is zero chance of collision we can safely use the computed id
	err = s.directory.Persist(ItemKindSegment, newId, newSegment, nil)
	if err != nil {
		return fmt.Errorf("error persisting segment: %v", err)
	}

	s.m.Lock()
	s.segIDs = append(s.segIDs, newId)
	s.m.Unlock()

	return nil
}

func (s *WriterOffline) doMerge() error {
	for len(s.segIDs) > 1 {
		var newIdsMutex sync.Mutex
		var newIds = make([]uint64, 0, len(s.segIDs))

		// Do not spawn infinite goroutines even the OS has a limit.
		// Attempt to use all available CPU but if there are too many
		// Just use 4, even SSD has a limit
		var workersCount = min(4, runtime.NumCPU())
		var workersCh = make(chan struct{}, workersCount)
		for range workersCount {
			workersCh <- struct{}{}
		}

		var wg sync.WaitGroup
		var errorsCh = make(chan error, max(1, len(s.segIDs)/s.mergeMax))
		for chunk := range slices.Chunk(s.segIDs, s.mergeMax) {
			<-workersCh
			wg.Go(func() {
				defer func() { workersCh <- struct{}{} }()
				// Cleanup code once merging is completed
				var closers = make([]io.Closer, 0, len(chunk))
				defer func() {
					for index, closer := range closers {
						err := closer.Close()
						if err != nil {
							errorsCh <- fmt.Errorf("failed to close closer at index: %d: %w", index, err)
						}
					}
				}()

				var mergeSegments = make([]segment.Segment, 0, len(chunk))
				for _, mergeID := range chunk {
					data, closer, err := s.directory.Load(ItemKindSegment, mergeID)
					if err != nil {
						errorsCh <- fmt.Errorf("error loading segment from directory: id: %d: %w", mergeID, err)
						return
					}

					if closer == nil { // Is there actually a chance of closer being nil?
						continue
					}

					closers = append(closers, closer)

					seg, err := s.segPlugin.Load(data)
					if err != nil {
						errorsCh <- fmt.Errorf("error loading segment: id: %d: %w", mergeID, err)
						return
					}

					mergeSegments = append(mergeSegments, seg)
				}

				// do the merge
				drops := make([]*roaring.Bitmap, len(chunk))
				merger := s.segPlugin.Merge(mergeSegments, drops, s.config.MergeBufferSize)

				newId := s.segCount.Add(1)
				err := s.directory.Persist(ItemKindSegment, newId, merger, nil)
				if err != nil {
					errorsCh <- fmt.Errorf("error merging segments (%v): %w", chunk, err)
					return
				}

				newIdsMutex.Lock()
				newIds = append(newIds, newId)
				newIdsMutex.Unlock()

				// remove merged segments
				for _, mergeID := range chunk {
					err = s.directory.Remove(ItemKindSegment, mergeID)
					if err != nil {
						errorsCh <- fmt.Errorf("error removing segment %v after merge: %w", chunk, err)
						return
					}
				}
			})
		}
		go func() { wg.Wait(); close(errorsCh) }()

		// Wait for errors once there are no workers channel will automatically closed
		var mergingErrs = make([]error, 0, max(1, len(s.segIDs)/s.mergeMax))
		for err := range errorsCh {
			mergingErrs = append(mergingErrs, err)
		}

		switch len(mergingErrs) {
		case 0:
			s.segIDs = newIds
		case 1:
			return fmt.Errorf("an error ocurred during merge: %w", mergingErrs[0])
		default:
			return fmt.Errorf("multiple errors during merge: %w", errors.Join(mergingErrs...))
		}

	}

	return nil
}

func (s *WriterOffline) Close() error {
	s.m.Lock()
	defer s.m.Unlock()

	// perform all the merging into one segment
	err := s.doMerge()
	if err != nil {
		return fmt.Errorf("error while merging: %w", err)
	}

	// open the merged segment
	data, closer, err := s.directory.Load(ItemKindSegment, s.segIDs[0])
	if err != nil {
		return fmt.Errorf("error loading segment from directory: %w", err)
	}
	finalSeg, err := s.segPlugin.Load(data)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return fmt.Errorf("error loading segment: %w", err)
	}

	// fake snapshot referencing this segment
	snapshot := &Snapshot{
		segment: []*segmentSnapshot{
			{
				id: s.segIDs[0],
				segment: &segmentWrapper{
					Segment:    finalSeg,
					refCounter: nil,
					persisted:  true,
				},
				segmentType:    s.segPlugin.Type,
				segmentVersion: s.segPlugin.Version,
			},
		},
		epoch: s.segIDs[0],
	}

	// persist the snapshot
	err = s.directory.Persist(ItemKindSnapshot, s.segIDs[0], snapshot, nil)
	if err != nil {
		return fmt.Errorf("error recording snapshot: %w", err)
	}

	if closer != nil {
		return closer.Close()
	}
	return nil
}
