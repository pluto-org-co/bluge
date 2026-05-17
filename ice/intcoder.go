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
	"encoding/binary"
	"fmt"
)

// We can safely use 0 to represent termNotEncoded since 0
// could never be a valid address for term location information.
// (stored field index is always non-empty and earlier in the
// file)
const termNotEncoded = 0

type chunkedIntCoder struct {
	final     []byte
	chunkSize uint64
	chunkBuf  bytes.Buffer
	chunkLens []uint64
	currChunk uint64

	buf []byte
}

// newChunkedIntCoder returns a new chunk int coder which packs data into
// chunks based on the provided chunkSize and supports up to the specified
// maxDocNum
func newChunkedIntCoder(chunkSize, maxDocNum uint64) *chunkedIntCoder {
	total := maxDocNum/chunkSize + 1
	rv := &chunkedIntCoder{
		chunkSize: chunkSize,
		chunkLens: make([]uint64, total),
		final:     make([]byte, 0, 64),
	}

	return rv
}

// Reset lets you reuse this chunked int coder.  buffers are reset and reused
// from previous use.  you cannot change the chunk size or max doc num.
func (c *chunkedIntCoder) Reset() {
	c.final = c.final[:0]
	c.chunkBuf.Reset()
	c.currChunk = 0
	for i := range c.chunkLens {
		c.chunkLens[i] = 0
	}
}

// SetChunkSize changes the chunk size.  It is only valid to do so
// with a new chunkedIntCoder, or immediately after calling Reset()
func (c *chunkedIntCoder) SetChunkSize(chunkSize, maxDocNum uint64) {
	total := int(maxDocNum/chunkSize + 1)
	c.chunkSize = chunkSize
	if cap(c.chunkLens) < total {
		c.chunkLens = make([]uint64, total)
	} else {
		c.chunkLens = c.chunkLens[:total]
	}
}

// Add encodes the provided integers into the correct chunk for the provided
// doc num.  You MUST call Add() with increasing docNums.
func (c *chunkedIntCoder) Add(docNum uint64, vals ...uint64) {
	chunk := docNum / c.chunkSize
	if chunk != c.currChunk {
		// starting a new chunk
		c.Close()
		c.chunkBuf.Reset()
		c.currChunk = chunk
	}

	if len(c.buf) < binary.MaxVarintLen64 {
		c.buf = make([]byte, binary.MaxVarintLen64)
	}

	for _, val := range vals {
		wb := binary.PutUvarint(c.buf, val)
		c.chunkBuf.Write(c.buf[:wb])
	}
}

// Close indicates you are done calling Add() this allows the final chunk
// to be encoded.
func (c *chunkedIntCoder) Close() {
	encodingBytes := c.chunkBuf.Bytes()
	c.chunkLens[c.currChunk] = uint64(len(encodingBytes))
	c.final = append(c.final, encodingBytes...)
	c.currChunk = uint64(cap(c.chunkLens)) // sentinel to detect double close
}

// WriteTo commits all the encoded chunked integers to the provided writer.
func (c *chunkedIntCoder) WriteToBuffer(w *bytes.Buffer) (n int64, err error) {
	var workingBuf = make([]byte, binary.MaxVarintLen64)

	bufNeeded := binary.MaxVarintLen64*(1+len(c.chunkLens)) + len(c.final)
	if w.Available() < bufNeeded {
		w.Grow(bufNeeded - w.Available())
	}

	// convert the chunk lengths into chunk offsets
	chunkOffsets := modifyLengthsToEndOffsets(c.chunkLens)

	// write out the number of chunks & each chunk offsets
	size := binary.PutUvarint(workingBuf, uint64(len(chunkOffsets)))
	written, _ := w.Write(workingBuf[:size])
	n += int64(written)
	for _, chunkOffset := range chunkOffsets {
		size := binary.PutUvarint(workingBuf, chunkOffset)
		written, _ := w.Write(workingBuf[:size])
		n += int64(written)
	}

	// write out the data
	written, _ = w.Write(c.final)
	n += int64(written)
	return n, nil
}

// WriteTo commits all the encoded chunked integers to the provided writer.
func (c *chunkedIntCoder) WriteToCountHashWriter(w *countHashWriter) (n int64, err error) {
	bufNeeded := binary.MaxVarintLen64 * (1 + len(c.chunkLens))
	if len(c.buf) < bufNeeded {
		c.buf = make([]byte, bufNeeded)
	}
	buf := c.buf

	// convert the chunk lengths into chunk offsets
	chunkOffsets := modifyLengthsToEndOffsets(c.chunkLens)

	// write out the number of chunks & each chunk offsets
	offset := binary.PutUvarint(buf, uint64(len(chunkOffsets)))
	n += int64(offset)
	for _, chunkOffset := range chunkOffsets {
		delta := binary.PutUvarint(buf[offset:], chunkOffset)
		n += int64(delta)
		offset += delta
	}

	delta, err := w.Write(buf[:n])
	n += int64(delta)
	if err != nil {
		return n, fmt.Errorf("failed to write offset chunk: %w", err)
	}

	// write out the data
	delta, err = w.Write(c.final)
	n += int64(delta)
	if err != nil {
		return n, fmt.Errorf("failed to write data: %w", err)
	}
	return n, nil
}

// writeAt commits all the encoded chunked integers to the provided writer
// and returns the starting offset, total bytes written and an error
func (c *chunkedIntCoder) writeAt(w *countHashWriter) (startOffset uint64, err error) {
	if len(c.final) == 0 {
		return termNotEncoded, nil
	}

	startOffset = uint64(w.Count())

	_, err = c.WriteToCountHashWriter(w)
	if err != nil {
		return startOffset, fmt.Errorf("failed to write to hash writer: %w", err)
	}
	return startOffset, nil
}

func (c *chunkedIntCoder) FinalSize() int {
	return len(c.final)
}

// modifyLengthsToEndOffsets converts the chunk length array
// to a chunk offset array. The readChunkBoundary
// will figure out the start and end of every chunk from
// these offsets. Starting offset of i'th index is stored
// in i-1'th position except for 0'th index and ending offset
// is stored at i'th index position.
// For 0'th element, starting position is always zero.
// eg:
// Lens ->  5 5 5 5 => 5 10 15 20
// Lens ->  0 5 0 5 => 0 5 5 10
// Lens ->  0 0 0 5 => 0 0 0 5
// Lens ->  5 0 0 0 => 5 5 5 5
// Lens ->  0 5 0 0 => 0 5 5 5
// Lens ->  0 0 5 0 => 0 0 5 5
func modifyLengthsToEndOffsets(lengths []uint64) []uint64 {
	var runningOffset uint64
	var index, i int
	for i = 1; i <= len(lengths); i++ {
		runningOffset += lengths[i-1]
		lengths[index] = runningOffset
		index++
	}
	return lengths
}

func readChunkBoundary(chunk int, offsets []uint64) (start, end uint64) {
	if chunk > 0 {
		start = offsets[chunk-1]
	}
	return start, offsets[chunk]
}
