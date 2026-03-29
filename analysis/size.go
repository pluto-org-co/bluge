package analysis

import (
	"unsafe"
)

const sizeOfMap = unsafe.Sizeof(map[int]int{})
const sizeOfPtr = unsafe.Sizeof((*int)(nil))
const sizeOfString = unsafe.Sizeof("")
