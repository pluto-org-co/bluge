package analysis

import (
	"unsafe"
)

var sizeOfMap = unsafe.Sizeof(map[int]int{})
var sizeOfPtr = unsafe.Sizeof((*int)(nil))
var sizeOfString = unsafe.Sizeof("")
