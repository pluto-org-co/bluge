package testsuite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Creates a temporary directory that autocleans up
func TemporaryDirectory(tb testing.TB) (dir string) {
	assertions := assert.New(tb)

	dir, err := os.MkdirTemp(tb.TempDir(), "dir-*")
	if !assertions.Nil(err, "failed to create temporary directory") {
		return
	}
	tb.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}
