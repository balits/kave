package testutil

import "os"

func Tempdir(prefix string) (dir string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", prefix+"-temp-*")
	if err == nil {
		cleanup = func() {
			os.RemoveAll(dir)
		}
	}
	return
}
