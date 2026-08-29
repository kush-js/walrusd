// Command lib builds the WALrus C shared library:
//
//	go build -buildmode=c-shared -tags vfs -o libwalrus.dylib ./bindings/c/lib
package main

import (
	_ "walrus/bindings/c"
)

func main() {}
