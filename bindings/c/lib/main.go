// Command lib builds the walrusd C shared library:
//
//	go build -buildmode=c-shared -tags vfs -o libwalrusd.dylib ./bindings/c/lib
package main

import (
	_ "walrusd/bindings/c"
)

func main() {}
