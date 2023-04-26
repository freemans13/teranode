package main

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func main() {
	if err := fdb.APIVersion(720); err != nil {
		fmt.Printf("ERROR: %v", err)
	}

	version, err := fdb.GetAPIVersion()
	fmt.Printf("FoundationDB client library version: %d\nERROR: %v", version, err)

}
