//go:build !test_all

package main

import (
	"fmt"

	"github.com/bsv-blockchain/teranode/test/utils/tconfig"
)

// How to run
//
//	SUITE_NAME="OverrideEnv" LOCALSYSTEM_COMPOSES="OverrideEnv1 OverrideEnv2" go run test/utils/cmd/tconfig/main.go
//	SUITE_TESTID="OverrideEnv" TERANODE_CONTEXTS="OverrideEnv1 OverrideEnv2" go run test/utils/cmd/tconfig/main.go --tconfig-file=./test/utils/cmd/tconfig/testabc.env
func main() {
	tconfig := tconfig.LoadTConfig(
		map[string]any{
			tconfig.KeyTeranodeContexts: []string{"Hardcoded1", "Hardcoded2"},
		},
	)

	// Marshal the struct to YAML
	tconfigYAML := tconfig.StringYAML()

	// Convert the byte array to string and print
	fmt.Printf("\n###  Config for testing inputs  ###\n\n%v", tconfigYAML)
}
