package main

import (
	"fmt"
	"os"

	"github.com/tansive/tansive-internal/internal/cli"
)

func main() {
	cmd := cli.NewRootCmd()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
