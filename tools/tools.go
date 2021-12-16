//go:build tools

package main

import (
	_ "github.com/GeertJohan/fgt"
	_ "github.com/mattn/goveralls"
	_ "golang.org/x/lint"
	_ "golang.org/x/tools/cmd/cover"
)
