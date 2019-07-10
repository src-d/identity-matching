// +build tools
// This file contains developer dependencies as suggested in
// https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module

package tools

import (
	_ "github.com/mjibson/esc"
	_ "golang.org/x/lint/golint"
	_ "golang.org/x/tools/cmd/goimports"
)
