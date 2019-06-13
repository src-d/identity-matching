current_dir = $(shell pwd)

PROJECT = identity_matching
COMMANDS = cmd/match-identities

PKG_OS = darwin linux

DOCKERFILES = Dockerfile:$(PROJECT)
DOCKER_ORG = "srcd"

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_BRANCH ?= v1
CI_PATH ?= .ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH);
-include $(MAKEFILE)

check-style:
	golint -set_exit_status ./...
	[[ -z $$(gofmt -s -d .) ]] || exit 1  # Run `gofmt -s -w .` to fix style errors
	go vet

.PHONY: check-style