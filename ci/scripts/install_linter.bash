#!/usr/bin/env bash

LINTER=$(which golangci-lint)
LINTER_VERSION=1.16.0

set -eo pipefail

if [[ "$LINTER" == "" ]] ; then
    curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b $(go env GOPATH)/bin v${LINTER_VERSION}
    golangci-lint --version
fi
