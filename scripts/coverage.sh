#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root (parent of scripts directory)
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

# Change to project root to ensure consistent paths
cd "$PROJECT_ROOT"

TAGS="testtxmetacache,test_all"

SETTINGS_CONTEXT=test \
  go test -race -count=1 -coverprofile=coverage.out -tags "${TAGS}" \
  $(go list -tags "${TAGS}" ./... | grep -v 'github.com/bsv-blockchain/teranode/cmd')


# go install github.com/wadey/gocovmerge@latest
# gocovmerge 1.out 4.out 5.out 6.out 7.out > total.out

grep -v --no-filename -e "\.pb\.go" \
        -e "github\.com/bsv-blockchain/teranode/cmd/" \
        -e "github\.com/bsv-blockchain/teranode/test/" \
        -e "github\.com/bsv-blockchain/teranode/tracing/" coverage.out > filtered.out


go tool cover -func=filtered.out | tail -1
go tool cover -html=filtered.out
