
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export GOTOOLCHAIN=local
export GOPROXY=off
export GOENV=off
export GOWORK=off
export GOFLAGS=

expected=$(cat toolchain.txt)
actual=$(go version)

if [[ "$actual" != "$expected" ]]; then
    printf 'check: Go version mismatch\nexpected: %s\nactual: %s\n' \
        "$expected" "$actual" >&2
    exit 1
fi

export GOCACHE
GOCACHE=$(mktemp -d)
trap 'rm -rf "$GOCACHE"' EXIT

mapfile -d '' gofiles < <(
    find . -type f -name '*.go' -not -path './vendor/*' -print0
)

if ((${#gofiles[@]})); then
    unformatted=$(gofmt -l "${gofiles[@]}")
    if [[ -n "$unformatted" ]]; then
        printf 'check: run gofmt for these files:\n%s\n' "$unformatted" >&2
        exit 1
    fi
fi

mkdir -p bin
go vet ./...
go test ./...
go build -o bin/au-guard ./cmd/au-guard

