# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Teranode is a horizontally scalable BSV Blockchain node implementation using a microservices architecture. It achieves over 1 million transactions per second through distributed processing across multiple machines.

## Common Development Commands

```bash
# Build
make build                  # Main binary with dashboard
make build-teranode         # Without dashboard
make build-teranode-ci      # CI build with race detection
make build-teranode-cli     # CLI tool

# Test
make test                   # All tests except integration
make longtest               # Long-running tests
make sequentialtest         # Sequential tests
make smoketest              # Smoke tests
make testall                # All tests
go test -v -race -tags "testtxmetacache" -run TestNameHere ./path/to/package

# Lint
make lint                   # Changed files vs main branch
make lint-new               # Unstaged/untracked changes only
make lint-full              # All files
gci write --skip-generated -s standard -s default <filename>

# Dev
make dev                    # Teranode + dashboard
make dev-teranode           # Teranode only
make dev-dashboard          # Dashboard only
```

## Architecture

Teranode consists of specialized services communicating via gRPC and Kafka:

| Service | Path | Role |
|---------|------|------|
| Asset Server | `services/asset/` | HTTP/WebSocket interface to data stores |
| Propagation | `services/propagation/` | Receives and forwards transactions |
| Validator | `services/validator/` | Validates transactions against consensus rules |
| Block Validation | `services/blockvalidation/` | Validates complete blocks |
| Block Assembly | `services/blockassembly/` | Assembles new blocks from validated txs |
| Blockchain | `services/blockchain/` | Manages blockchain state and FSM |
| Subtree Validation | `services/subtreevalidation/` | Validates merkle subtrees |
| P2P | `services/p2p/` | Peer-to-peer network communication |
| RPC | `services/rpc/` | Bitcoin-compatible JSON-RPC interface |
| Legacy | `services/legacy/` | Backward compat with existing Bitcoin nodes |

**Data Stores:**
- **UTXO Store** (`stores/utxo/`): Unspent transaction outputs (Aerospike)
- **Blob Store** (`stores/blob/`): Transactions and subtrees (S3/filesystem)
- **Blockchain Store** (`stores/blockchain/`): Block headers and chain state (PostgreSQL)

## Configuration

- `settings.conf`: Default settings and environment-specific overrides
- `settings_local.conf`: Local development overrides (not committed)
- Environment contexts: `dev`, `test`, `docker`, `operator`

## Coding Conventions

**All code must follow [`docs/references/codingConventions.md`](docs/references/codingConventions.md).**

Gotchas not covered there:
- Don't use mock blockchain client/store — use a real one with the `sqlitememory` store
- Don't use mock Kafka — use `in_memory_kafka.go`
- **Log messages must always be on a single line** — never use multi-line log statements
- Protobuf files generate Go code via `make gen`
- Dashboard is a Svelte application in `ui/dashboard/`

## Testing

### Categories
1. **Unit Tests**: Package-level tests with mocks
2. **Integration Tests**: Multi-service interaction tests (use TestContainers)
3. **Consensus Tests** (`test/consensus/`): Bitcoin script validation
4. **E2E Tests** (`test/e2e/`): Full system tests with containers
5. **Sequential Tests**: Order-dependent test scenarios

### Build Tags
- `testtxmetacache`: Small cache for testing
- `largetxmetacache`: Production cache size
- `aerospike`: Tests requiring Aerospike

## Git Workflow (Fork Mode)

All developers work in forked repositories with `upstream` remote pointing to the original repo.

### Pushing Work
```bash
# Always sync with upstream first
git pull upstream main

# If conflicts occur: STOP and ask user for resolution guidance
# After resolving (or if no conflicts):
git push origin <current-branch>
```

**Important**: Never auto-resolve merge conflicts. Always show conflicting files and wait for user approval on resolution strategy.

### Creating New Branches
```bash
git checkout main
git pull upstream main
git checkout -b <new-branch-name>
```

### Quick Reference
- **Push work**: Sync upstream → resolve conflicts (with user approval) → push to fork
- **New branch**: Switch to main → sync upstream → create branch
- **Sync with upstream**: `git checkout main && git pull upstream main`
