# Development Guide

## Prerequisites (Target Toolchain)
- Go `1.27+` (verified installed: `go1.27.3 windows/amd64`)
- Git (`2.56+` verified installed)
- OpenSpec CLI (`1.14.1` verified installed)
- Docker & Docker Compose (for local PostgreSQL, NATS JetStream, and observability stack)
- Foundry (`forge`, `anvil`, `cast`) for Solidity test contracts and local reorg simulation (complemented by `go-ethereum/ethclient/simulated` for pure-Go tests)

## Workflow Rules
1. Follow TDD (Red -> Green -> Refactor) for every milestone in [07-roadmap.md](../architecture/07-roadmap.md).
2. Run `go vet ./...` and `go test -race ./...` before committing.
3. Never commit `.env` files, private keys, or RPC API keys.
