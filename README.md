# ChainFlow

**Distributed Blockchain Transaction Orchestration Engine**

ChainFlow is a fault-tolerant, event-driven Go orchestration system for accepting, scheduling, signing, broadcasting, retrying, and reconciling EVM blockchain transactions with durable PostgreSQL state and NATS JetStream messaging.

> **Current Phase:** Architecture Initiation (Steps 1-10). No application source code has been generated yet; all designs below are open for review before Milestone M0 implementation begins.

## Documentation Map

- **OpenSpec:** [openspec/specs/constitution.md](openspec/specs/constitution.md)
- **Architecture:**
  - [01 - Requirements & Scope](docs/architecture/01-requirements-and-scope.md)
  - [02 - System Architecture & Diagrams](docs/architecture/02-system-architecture.md)
  - [03 - Transaction Lifecycle & State Machine](docs/architecture/03-transaction-lifecycle.md)
  - [04 - Domain Model & PostgreSQL Schema](docs/architecture/04-domain-and-data-model.md)
  - [05 - Event Catalog (NATS JetStream)](docs/architecture/05-event-catalog.md)
  - [06 - Concurrency, Nonce Management & Failure Matrix](docs/architecture/06-concurrency-nonce-and-failures.md)
  - [07 - Implementation Roadmap](docs/architecture/07-roadmap.md)
- **Security:**
  - [STRIDE Threat Model & Signer Isolation](docs/security/threat-model.md)
- **Architecture Decision Records (ADRs):**
  - [ADR Index](docs/adr/README.md)
- **Specs, Runbooks & Dev Guides:**
  - [Feature Specs](docs/specs/README.md)
  - [Operational Runbooks](docs/runbooks/README.md)
  - [Development Guide](docs/development/README.md)
