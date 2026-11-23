# ADR-0009: Docker Compose Local Validation Before Kubernetes

## Status
Accepted (2026-11-21)

## Context
ChainFlow targets eventual Kubernetes/Helm deployment (HPA on queue depth, rolling updates, graceful `SIGTERM` termination), but introducing Kubernetes manifests, Helm charts, and cluster ingress before validating service boundaries, migrations, NATS tuning, and chaos recovery locally slows iteration and hides local bugs behind cluster YAML.

## Considered Options
1. **Kubernetes-first:** Build Helm charts and K8s manifests alongside initial Go code.
2. **Docker Compose first, Kubernetes post-M7:** Build multi-stage non-root Dockerfiles and a full `compose.yaml` (app roles + PG + NATS + Prometheus + Grafana + Loki + local EVM) first, validate chaos/load tests locally, then generate K8s/Helm manifests.

## Decision
Adopt **Docker Compose First, Kubernetes Migration After Local Validation**.

## Trade-offs & Consequences
- **Positive:** Fast feedback loop, lower local resource overhead, and validated container healthchecks/env configs before authoring Helm templates.
- **Negative:** Kubernetes-specific mechanics (HPA/KEDA metrics adapter, PDBs, NetworkPolicies) are deferred until Milestone M8.
- **Trigger to Revisit:** Enter Milestone M8 immediately after Milestone M7 load and chaos verification passes cleanly.
