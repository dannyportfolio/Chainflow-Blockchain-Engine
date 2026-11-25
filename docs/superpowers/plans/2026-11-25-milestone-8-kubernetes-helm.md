# Milestone 8: Kubernetes & Helm Migration Implementation Plan

> **Goal:** Package the four validated ChainFlow role binaries (`api`, `orchestrator`, `tx-worker`, `confirmation-worker`) built by `deployments/docker/Dockerfile` into a production-hardened Helm chart with KEDA queue-depth autoscaling, PodDisruptionBudgets, NetworkPolicies, and graceful drain hooks.

---

## 1. Helm Chart Layout (`deployments/helm/chainflow/`)

```text
deployments/helm/chainflow/
├── Chart.yaml
├── values.yaml
├── values-staging.yaml
├── values-prod.yaml
└── templates/
    ├── _helpers.tpl
    ├── configmap.yaml               # Non-secret CHAINFLOW_* config
    ├── externalsecret.yaml          # Optional ExternalSecrets / Secret template
    ├── serviceaccount.yaml
    ├── api-deployment.yaml
    ├── api-service.yaml
    ├── api-hpa.yaml
    ├── api-pdb.yaml
    ├── orchestrator-deployment.yaml
    ├── tx-worker-deployment.yaml
    ├── tx-worker-keda.yaml          # KEDA ScaledObject on NATS JetStream lag
    ├── tx-worker-pdb.yaml
    ├── confirmation-worker-deployment.yaml
    ├── networkpolicy.yaml           # Default-deny + per-role egress/ingress
    └── servicemonitor.yaml          # Prometheus Operator ServiceMonitor (/metrics)
```

---

## 2. Role-by-Role Workload Specifications

| Role | Binary | Replicas (Prod) | Scaling Mechanism | Health / Metrics |
| :--- | :--- | :--- | :--- | :--- |
| `api` | `./apps/api` | Min `2`, Max `10` | `HorizontalPodAutoscaler` (`cpu: 70%`) | `GET /healthz`, `GET /readyz`, `GET /metrics` (`:8080`) |
| `orchestrator` | `./apps/orchestrator` | `1` (or `2` with `Recreate` / DB lease) | Fixed (`1` active outbox relay + sweeper) | Process liveness |
| `tx-worker` | `./apps/tx-worker` | Min `2`, Max `20` | KEDA `ScaledObject` on NATS JetStream consumer lag | Process liveness + `preStop` drain |
| `confirmation-worker` | `./apps/confirmation-worker` | `1` (or `2` partitioned by `CHAINFLOW_CHAIN_ID`) | Fixed per chain | Process liveness |

---

## 3. Specific Configuration & Security Hardening

### 3.1 Non-Root & Least-Privilege Pod SecurityContext (All Deployments)
Matches `gcr.io/distroless/static-debian12:nonroot` (`uid/gid 65532`) in `deployments/docker/Dockerfile`:
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile:
    type: RuntimeDefault
containers:
  - name: service
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
```

### 3.2 Graceful Shutdown & `preStop` Hooks
- `terminationGracePeriodSeconds: 45` (exceeds `CHAINFLOW_HTTP_SHUTDOWN_TIMEOUT=25s` and `CHAINFLOW_WORKER_LEASE_TTL=30s`).
- `api` container:
  - `readinessProbe`: `httpGet: { path: /readyz, port: 8080 }`, `initialDelaySeconds: 5`, `periodSeconds: 5`
  - `livenessProbe`: `httpGet: { path: /healthz, port: 8080 }`, `initialDelaySeconds: 10`, `periodSeconds: 10`
  - `lifecycle.preStop.exec.command`: `["/bin/sh", "-c", "sleep 5"]` (or distroless-compatible: rely on `CHAINFLOW_HTTP_SHUTDOWN_TIMEOUT=25s` since distroless has no `/bin/sh`; set `terminationGracePeriodSeconds: 35` so Kubernetes endpoint removal propagates before `SIGTERM` completes).
- `tx-worker` container:
  - `terminationGracePeriodSeconds: 45` so `WorkerPool.Run(ctx)` finishes in-flight `SendRawTransaction` calls and acks/naks JetStream messages cleanly.

### 3.3 KEDA Queue-Depth Autoscaling (`tx-worker-keda.yaml`)
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: chainflow-tx-worker
spec:
  scaleTargetRef:
    name: chainflow-tx-worker
  minReplicaCount: 2
  maxReplicaCount: 20
  pollingInterval: 10
  cooldownPeriod: 60
  triggers:
    - type: nats-jetstream
      metadata:
        natsServerMonitoringEndpoint: "nats.chainflow.svc.cluster.local:8222"
        stream: "CHAINFLOW_TX"
        consumer: "tx-worker-dispatch"
        lagThreshold: "50"
        activationLagThreshold: "1"
```

### 3.4 PodDisruptionBudgets (`api-pdb.yaml`, `tx-worker-pdb.yaml`)
- `api`: `minAvailable: 1` (or `maxUnavailable: 1` when `replicas >= 2`).
- `tx-worker`: `minAvailable: 1` so cluster upgrades never halt transaction dispatch.

### 3.5 NetworkPolicies (`networkpolicy.yaml`)
1. **Default Deny:** `policyTypes: [Ingress, Egress]` on all pods in the `chainflow` namespace.
2. **DNS Egress:** Allow UDP/TCP `53` to `kube-system` (`CoreDNS`).
3. **`api`:**
   - Ingress: Port `8080` from Ingress Controller namespace and Prometheus namespace (`/metrics`).
   - Egress: PostgreSQL (`5432`).
4. **`orchestrator`:**
   - Egress: PostgreSQL (`5432`), NATS (`4222`).
5. **`tx-worker` & `confirmation-worker`:**
   - Egress: PostgreSQL (`5432`), NATS (`4222`), and HTTPS (`443`) to external EVM RPC endpoints.

### 3.6 Secret & Config Separation
- `ConfigMap` (`configmap.yaml`): `CHAINFLOW_ENV`, `CHAINFLOW_LOG_LEVEL`, `CHAINFLOW_HTTP_ADDR`, `CHAINFLOW_NATS_URL`, `CHAINFLOW_NATS_STREAM`, `CHAINFLOW_CHAIN_ID`, `CHAINFLOW_REQUIRED_CONFIRMATIONS`, `CHAINFLOW_RPC_URLS`, `CHAINFLOW_WORKER_CONCURRENCY`, `CHAINFLOW_MONITOR_*`.
- `Secret` / `ExternalSecret` (`externalsecret.yaml`): `CHAINFLOW_DB_DSN`, `CHAINFLOW_API_KEYS`, `CHAINFLOW_DEV_SIGNER_KEYS` (or KMS/HSM signer credentials in production).

---

## 4. Step-by-Step Execution & Verification Tasks

1. **Task 1 — Scaffold Helm Chart & Values:** Create `Chart.yaml`, `_helpers.tpl`, `values.yaml`, `values-staging.yaml`, and `values-prod.yaml` with schema validation (`values.schema.json`).
2. **Task 2 — Implement Workload & Config Templates:** Author `configmap.yaml`, `externalsecret.yaml`, `serviceaccount.yaml`, and the 4 `Deployment`s + `api-service.yaml`.
3. **Task 3 — Implement Scaling, Availability & Security Policies:** Author `api-hpa.yaml`, `tx-worker-keda.yaml`, `api-pdb.yaml`, `tx-worker-pdb.yaml`, `networkpolicy.yaml`, and `servicemonitor.yaml`.
4. **Task 4 — Static Validation (CI Gate):**
   - `helm lint deployments/helm/chainflow`
   - `helm template chainflow deployments/helm/chainflow -f deployments/helm/chainflow/values-prod.yaml | kubeconform -strict -ignore-missing-schemas`
5. **Task 5 — Local Cluster Smoke & Drain Test (`kind`):**
   - Build images via `docker build --build-arg SERVICE=<role> -f deployments/docker/Dockerfile .`
   - Load into `kind`, deploy chart + Postgres + NATS, verify `/healthz`, `/readyz`, `/metrics`, submit test intents, and run `kubectl rollout restart deployment/chainflow-tx-worker` under load to verify zero lost/duplicated nonces.
