# karmada-multicluster-dashboard

A terminal dashboard for observing live deployments across Karmada multi-cluster setups.

> **Scope**: this tool was built specifically to support tutorial guides for
> [Karmada](https://karmada.io). Its options, canary detection logic, traffic format, and
> scenario modes are all tailored to the needs of those demos and the specific test application
> they use. It is not a general-purpose Kubernetes dashboard and is not designed to work with
> arbitrary clusters, applications, or rollout configurations without modification.

Displays a per-cluster replica panel and a live traffic panel side by side. Supports both
Argo Rollouts canary workflows and primitive Karmada deployments (canary via OverridePolicy,
rolling upgrade, wave rollout).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│        Argo Rollouts Multi-Cluster Dashboard  [q] quit                      │
│ Healthy  step 8/8  remaining 0  setWeight 100%  actualWeight 100%  ...      │
│─────────────────────────────────────────────────────────────────────────────│
│ member1  stable=3 canary=0 │ member2  stable=3 canary=0 │ member3  stable=3 canary=0  │
│────────────────────────────┼────────────────────────────┼─────────────────────────────│
│ ○ http-probe-app-...       │ ○ http-probe-app-...       │ ○ http-probe-app-...        │
│ ○ http-probe-app-...       │ ○ http-probe-app-...       │ ○ http-probe-app-...        │
│ ○ http-probe-app-...       │ ○ http-probe-app-...       │ ○ http-probe-app-...        │
│──────────────────────────────── Traffic ────────────────────────────────────────────│
│ member1                    │ member2                    │ member3                     │
│ http-probe-app-xyz  ...    │ http-probe-app-abc  ...    │ http-probe-app-def ...      │
│ http-probe-app-xyz  ...    │ http-probe-app-abc  ...    │ http-probe-app-def ...      │
└─────────────────────────────────────────────────────────────────────────────┘
```

Canary pods and traffic responses are highlighted in yellow (`●`); stable pods in green (`○`).

## Requirements

- Python 3.8+
- `kubectl` on `PATH`
- Port-forwards running to each member cluster's ingress on the configured ports (see Usage)

No third-party Python packages required — only the standard library.

## Installation

```bash
git clone https://github.com/unixsurfer/karmada-multicluster-dashboard
cd karmada-multicluster-dashboard
```

## Usage

### With Argo Rollouts

```bash
python3 dashboard.py \
  --namespace <ns> \
  --rollout-name <name> \
  --host-header <host> \
  --members-kubeconfig ~/.kube/members.config \
  --karmada-kubeconfig ~/.kube/karmada.config \
  --scenario s2
```

### Without Argo Rollouts (primitive deployments)

```bash
python3 dashboard.py --no-rollout \
  --namespace <ns> \
  --host-header <host> \
  --members-kubeconfig ~/.kube/members.config
```

### Options

| Flag | Description | Default |
|---|---|---|
| `--no-rollout` | Disable Argo Rollouts status panel | off |
| `--namespace NAME` | Kubernetes namespace | `default` |
| `--host-header HOST` | Host header for traffic requests | `http-probe.local` |
| `--members-kubeconfig PATH` | kubeconfig for member clusters | `~/.kube/members.config` |
| `--rollout-name NAME` | Argo Rollouts rollout name (required without `--no-rollout`) | |
| `--karmada-kubeconfig PATH` | kubeconfig for Karmada API | `~/.kube/karmada.config` |
| `--karmada-context NAME` | kubectl context for Karmada API | `karmada-apiserver` |
| `--cluster label:context:port:metrics-port` | Cluster spec, repeatable | see below |
| `--scenario s2\|s4` | Enable additional panels | |
| `--web-metrics-port N` | Port for web-metrics readiness probe | `9095` |

## Examples

**Default cluster configuration:**

When no `--cluster` flags are provided, the dashboard uses the contexts and ports that match
the standard Karmada local environment (`export KUBECONFIG=~/.kube/members.config`):

- `member1` → context `member1`, ingress port `8090`, metrics port `10254`
- `member2` → context `member2`, ingress port `8091`, metrics port `10255`
- `member3` → context `member3`, ingress port `8092`, metrics port `10256`

**Argo Rollouts (Scenario 2 with ingress metrics):**

```bash
python3 dashboard.py \
  --namespace <namespace> \
  --rollout-name <rollout-name> \
  --host-header http-probe.local \
  --members-kubeconfig ~/.kube/members.config \
  --karmada-kubeconfig ~/.kube/karmada.config \
  --scenario s2
```

**Primitive deployment (OverridePolicy-based canary, no Argo Rollouts):**

```bash
python3 dashboard.py --no-rollout \
  --namespace default \
  --host-header http-probe.local \
  --members-kubeconfig ~/.kube/members.config
```

**Custom cluster configuration:**

```bash
python3 dashboard.py --no-rollout \
  --cluster member1:member1:8090:10254 \
  --cluster member2:member2:8091:10255 \
  --cluster member3:member3:8092:10256 \
  --namespace default \
  --host-header http-probe.local \
  --members-kubeconfig ~/.kube/members.config
```

## Dashboard panels

### Replica panel (top half)

Shows all running pods per cluster, updated every 2 seconds via `kubectl get pods`.

- `○` green — stable pod
- `●` yellow — canary pod
- Column header shows `stable=N canary=N` per cluster

**Canary detection:**

- With Argo Rollouts: pods are identified as canary by image tag (the minority tag across all clusters is treated as canary)
- Without Argo Rollouts (`--no-rollout`): pods are identified by `pod-template-hash` label (the minority hash is treated as canary)

### Traffic panel (bottom half)

Streams live `/info` responses from each cluster's ingress, updated every 500ms. Responses from
canary pods are highlighted yellow.

**Canary detection in traffic:**

- With Argo Rollouts: canary responses are identified by version tag in the `/info` response body
- Without Argo Rollouts: canary responses are identified by `cluster_name` field (the minority cluster name across all responses is treated as canary)

### Rollout status line (Argo Rollouts mode only)

Shows the current rollout phase, step progress, `setWeight`/`actualWeight`, and replica counts,
updated every 2 seconds. Phase is colour-coded: green (`Healthy`), yellow (`Paused`,
`Progressing`), red (`Degraded`).

```
 Paused (manual)  step 7/18  remaining 11  setWeight 60%  actualWeight 60%  replicas 10  updated 1
```

| Field | Source | Notes |
|---|---|---|
| phase | `status.phase` | `Paused (manual)` when `spec.paused=true`; plain `Paused` for a step pause |
| step N/total | `status.currentStepIndex` / `len(spec.strategy.canary.steps)` | Index points to the next step to execute |
| remaining | total − current | Steps left before completion |
| setWeight | Last `setWeight` step before `currentStepIndex` | 100% when phase is `Healthy` |
| actualWeight | `status.canary.weights.canary.weight` if present, else `setWeight` | Live NGINX weight during active canary |
| replicas | `status.replicas` | Total pods across all clusters |
| updated | `status.updatedReplicas` | Pods running the canary revision |

### Ingress metrics panel (scenario `s2`)

Shows per-cluster NGINX request deltas (new requests in the last 5 seconds) for stable and canary
ingress backends. Requires the metrics port-forward to be running on the configured `metrics-port`
for each cluster.

### Web-metrics badge (scenario `s4`)

Shows a `READY` / `NOT READY` badge in the top-right corner, polling `localhost:<web-metrics-port>/info`
every 2 seconds. Used with Argo Rollouts analysis demos to visualise the readiness state being
evaluated by the analysis controller.

## Port-forward requirements

The dashboard expects port-forwards to be running before it starts. Set these up once before
launching the dashboard:

```bash
export KUBECONFIG=~/.kube/members.config

kubectl --context member1 -n ingress-nginx \
  port-forward deployment/ingress-nginx-controller 8090:80 &

kubectl --context member2 -n ingress-nginx \
  port-forward deployment/ingress-nginx-controller 8091:80 &

kubectl --context member3 -n ingress-nginx \
  port-forward deployment/ingress-nginx-controller 8092:80 &
```

For ingress metrics (scenario `s2`), additional port-forwards to port `10254` are managed
automatically by the dashboard itself.

## Keyboard shortcuts

| Key | Action |
|---|---|
| `q` / `Q` / `Esc` | Quit |

## Architecture

The dashboard runs a dedicated background thread for each data source. All state is protected by
per-object locks; the `draw` loop reads snapshots under lock and never blocks on I/O. Terminal
resize is handled automatically — `rows, cols = stdscr.getmaxyx()` is called on every frame.

| Thread | Interval | Data source | Active for |
|---|---|---|---|
| `rollout_worker` | 2s | `kubectl get rollout` on Karmada API | Argo Rollouts mode |
| `replica_worker` × N | 2s | `kubectl get pods` on each member cluster | always |
| `traffic_worker` × N | 0.5s | `GET /info` via port-forward | always |
| `metrics_worker` × N | 5s | NGINX Prometheus metrics via internal port-forward | `s2` only |
| `web_metrics_worker` | 2s | `GET /info` on `--web-metrics-port` | `s4` only |

Poll intervals can be tuned by editing the constants at the top of `dashboard.py`:

```python
REFRESH_REPLICAS_S      = 2
REFRESH_ROLLOUT_S       = 2
TRAFFIC_INTERVAL_S      = 0.5
METRICS_INTERVAL_S      = 5
WEB_METRICS_INTERVAL_S  = 2
```

## Background

This dashboard was built alongside a Karmada multi-cluster demo environment covering:

- Argo Rollouts canary deployments propagated across member clusters via Karmada
- Primitive canary, rolling upgrade, and wave rollout strategies using Karmada `OverridePolicy`

The `--no-rollout` mode was added to support the primitive deployment workflows, where no Argo
Rollouts controller is present but the same visual feedback (replica distribution, live traffic,
canary highlighting) is still useful.
