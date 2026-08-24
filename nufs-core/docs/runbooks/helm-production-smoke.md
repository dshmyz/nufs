# Helm production smoke runbook

`scripts/soak/run-v21-helm-smoke.sh` is the production-shaped acceptance gate
for the existing `deploy/helm/nufs` chart. It installs no alternate manifests:
it renders the chart, forces a three-voter metad topology, then runs the
verification in one isolated Kubernetes namespace.

## What it proves

For a real Kubernetes run, the script:

1. lints and renders the chart before creating any cluster resource;
2. enforces three metad voters and three datanodes (the smoke policy is RF=3),
   and rejects a rendered `--allow-insecure-dev` option;
3. waits for the metad and datanode StatefulSets and S3 gateway Deployment;
4. creates a bucket through the S3 gateway, writes a deterministic object,
   reads it back, and records its SHA-256;
5. finds and deletes the current metad leader pod, waits for a *different*
   leader and ready cluster, then repeats the write/read and checks the same
   payload hash; and
6. writes artifacts for every run. On failure it captures resources, PVCs,
   events, pod descriptions, current/previous container logs, port-forward
   logs, the Helm render, and Helm output before cleanup.

The exit status is zero only when all checks succeed.

## Prerequisites

- Bash, Helm 3, `kubectl`, `curl`, and either `sha256sum` or `shasum`.
- A reachable Kubernetes context with permission to create/delete namespaces,
  create port-forwards, list/read pod logs and events, and delete pods in the
  smoke namespace.
- A tagged image available to every cluster node. It must contain the NUFS
  `metad`, `datanode`, and `nufs-s3` runtime binaries because the existing
  chart has one repository/tag value per process and the script applies the
  supplied image to each of them.
- Storage provisioning compatible with the chart's PVCs. For clusters without
  a default StorageClass, pass an override values file with `global.storageClass`
  (or the per-service persistence storage class) set.

Do not use an image or values file that injects `--allow-insecure-dev`; the
script deliberately rejects that rendered deployment as non-production smoke
evidence.

### Loading an image into a local cluster

Build or pull the exact tagged image first, then load it into each local
cluster's node image store. For example:

```sh
docker build -t nufs/runtime:smoke .
kind load docker-image nufs/runtime:smoke --name nufs
# or: minikube image load nufs/runtime:smoke
```

For a remote cluster, push the image to the registry accessible from the
nodes. If the registry needs authentication, create the cluster pull secret
and pass a values file such as:

```yaml
global:
  imagePullSecrets:
    - name: registry-credentials
  storageClass: fast-rwo
```

Then use it with `--values /path/to/smoke-overrides.yaml`. The chart's current
secret/value inputs for metadata backup remain available under
`metad.backup.credentialsSecret`; no backup secret is needed for this smoke
test unless a supplied environment override enables backups.

## Commands

Review the interface without any prerequisites:

```sh
bash nufs-core/scripts/soak/run-v21-helm-smoke.sh --help
```

Render and lint only. This never invokes `kubectl` or creates resources, but
it intentionally still requires Helm and a valid tagged image:

```sh
IMAGE=registry.example/nufs/runtime:v1.0.0
RESULTS="$PWD/helm-smoke-results/render-$(date +%Y%m%d%H%M%S)"
bash nufs-core/scripts/soak/run-v21-helm-smoke.sh \
  --render-only --image "$IMAGE" --results "$RESULTS"
```

Run the real smoke test against an explicitly selected cluster:

```sh
IMAGE=registry.example/nufs/runtime:v1.0.0
RESULTS="$PWD/helm-smoke-results/real-$(date +%Y%m%d%H%M%S)"
bash nufs-core/scripts/soak/run-v21-helm-smoke.sh \
  --kube-context nufs-staging \
  --image "$IMAGE" \
  --values /secure/path/nufs-smoke-overrides.yaml \
  --results "$RESULTS"
```

`--namespace NAME` and `--release NAME` are available for reproducible test
names. The script only accepts a pre-existing namespace if it carries its own
`nufs.io/helm-smoke=owned` label, preventing accidental use of an operator
namespace. `--set KEY=VALUE` may be repeated for normal chart overrides, but
the script's three-voter/three-datanode overrides take precedence only when no
later conflicting `--set` is supplied; do not lower either count.

## Expected report and artifacts

`<results>/report.env` is the concise machine-readable result:

- `result=PASS|FAIL`, `stage`, `mode`, `namespace`, `release`, and `image`;
- `metad_voters=3` and `datanodes_required_for_smoke_rf=3`;
- `leader_before` and `leader_after` after a failover run; and
- `payload_sha256`, which must match both object reads.

Important files under the results directory are:

- `helm-lint.txt` and `rendered.yaml` — pre-install chart evidence;
- `helm-upgrade.txt`, `cluster-status-*.json`, and
  `cluster-readiness-*.json` — deployment/failover evidence;
- `object-sha256.txt`, `payload-*.readback`, and S3 request output —
  byte-exact object evidence; and
- on failure, `kubernetes-*.txt`, `*.log`, `*.previous.log`, and
  `port-forward-*.log` — diagnostics collected before cleanup.

## Cleanup

By default the EXIT trap deletes only the namespace that the script created or
previously labelled as its own. It does not delete cluster-wide resources or
any unlabelled namespace. Use `--keep` to retain the namespace for debugging;
rerunning with that namespace is supported because the ownership label makes
the run idempotent. Remove a kept namespace manually when investigation is
finished:

```sh
kubectl --context nufs-staging delete namespace nufs-smoke-...
```
