#!/usr/bin/env bash
# Run the production-shaped NUFS Helm smoke test in an isolated namespace.
#
# This script deliberately uses the chart in deploy/helm/nufs.  It is not a
# second deployment definition: all topology and image changes are Helm value
# overrides, and the rendered manifest is saved as evidence before install.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
CORE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
CHART_DIR="$CORE_DIR/deploy/helm/nufs"

KEEP=false
RENDER_ONLY=false
NAMESPACE=""
NAMESPACE_WAS_GENERATED=false
RELEASE="nufs-smoke"
IMAGE=""
KUBE_CONTEXT=""
RESULTS_DIR=""
VALUES_FILE=""
EXTRA_SET_ARGS=()
KUBECTL_ARGS=()
HELM_CONTEXT_ARGS=()
HELM_VALUE_ARGS=()
PORT_FORWARD_PIDS=()
AUTH_TOKEN=""                  # operator bearer; when set, seed + SigV4-sign S3 checks
SMOKE_AK="${SMOKE_AK:-AKIAIOSFODNN7EXAMPLE}"
SMOKE_SK="${SMOKE_SK:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
NAMESPACE_OWNED=false
MUTATED=false
STAGE="parse"
INITIAL_LEADER=""
FINAL_LEADER=""
PAYLOAD_HASH=""

usage() {
  cat <<'EOF'
Usage: run-v21-helm-smoke.sh [options]

Install the existing NUFS Helm chart in an isolated namespace and prove a
three-voter metadata cluster can serve S3 writes through a leader failover.

Required:
  --image IMAGE             Tagged image containing the NUFS runtime binaries.

Options:
  --namespace NAME          Namespace to use (default: unique nufs-smoke-*).
  --release NAME            Helm release name (default: nufs-smoke).
  --kube-context CONTEXT    kubectl/Helm context to use.
  --results DIR             Evidence directory (default: ./helm-smoke-results/<namespace>).
  --values FILE             Extra Helm values file (optional; applied before smoke overrides).
  --set KEY=VALUE           Extra Helm value override (may be repeated).
  --keep                    Keep the smoke namespace after the run.
  --render-only             Lint and render only; never contacts or mutates Kubernetes.
  -h, --help                Show this help and exit.

The smoke run always renders and installs metad.replicaCount=3 and
datanode.replicaCount=3. It rejects a rendered --allow-insecure-dev flag.
EOF
}

log() { printf '[helm-smoke] %s\n' "$*"; }
die() { printf '[helm-smoke] ERROR: %s\n' "$*" >&2; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

validate_dns_label() {
  local value="$1" label="$2"
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] && [ "${#value}" -le 63 ] || \
    die "$label must be a DNS label (lowercase letters, digits, hyphens; max 63 chars): $value"
}

validate_image() {
  [ -n "$IMAGE" ] || die "--image is required (for example registry.example/nufs/runtime:v1.0.0)"
  [[ "$IMAGE" != *[[:space:]]* ]] || die "--image must not contain whitespace"
  [[ "$IMAGE" != *@* ]] || die "--image must use an explicit tag, not a digest (the chart has repository/tag fields)"
  local last_component="${IMAGE##*/}"
  [[ "$last_component" == *:* ]] || \
    die "--image must include an explicit tag (for example registry.example/nufs/runtime:v1.0.0)"
  [ -n "${IMAGE##*:}" ] || die "--image tag must not be empty"
}

image_repository() { printf '%s' "${IMAGE%:*}"; }
image_tag() { printf '%s' "${IMAGE##*:}"; }

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

add_context_arguments() {
  if [ -n "$KUBE_CONTEXT" ]; then
    KUBECTL_ARGS=(--context "$KUBE_CONTEXT")
    HELM_CONTEXT_ARGS=(--kube-context "$KUBE_CONTEXT")
  fi
}

kubectl_cmd() {
  if [ "${#KUBECTL_ARGS[@]}" -gt 0 ]; then
    kubectl "${KUBECTL_ARGS[@]}" "$@"
  else
    kubectl "$@"
  fi
}

helm_cmd() {
  if [ "${#HELM_CONTEXT_ARGS[@]}" -gt 0 ]; then
    helm "${HELM_CONTEXT_ARGS[@]}" "$@"
  else
    helm "$@"
  fi
}

configure_values() {
  local repository tag
  repository="$(image_repository)"
  tag="$(image_tag)"
  HELM_VALUE_ARGS=()
  if [ -n "$VALUES_FILE" ]; then
    [ -f "$VALUES_FILE" ] || die "--values file does not exist: $VALUES_FILE"
    HELM_VALUE_ARGS+=(--values "$VALUES_FILE")
  fi
  if [ "${#EXTRA_SET_ARGS[@]}" -gt 0 ]; then
    HELM_VALUE_ARGS+=("${EXTRA_SET_ARGS[@]}")
  fi
  HELM_VALUE_ARGS+=(
    --set-string "metad.replicaCount=3"
    --set-string "datanode.replicaCount=3"
    --set-string "metad.image.repository=$repository"
    --set-string "metad.image.tag=$tag"
    --set-string "datanode.image.repository=$repository"
    --set-string "datanode.image.tag=$tag"
    --set-string "s3gateway.image.repository=$repository"
    --set-string "s3gateway.image.tag=$tag"
  )
}

assert_rendered_replicas() { # kind resource-name replicas
  local kind="$1" resource="$2" replicas="$3"
  awk -v kind="$kind" -v resource="$resource" -v replicas="$replicas" '
    /^---$/ { in_doc=0; matched=0; next }
    $1 == "kind:" && $2 == kind { in_doc=1; next }
    in_doc && $1 == "name:" && $2 == resource { matched=1; next }
    matched && $1 == "replicas:" && $2 == replicas { found=1 }
    END { exit found ? 0 : 1 }
  ' "$RESULTS_DIR/rendered.yaml"
}

validate_rendered_topology() {
  STAGE="validate-render"
  assert_rendered_replicas StatefulSet "$RELEASE-metad" 3 || \
    die "render did not contain StatefulSet/$RELEASE-metad with exactly 3 replicas"
  assert_rendered_replicas StatefulSet "$RELEASE-datanode" 3 || \
    die "render did not contain StatefulSet/$RELEASE-datanode with 3 replicas for the smoke RF=3 policy"
  grep -Eq -- '--raft-bootstrap-peers=.*meta-1=.*meta-2=.*meta-3=' "$RESULTS_DIR/rendered.yaml" || \
    die "render did not contain all three metad Raft voters"
  if grep -Fq -- '--allow-insecure-dev' "$RESULTS_DIR/rendered.yaml"; then
    die "render contains --allow-insecure-dev; insecure development deployments are not smoke evidence"
  fi
}

render_chart() {
  STAGE="lint"
  log "linting existing chart"
  helm_cmd lint "$CHART_DIR" "${HELM_VALUE_ARGS[@]}" | tee "$RESULTS_DIR/helm-lint.txt"

  STAGE="render"
  log "rendering three-metad, three-datanode topology"
  helm_cmd template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" "${HELM_VALUE_ARGS[@]}" > "$RESULTS_DIR/rendered.yaml"
  validate_rendered_topology
}

can_i() {
  local verb="$1" resource="$2"
  local answer
  answer="$(kubectl_cmd auth can-i "$verb" "$resource" 2>/dev/null || true)"
  [ "$answer" = "yes" ] || die "Kubernetes context lacks permission: $verb $resource"
}

preflight_cluster() {
  STAGE="cluster-preflight"
  log "checking Kubernetes context${KUBE_CONTEXT:+ $KUBE_CONTEXT}"
  kubectl_cmd version --request-timeout=15s >/dev/null || die "cannot reach the selected Kubernetes API server"
  can_i create namespaces
  can_i delete namespaces
  can_i get pods
  can_i list pods
  can_i delete pods
  can_i get events
  can_i get pods/log
  can_i create pods/portforward
}

prepare_namespace() {
  STAGE="namespace"
  if kubectl_cmd get namespace "$NAMESPACE" >/dev/null 2>&1; then
    local owned
    owned="$(kubectl_cmd get namespace "$NAMESPACE" -o jsonpath='{.metadata.labels.nufs\.io/helm-smoke}' 2>/dev/null || true)"
    [ "$owned" = "owned" ] || die "refusing to use existing namespace $NAMESPACE because it is not owned by this smoke script"
    NAMESPACE_OWNED=true
    log "reusing previously kept smoke namespace $NAMESPACE"
  else
    kubectl_cmd create namespace "$NAMESPACE" > "$RESULTS_DIR/namespace-create.txt"
    kubectl_cmd label namespace "$NAMESPACE" nufs.io/helm-smoke=owned --overwrite >/dev/null
    NAMESPACE_OWNED=true
    log "created isolated namespace $NAMESPACE"
  fi
}

install_chart() {
  STAGE="install"
  MUTATED=true
  log "installing/upgrading existing chart release $RELEASE"
  helm_cmd upgrade --install "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --wait --timeout 8m "${HELM_VALUE_ARGS[@]}" \
    | tee "$RESULTS_DIR/helm-upgrade.txt"
}

wait_for_workloads() {
  STAGE="readiness"
  log "waiting for three metad voters, three datanodes, and the S3 gateway"
  kubectl_cmd -n "$NAMESPACE" rollout status "statefulset/$RELEASE-metad" --timeout=8m
  kubectl_cmd -n "$NAMESPACE" rollout status "statefulset/$RELEASE-datanode" --timeout=8m
  kubectl_cmd -n "$NAMESPACE" rollout status "deployment/$RELEASE-s3gateway" --timeout=8m
  kubectl_cmd -n "$NAMESPACE" wait --for=condition=Ready pod \
    -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=metad" --timeout=2m
  local voters datanodes
  voters="$(kubectl_cmd -n "$NAMESPACE" get pod -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=metad" -o name | wc -l | tr -d '[:space:]')"
  datanodes="$(kubectl_cmd -n "$NAMESPACE" get pod -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=datanode" -o name | wc -l | tr -d '[:space:]')"
  [ "$voters" = 3 ] || die "expected 3 metad voters, found $voters"
  [ "$datanodes" -ge 3 ] || die "smoke RF=3 requires at least 3 datanodes, found $datanodes"
}

allocate_port() {
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
  else
    # A random high port is a fallback only; port-forward reports a clear error
    # if another process wins the race.
    printf '%s\n' "$((20000 + RANDOM % 20000))"
  fi
}

wait_for_url() { # url seconds description
  local url="$1" timeout="$2" description="$3"
  local end=$((SECONDS + timeout))
  until curl --connect-timeout 2 --max-time 5 -fsS "$url" >/dev/null; do
    [ "$SECONDS" -lt "$end" ] || die "timed out waiting for $description at $url"
    sleep 2
  done
}

start_port_forwards() {
  STAGE="port-forward"
  local gateway_port metad_port
  gateway_port="$(allocate_port)"
  metad_port="$(allocate_port)"
  [ "$gateway_port" != "$metad_port" ] || metad_port="$(allocate_port)"
  GATEWAY_ENDPOINT="http://127.0.0.1:$gateway_port"
  METAD_ENDPOINT="http://127.0.0.1:$metad_port"

  kubectl_cmd -n "$NAMESPACE" port-forward "service/$RELEASE-s3gateway" "$gateway_port:8080" \
    > "$RESULTS_DIR/port-forward-s3gateway.log" 2>&1 &
  PORT_FORWARD_PIDS+=("$!")
  kubectl_cmd -n "$NAMESPACE" port-forward "service/$RELEASE-metad" "$metad_port:8091" \
    > "$RESULTS_DIR/port-forward-metad.log" 2>&1 &
  PORT_FORWARD_PIDS+=("$!")

  wait_for_url "$GATEWAY_ENDPOINT/healthz" 60 "S3 gateway port-forward"
  wait_for_url "$METAD_ENDPOINT/api/v1/health" 60 "metad port-forward"
}

leader_from_status() {
  local status="$1"
  printf '%s\n' "$status" | grep -oE "${RELEASE}-metad-[0-9]+" | head -n 1 || true
}

wait_for_leader() { # [different-from]
  local previous="${1:-}" status candidate readiness end auth
  auth=""
  [ -n "$AUTH_TOKEN" ] && auth="-H Authorization: Bearer $AUTH_TOKEN"
  end=$((SECONDS + 120))
  until false; do
    status="$(curl --connect-timeout 2 --max-time 5 -fsS $auth "$METAD_ENDPOINT/api/v1/cluster/status" 2>/dev/null || true)"
    candidate="$(leader_from_status "$status")"
    readiness="$(curl --connect-timeout 2 --max-time 5 -fsS $auth "$METAD_ENDPOINT/api/v1/cluster/readiness" 2>/dev/null || true)"
    if [ -n "$candidate" ] && [ "$candidate" != "$previous" ] &&
      [[ "$readiness" != *'"status":"not_ready"'* ]]; then
      printf '%s\n' "$status" > "$RESULTS_DIR/cluster-status-${candidate}.json"
      printf '%s\n' "$readiness" > "$RESULTS_DIR/cluster-readiness-${candidate}.json"
      printf '%s\n' "$candidate"
      return 0
    fi
    [ "$SECONDS" -lt "$end" ] || die "timed out waiting for ${previous:+a leader different from $previous and }a ready metadata cluster"
    sleep 2
  done
}

seed_registry_credential() { # seed the S3 credential into the metad registry
  [ -n "$AUTH_TOKEN" ] || return 0
  STAGE="seed-credential"
  log "seeding S3 credential ($SMOKE_AK) into metad registry"
  curl --connect-timeout 5 --max-time 30 -fsS -X PUT \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    -H "content-type: application/json" \
    -d "{\"secret_key\":\"$SMOKE_SK\",\"principal\":\"helm-smoke\"}" \
    "$METAD_ENDPOINT/api/v1/auth/creds/$SMOKE_AK" \
    > "$RESULTS_DIR/seed-credential.txt" 2>&1 \
    || die "failed to seed S3 credential: $(cat "$RESULTS_DIR/seed-credential.txt" 2>/dev/null)"
  log "credential seeded"
}

s3_sigv4_request() { # method path body-file out-file  — python SigV4 signer
  local method="$1" path="$2" body="$3" out="$4"
  python3 - "$method" "$GATEWAY_ENDPOINT" "$path" "$body" "$out" "$SMOKE_AK" "$SMOKE_SK" <<'PYEOF' || return $?
import sys, os, hashlib, hmac, datetime, urllib.request, urllib.error
m, ep, path, body, out, ak, sk = sys.argv[1:8]
bd = open(body, 'rb').read() if body and os.path.exists(body) else b''
def sign(method, p, hdrs, data):
    n = datetime.datetime.now(datetime.timezone.utc)
    d = n.strftime('%Y%m%dT%H%M%SZ'); ds = n.strftime('%Y%m%d')
    hdrs['host'] = ep.split('//')[1]
    hdrs['x-amz-date'] = d
    hdrs['x-amz-content-sha256'] = hashlib.sha256(data).hexdigest()
    ch = ''.join(f'{k.lower()}:{v.strip()}\n' for k, v in sorted(hdrs.items()))
    sh = ';'.join(sorted(k.lower() for k in hdrs))
    cr = f'{method}\n{p}\n\n{ch}\n{sh}\n{hdrs["x-amz-content-sha256"]}'
    cs = f'{ds}/us-east-1/s3/aws4_request'
    sts = f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(k, m2): return hmac.new(k, m2.encode(), hashlib.sha256).digest()
    kd = skf(('AWS4'+sk).encode(), ds); kr = skf(kd, 'us-east-1'); ks = skf(kr, 's3'); kg = skf(ks, 'aws4_request')
    hdrs['Authorization'] = f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={hmac.new(kg, sts.encode(), hashlib.sha256).hexdigest()}'
    return hdrs
h = sign(m, path, {'content-type': 'application/octet-stream'}, bd)
req = urllib.request.Request(ep + path, data=bd or None, headers=h, method=m)
try:
    r = urllib.request.urlopen(req, timeout=60)
    open(out, 'wb').write(r.read())
    sys.exit(0 if r.status in (200, 201, 204) else 1)
except urllib.error.HTTPError as e:
    open(out, 'wb').write(e.read())
    print('HTTP %s' % e.code, file=sys.stderr)
    sys.exit(1)
PYEOF
}

s3_create_bucket_and_verify() { # object suffix
  local suffix="$1" bucket object downloaded hash
  bucket="helm-smoke-${NAMESPACE##*-}"
  object="payload-$suffix.txt"
  printf '%s\n' "nufs helm smoke payload namespace=$NAMESPACE release=$RELEASE" > "$RESULTS_DIR/payload.txt"
  PAYLOAD_HASH="$(sha256_file "$RESULTS_DIR/payload.txt")"

  if [ -n "$AUTH_TOKEN" ]; then
    STAGE="s3-create-bucket"
    s3_sigv4_request PUT "/$bucket" "" "$RESULTS_DIR/s3-create-bucket-$suffix.txt" \
      || die "SigV4 bucket create failed (see s3-create-bucket-$suffix.txt)"
    STAGE="s3-write-$suffix"
    s3_sigv4_request PUT "/$bucket/$object" "$RESULTS_DIR/payload.txt" "$RESULTS_DIR/s3-put-$suffix.txt" \
      || die "SigV4 object PUT failed (see s3-put-$suffix.txt)"
    STAGE="s3-read-$suffix"
    downloaded="$RESULTS_DIR/payload-$suffix.readback"
    s3_sigv4_request GET "/$bucket/$object" "" "$downloaded" \
      || die "SigV4 object GET failed"
  else
    STAGE="s3-create-bucket"
    curl --connect-timeout 5 --max-time 30 -fsS -X PUT "$GATEWAY_ENDPOINT/$bucket" \
      > "$RESULTS_DIR/s3-create-bucket-$suffix.txt"
    STAGE="s3-write-$suffix"
    curl --connect-timeout 5 --max-time 90 --retry 3 --retry-all-errors -fsS -X PUT \
      --data-binary "@$RESULTS_DIR/payload.txt" "$GATEWAY_ENDPOINT/$bucket/$object" \
      > "$RESULTS_DIR/s3-put-$suffix.txt"
    STAGE="s3-read-$suffix"
    downloaded="$RESULTS_DIR/payload-$suffix.readback"
    curl --connect-timeout 5 --max-time 90 --retry 3 --retry-all-errors -fsS \
      "$GATEWAY_ENDPOINT/$bucket/$object" -o "$downloaded"
  fi
  hash="$(sha256_file "$downloaded")"
  [ "$hash" = "$PAYLOAD_HASH" ] || die "S3 readback hash mismatch for $object: got $hash want $PAYLOAD_HASH"
  printf '%s  %s\n' "$hash" "$object" >> "$RESULTS_DIR/object-sha256.txt"
}

kill_leader_and_wait() {
  STAGE="leader-before-kill"
  INITIAL_LEADER="$(wait_for_leader)"
  log "current metadata leader is $INITIAL_LEADER; deleting it to force re-election"
  printf '%s\n' "$INITIAL_LEADER" > "$RESULTS_DIR/leader-before.txt"

  STAGE="leader-kill"
  kubectl_cmd -n "$NAMESPACE" delete pod "$INITIAL_LEADER" --wait=true --timeout=90s \
    | tee "$RESULTS_DIR/leader-delete.txt"

  STAGE="leader-re-election"
  FINAL_LEADER="$(wait_for_leader "$INITIAL_LEADER")"
  [ "$FINAL_LEADER" != "$INITIAL_LEADER" ] || die "leader did not change after deleting $INITIAL_LEADER"
  printf '%s\n' "$FINAL_LEADER" > "$RESULTS_DIR/leader-after.txt"
  log "new metadata leader is $FINAL_LEADER"
  kubectl_cmd -n "$NAMESPACE" rollout status "statefulset/$RELEASE-metad" --timeout=8m
}

collect_diagnostics() {
  [ "$NAMESPACE_OWNED" = true ] || return 0
  log "collecting failure diagnostics in $RESULTS_DIR"
  kubectl_cmd -n "$NAMESPACE" get all -o wide > "$RESULTS_DIR/kubernetes-all.txt" 2>&1 || true
  kubectl_cmd -n "$NAMESPACE" get pvc -o wide > "$RESULTS_DIR/kubernetes-pvc.txt" 2>&1 || true
  kubectl_cmd -n "$NAMESPACE" get events --sort-by=.lastTimestamp > "$RESULTS_DIR/kubernetes-events.txt" 2>&1 || true
  kubectl_cmd -n "$NAMESPACE" describe pods > "$RESULTS_DIR/kubernetes-pods-describe.txt" 2>&1 || true
  local pod
  while IFS= read -r pod; do
    [ -n "$pod" ] || continue
    kubectl_cmd -n "$NAMESPACE" logs "$pod" --all-containers=true --tail=250 \
      > "$RESULTS_DIR/${pod}.log" 2>&1 || true
    kubectl_cmd -n "$NAMESPACE" logs "$pod" --all-containers=true --previous --tail=250 \
      > "$RESULTS_DIR/${pod}.previous.log" 2>&1 || true
  done < <(kubectl_cmd -n "$NAMESPACE" get pods -o name 2>/dev/null | sed 's#^pod/##')
}

stop_port_forwards() {
  local pid
  for pid in "${PORT_FORWARD_PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${PORT_FORWARD_PIDS[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}

write_report() { # exit status
  local status="$1" result="FAIL"
  [ "$status" -eq 0 ] && result="PASS"
  {
    printf 'result=%s\n' "$result"
    printf 'stage=%s\n' "$STAGE"
    printf 'namespace=%s\n' "$NAMESPACE"
    printf 'release=%s\n' "$RELEASE"
    printf 'image=%s\n' "$IMAGE"
    printf 'mode=%s\n' "$([ "$RENDER_ONLY" = true ] && echo render-only || echo cluster)"
    printf 'metad_voters=3\n'
    printf 'datanodes_required_for_smoke_rf=3\n'
    [ -n "$INITIAL_LEADER" ] && printf 'leader_before=%s\n' "$INITIAL_LEADER"
    [ -n "$FINAL_LEADER" ] && printf 'leader_after=%s\n' "$FINAL_LEADER"
    [ -n "$PAYLOAD_HASH" ] && printf 'payload_sha256=%s\n' "$PAYLOAD_HASH"
    printf 'keep_namespace=%s\n' "$KEEP"
  } > "$RESULTS_DIR/report.env"
}

cleanup_namespace() {
  [ "$KEEP" = true ] && return 0
  [ "$NAMESPACE_OWNED" = true ] || return 0
  log "deleting only smoke namespace $NAMESPACE"
  kubectl_cmd delete namespace "$NAMESPACE" --wait=true --timeout=3m > "$RESULTS_DIR/namespace-delete.txt" 2>&1 || \
    log "namespace cleanup did not complete; see $RESULTS_DIR/namespace-delete.txt"
}

on_exit() {
  local status=$?
  trap - EXIT
  set +e
  if [ "$RENDER_ONLY" = false ] && [ "$status" -ne 0 ]; then
    collect_diagnostics
  fi
  stop_port_forwards
  if [ "$RENDER_ONLY" = false ]; then
    cleanup_namespace
  fi
  [ -n "$RESULTS_DIR" ] && write_report "$status"
  exit "$status"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --namespace) [ "$#" -ge 2 ] || die "--namespace requires a value"; NAMESPACE="$2"; shift 2 ;;
    --release) [ "$#" -ge 2 ] || die "--release requires a value"; RELEASE="$2"; shift 2 ;;
    --image) [ "$#" -ge 2 ] || die "--image requires a value"; IMAGE="$2"; shift 2 ;;
    --kube-context) [ "$#" -ge 2 ] || die "--kube-context requires a value"; KUBE_CONTEXT="$2"; shift 2 ;;
    --results) [ "$#" -ge 2 ] || die "--results requires a value"; RESULTS_DIR="$2"; shift 2 ;;
    --values) [ "$#" -ge 2 ] || die "--values requires a file"; VALUES_FILE="$2"; shift 2 ;;
    --set) [ "$#" -ge 2 ] || die "--set requires KEY=VALUE"; EXTRA_SET_ARGS+=(--set-string "$2"); shift 2 ;;
    --auth-token) [ "$#" -ge 2 ] || die "--auth-token requires a value"; AUTH_TOKEN="$2"; shift 2 ;;
    --access-key) [ "$#" -ge 2 ] || die "--access-key requires a value"; SMOKE_AK="$2"; shift 2 ;;
    --secret-key) [ "$#" -ge 2 ] || die "--secret-key requires a value"; SMOKE_SK="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --render-only) RENDER_ONLY=true; shift ;;
    *) die "unknown option: $1 (use --help)" ;;
  esac
done

validate_image
validate_dns_label "$RELEASE" "--release"
if [ -z "$NAMESPACE" ]; then
  NAMESPACE="nufs-smoke-$(date +%Y%m%d%H%M%S)-$RANDOM"
  NAMESPACE="${NAMESPACE:0:63}"
  NAMESPACE_WAS_GENERATED=true
fi
validate_dns_label "$NAMESPACE" "--namespace"
if [ -z "$RESULTS_DIR" ]; then
  RESULTS_DIR="$PWD/helm-smoke-results/$NAMESPACE"
fi
mkdir -p "$RESULTS_DIR"
trap on_exit EXIT
trap 'exit 130' INT TERM

require_command helm
if [ "$RENDER_ONLY" = false ]; then
  require_command kubectl
  require_command curl
  command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 || \
    die "required SHA-256 command not found (sha256sum or shasum)"
fi
add_context_arguments
configure_values
render_chart

if [ "$RENDER_ONLY" = true ]; then
  STAGE="render-only-complete"
  log "render-only validation passed; Kubernetes was not contacted"
  exit 0
fi

preflight_cluster
prepare_namespace
install_chart
wait_for_workloads
start_port_forwards
seed_registry_credential
s3_create_bucket_and_verify before-failover
kill_leader_and_wait
s3_create_bucket_and_verify after-failover
STAGE="complete"
log "PASS: S3 write/read and leader failover checks completed"
