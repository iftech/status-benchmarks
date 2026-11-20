#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rollout_timeout="${STATUS_BENCHMARK_ROLLOUT_TIMEOUT:-20m}"

case "${1:-all}" in
  group|private) scenarios="$1" ;;
  all) scenarios="group private" ;;
  -h|--help)
    printf 'Usage: %s [group|private|all]\nUses the current kubectl context and waits for each deployment.\n' "$0"
    exit 0
    ;;
  *) printf 'Expected group, private or all\n' >&2; exit 2 ;;
esac

for scenario in $scenarios; do
  namespace="status-bench-$scenario"
  manifests="$script_dir/$scenario"
  printf 'Deploying %s experiment in %s\n' "$scenario" "$namespace"
  kubectl apply -f "$manifests/namespace.yaml"
  kubectl apply -f "$manifests/access.yaml"

  for service in service/status-service-bootstrap-service.yaml service/status-service-node-service.yaml \
                 relay/status-backend-relay-services.yaml light/status-backend-light-service.yaml; do
    kubectl apply -f "$manifests/$service"
  done

  kubectl apply -f "$manifests/service/status-service-bootstrap.yaml"
  kubectl -n "$namespace" rollout status statefulset/status-service-bootstrap --timeout="$rollout_timeout"
  kubectl apply -f "$manifests/service/status-service-node.yaml"
  kubectl -n "$namespace" rollout status statefulset/status-service-node --timeout="$rollout_timeout"

  kubectl apply -f "$manifests/relay/status-backend-relay.yaml"
  kubectl apply -f "$manifests/light/status-backend-light.yaml"
  kubectl -n "$namespace" rollout status statefulset/status-backend-relay --timeout="$rollout_timeout"
  kubectl -n "$namespace" rollout status statefulset/status-backend-light --timeout="$rollout_timeout"
done
