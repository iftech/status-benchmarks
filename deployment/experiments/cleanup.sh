#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "${1:-}" in
  group|private) scenarios="$1" ;;
  all) scenarios="group private" ;;
  -h|--help)
    printf 'Usage: %s group|private|all\nDeletes the selected experiment namespaces and all their resources.\n' "$0"
    exit 0
    ;;
  *) printf 'Select group, private or all explicitly\n' >&2; exit 2 ;;
esac

for scenario in $scenarios; do
  kubectl delete -f "$script_dir/$scenario/namespace.yaml" --ignore-not-found
done
