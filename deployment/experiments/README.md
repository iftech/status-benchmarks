# Parallel Messenger reliability experiments

These are separate copies of the messaging deployment manifests, configured for:

| Scenario | Namespace | Status nodes | Supporting Waku nodes |
| --- | --- | --- | --- |
| 20-member group | `status-bench-group` | 10 light, 10 relay | 1 bootstrap, 5 store/relay nodes |
| 10 independent 1:1 chats | `status-bench-private` | 10 light, 10 relay | 1 bootstrap, 5 store/relay nodes |

Each copy uses its own namespace for services, DNS searches, bootstrap discovery,
store discovery, service accounts, roles and bindings. The relay service's legacy
cluster-wide RBAC is replaced with namespace-scoped RBAC in these copies.
The Status image and Waku protocol settings match the original deployment.

The existing controlbox in `status-go-test` can discover both deployments through
the read-only role bindings in `access.yaml`. A controlbox using `controlbox-sa`
inside either experiment namespace also has access to its own nodes.

## Deploy

From the repository root, first select the intended kubectl context, then run:

```sh
kubectl config current-context
./deployment/experiments/deploy.sh all
```

Use `group` or `private` instead of `all` to deploy one environment. The script
waits for bootstrap, store and Status StatefulSets. Set
`STATUS_BENCHMARK_ROLLOUT_TIMEOUT` to override the default `20m` rollout timeout.
The original deployment and controlbox in `status-go-test` are kept intact.

## Run both experiments

Upload the updated benchmark Python files, including `src/delivery_artifacts.py`,
to the controlbox. Run each command in a separate terminal or PyCharm run
configuration, with the repository as the working directory:

```sh
/home/venv/bin/python main.py --scenario group
```

```sh
/home/venv/bin/python main.py --scenario private
```

The two processes can run simultaneously. Separate processes keep reporting and
plot generation in one experiment from blocking the other experiment's send loop.

Defaults implement the nominal 8-hour workloads:

| Scenario | Messages per node | Interval | Total messages | Expected deliveries |
| --- | ---: | ---: | ---: | ---: |
| Group | 480 | 60 seconds | 9,600 | 182,400 |
| Private | 960 | 30 seconds | 19,200 | 19,200 |

Use `--num-messages` for a short validation run before starting the full experiment:

```sh
/home/venv/bin/python main.py --scenario group --num-messages 3
/home/venv/bin/python main.py --scenario private --num-messages 3
```

Additional options:

- `--namespace NAME` selects another deployed namespace.
- `--interval SECONDS` overrides the send-start interval.
- `--output-dir PATH` selects a results directory.
- `--no-plots` disables plots.

Results are written at completion under
`logs/delivery-results/status-bench-group/` or
`logs/delivery-results/status-bench-private/`. Each summary records its namespace.
Plots go under `plots/<namespace>/<scenario>/`. Diagnostics remain disabled by
default. Missed send slots preserve the message count and can extend a run.

Each completed run writes four files, prefixed with its scenario and run ID:

- `*_summary.json`: requirement-level results, node counts, cadence, latency
  statistics and reliability overall and by recipient type. References to the
  detailed files use filenames relative to the summary's directory.
- `*_metrics.json`: detailed configuration, cadence and diagnostic summaries.
- `*_messages.csv`: message identifiers, send timestamps and API send outcomes.
- `*_deliveries.csv`: individual expected deliveries, latency, duplicates and order.

The latency plots show percentages and counts in five buckets, a logarithmic
curve of the percentage taking longer than each latency threshold, and median,
p95 and maximum latency in five-minute windows grouped by send time. Each plot
includes overall, light-recipient and relay-recipient reliability, duplicate and
out-of-order delivery counts, and percentile statistics.
Receive timestamps still have one-second precision.

Regenerate plots from an existing report and its CSV files without running an
experiment:

```sh
/home/venv/bin/python -m src.plotting logs/delivery-results/status-bench-group/group_<run-id>_metrics.json
```

The command finds CSV files beside the report first, accepts `--output-dir PATH`,
and updates the report's plot references without changing its measured results.

Separate bootstrap and store references keep the fresh deployments on separate
peer-discovery paths. These manifests do not add a network-policy boundary;
Kubernetes hosts, CPU and network capacity are still shared. Check the peer lists
after deployment before interpreting concurrent runs as independent workloads.

## Cleanup

The cleanup command deletes the selected experiment namespaces and everything
inside them. It requires an explicit selection:

```sh
./deployment/experiments/cleanup.sh group
./deployment/experiments/cleanup.sh private
```

Use `all` to delete both experiment namespaces. The original `status-go-test`
namespace is not targeted.
