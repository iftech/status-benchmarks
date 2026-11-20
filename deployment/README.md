# Deployment

Deployment files that will be used for the deployments in the experiments.

## Enable delivery tracing from the CLI

Tracing is disabled by default. To collect diagnostic events, deploy a backend
image supporting `message.trace` with `STATUS_MESSAGE_TRACE=1` on the Status nodes,
then pass `--trace-output-dir PATH` to the benchmark runner. For example, from the
repository root inside controlbox:

```sh
/home/venv/bin/python main.py --scenario group --num-messages 3 \
  --trace-output-dir logs/delivery-traces/status-bench-group

/home/venv/bin/python main.py --scenario private --num-messages 3 \
  --trace-output-dir logs/delivery-traces/status-bench-private
```

The option enables diagnostic signal retention and trace reports for either
scenario. Without it, the runner passes `None` and tracing stays disabled.
Standard metrics, summaries and CSV files still use `--output-dir`, which defaults
to `logs/delivery-results/<namespace>/`. Tracing retains detailed signal histories,
so use short diagnostic runs when investigating a delivery issue.

## light
[Definition](https://www.notion.so/Chat-Protocol-Benchmarks-1938f96fb65c80d8b22fdf641c5ff003?source=copy_link#1998f96fb65c802095e6fc894dc0b4fe)

A light node, is a status-backend node that runs in “light mode” (edge node in Waku concept).
Which means it uses Waku Peer Exchange for discovery, light push to send messages,
filter to receive live messages and store to retrieve historical messages.
It does not provide services.


## relay
[Definition](https://www.notion.so/Chat-Protocol-Benchmarks-1938f96fb65c80d8b22fdf641c5ff003?source=copy_link#1998f96fb65c8014bf0af42ce2645fa9)

A relay node, is a status-backend node that runs in “relay mode”.
Which means it uses discv5 and Waku Peer Exchange for discovery (rendezvous wip),
relay to send and receive live messages, store as a client to retrieved historical messages,
and provide filter, light push and waku peer exchanges services to other (light/edge) nodes.


## service
[Definition](https://www.notion.so/Chat-Protocol-Benchmarks-1938f96fb65c80d8b22fdf641c5ff003?source=copy_link#1998f96fb65c80c4b972fb560bd0f6f3)
A service node, is a wakunode2 (nwaku) node that runs service, similar to the Status prod fleet.
In this context, we can assume it provides Waku peer exchange, light push, filter and store services.
It uses discv5 for peer discovery. 
We may get into details to better reproduce fleet setup (boot and store nodes are separate,
several nwaku store nodes are connected to same posgres db), right now each store node has it's own db.

