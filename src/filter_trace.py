"""Locate Filter gaps while keeping early peer/time associations provisional."""

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict


FILTER_STAGES = {
    "filter_relay_routed", "filter_push_started", "filter_push_written", "filter_push_failed",
    "filter_client_push_started", "filter_client_admitted", "filter_client_rejected",
    "filter_client_accepted", "filter_subscription_queued", "filter_queue_rejected",
    "filter_multiplex_dequeued", "filter_multiplex_queued", "filter_api_handoff",
}


class FilterTraceIndex:
    def __init__(self, events: list, by_hash: dict, collection: dict):
        self.by_hash = by_hash
        self.collection = collection
        self.nodes = {event["node"] for event in events if event.get("trace_version", 1) >= 3}
        self.relay_nodes = {event["node"] for event in events
                            if event["kind"] == "filter_relay_routed" or event.get("source") == "Relay"}
        self.identities = defaultdict(set)
        self.envelopes = defaultdict(list)
        self.early_rejections = defaultdict(list)
        self.rejections = defaultdict(Counter)
        self.push_failures = Counter()
        self.cache = {}
        trace_hashes = defaultdict(set)
        selected = [event for event in events if event["kind"] in FILTER_STAGES]
        for event in selected:
            if event.get("local_peer_id"):
                self.identities[event["node"]].add(event["local_peer_id"])
            if event.get("filter_trace_id"):
                trace_hashes[event["node"], event["filter_trace_id"]].update(event["envelope_hashes"])
        for event in selected:
            hashes = set(event["envelope_hashes"])
            if event.get("filter_trace_id"):
                hashes.update(trace_hashes[event["node"], event["filter_trace_id"]])
            for envelope_hash in hashes:
                self.envelopes[envelope_hash].append(event)
            if event["kind"] in {"filter_client_rejected", "filter_queue_rejected"}:
                self.rejections[event.get("reason") or "unspecified"][event["node"]] += 1
                if not hashes:
                    self.early_rejections[event["node"], event.get("remote_peer_id")].append(event)
            if event["kind"] == "filter_push_failed":
                self.push_failures[event["node"]] += 1
        self.early_times = {}
        for key, items in self.early_rejections.items():
            items.sort(key=lambda event: event["at"])
            self.early_times[key] = [event["at"] for event in items]

    @staticmethod
    def _compact(event):
        return {key: event[key] for key in (
            "node", "kind", "at", "sequence", "local_peer_id", "remote_peer_id", "filter_trace_id",
            "stream_id", "reason", "error", "rate", "burst", "subscription_id", "queue_depth", "queue_capacity",
        ) if event.get(key) is not None}

    def describe(self, envelope_hash: str, receiver: str, cutoff: float = float("inf")) -> dict:
        key = envelope_hash, receiver, cutoff
        if key not in self.cache:
            self.cache[key] = self._describe(envelope_hash, receiver, cutoff)
        return self.cache[key]

    def _describe(self, envelope_hash, receiver, cutoff):
        if receiver not in self.nodes:
            return {"supported": False, "reason": "Filter tracing requires message.trace version 3."}
        if receiver in self.relay_nodes:
            return {"supported": True, "applicable": False, "status": "relay_receiver"}
        if any(event["kind"] == "received" and event["at"] <= cutoff
               for event in self.by_hash.get((receiver, envelope_hash), [])):
            return {"supported": True, "applicable": True, "status": "receive_hook_observed"}
        identities = self.identities.get(receiver, set())
        if len(identities) != 1:
            return {"supported": False, "reason": "Target transport-peer identity is unavailable or ambiguous."}
        target_peer = next(iter(identities))
        events = [event for event in self.envelopes.get(envelope_hash, []) if event["at"] <= cutoff]
        routes = [event for event in events if event["kind"] == "filter_relay_routed"]
        pushes = [event for event in events if event["kind"] in {
            "filter_push_started", "filter_push_written", "filter_push_failed"}
            and event.get("remote_peer_id") == target_peer]
        client = [event for event in events if event["node"] == receiver]
        kinds = {event["kind"] for event in client}
        candidates = {}
        for push in pushes:
            if push["kind"] != "filter_push_started" or not push.get("local_peer_id"):
                continue
            ends = [event["at"] for event in pushes if event.get("filter_trace_id") == push.get("filter_trace_id")
                    and event["node"] == push["node"] and event["kind"] != "filter_push_started"]
            # This is only a clock-tolerant peer/time association, never an ACK/hash identity match.
            start, end = push["at"] - 1, min(cutoff, max(ends, default=push["at"]) + 1)
            remote_key = receiver, push["local_peer_id"]
            times = self.early_times.get(remote_key, [])
            for event in self.early_rejections.get(remote_key, [])[bisect_left(times, start):bisect_right(times, end)]:
                candidates[event["node"], event["sequence"]] = event
        exact_rejections = [event for event in client if event["kind"] in {"filter_client_rejected", "filter_queue_rejected"}]
        matched = any(target_peer in event.get("matching_peers", []) for event in routes)
        routed_nodes = {event["node"] for event in routes}
        capable = bool(self.relay_nodes) and all(node in self.nodes for node in self.collection)
        complete_routes = capable and self.relay_nodes <= routed_nodes
        if exact_rejections:
            rejected = max(exact_rejections, key=lambda event: event["at"])
            status = "client_rejected: " + (rejected.get("reason") or "unspecified")
        elif "filter_api_handoff" in kinds:
            status = "api_handoff_receive_hook_unobserved"
        elif "filter_multiplex_queued" in kinds:
            status = "multiplex_queued_receive_hook_unobserved"
        elif "filter_multiplex_dequeued" in kinds:
            status = "multiplex_handoff_unobserved"
        elif "filter_subscription_queued" in kinds:
            status = "subscription_queued_multiplex_unobserved"
        elif "filter_client_accepted" in kinds:
            status = "client_accepted_subscription_handoff_unobserved"
        elif complete_routes and not matched:
            status = "no_matching_relay_subscription_observed"
        elif candidates:
            status = "early_rejection_candidates"
        elif any(event["kind"] == "filter_push_written" for event in pushes):
            status = "relay_push_written_client_intake_unobserved"
        elif any(event["kind"] == "filter_push_failed" for event in pushes):
            status = "relay_push_failed"
        elif matched:
            status = "matching_subscription_push_result_unobserved"
        elif routes:
            status = "relay_routing_evidence_incomplete"
        else:
            status = "relay_filter_routing_unobserved"
        evidence_nodes = {receiver} | {event["node"] for event in routes + pushes}
        healthy = all(self.collection.get(node, {}).get("status") == "collected" for node in evidence_nodes)
        return {"supported": True, "applicable": True,
                "status": status if healthy else "filter_trace_incomplete", "observed_status": status,
                "target_peer_id": target_peer, "diagnostic_streams_healthy": healthy,
                "routing_coverage_complete": complete_routes,
                "relay_routes": [{"node": event["node"], "at": event["at"],
                                  "matched_target": target_peer in event.get("matching_peers", [])} for event in routes],
                "relay_pushes": [self._compact(event) for event in sorted(pushes, key=lambda event: event["at"])],
                "client_events": [self._compact(event) for event in sorted(client, key=lambda event: event["at"])],
                "early_rejection_candidates": [self._compact(event) for event in sorted(candidates.values(), key=lambda event: event["at"])],
                "early_rejection_attribution": "peer_and_time_only; envelope hash is unavailable before decoding"}

    def summary(self):
        return {"supported": bool(self.nodes), "nodes_with_filter_tracing": sorted(self.nodes),
                "client_rejections_by_reason": {reason: dict(sorted(nodes.items())) for reason, nodes in sorted(self.rejections.items())},
                "push_failures_by_relay": dict(sorted(self.push_failures.items())),
                "unattributed_early_rejections": sum(len(items) for items in self.early_rejections.values())}
