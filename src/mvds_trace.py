"""Correlate explicit MVDS IDs and ACK IDs; never infer ACK contents from timing."""

from collections import Counter, defaultdict


_PACKET_STAGES = {
    "mvds_payload_received", "mvds_packet_queued", "mvds_packet_dropped",
    "mvds_packet_skipped", "mvds_packet_consumed",
}
_STATE_STAGES = {"mvds_state_removed", "mvds_state_not_found", "mvds_state_remove_failed"}


class MVDSAckIndex:
    def __init__(self, events: list, by_hash: dict, filter_index=None):
        self.by_hash = by_hash
        self.filter_index = filter_index
        self.by_ack = defaultdict(list)
        self.by_mvds = defaultdict(list)
        self.message_ids = defaultdict(set)
        self.ack_nodes = {event["node"] for event in events if event.get("trace_version", 1) >= 2}
        self.supported = bool(self.ack_nodes)
        self.queue_drops = Counter()
        self.queue_skips = Counter()
        self.state_errors = Counter()
        for event in events:
            remote = event.get("recipient") or event.get("peer")
            node = event["node"]
            mvds_ids = event.get("mvds_message_ids", [])
            ack_ids = event.get("ack_ids", [])
            for identifier in ack_ids:
                self.by_ack[node, remote, identifier].append(event)
            for identifier in mvds_ids:
                self.by_mvds[node, remote, identifier].append(event)
            app_ids = event.get("message_ids", [])
            if event["kind"] in {"mvds_message_queued", "attempt_started", "dispatch"} and len(app_ids) == len(mvds_ids):
                for app_id, mvds_id in zip(app_ids, mvds_ids):
                    self.message_ids[node, app_id, event.get("recipient")].add(mvds_id)
            if event["kind"] == "mvds_packet_dropped":
                self.queue_drops[node] += 1
            elif event["kind"] == "mvds_packet_skipped":
                self.queue_skips[node] += 1
            elif event["kind"] == "mvds_state_remove_failed":
                self.state_errors[node] += 1

    def _publications(self, outgoing: list, recipient: str, cutoff: float) -> list:
        attempts = defaultdict(list)
        for event in outgoing:
            if event["at"] <= cutoff and event.get("attempt_id"):
                attempts[event["attempt_id"]].append(event)
        result = []
        for attempt_id, events in attempts.items():
            hashes = sorted({value for event in events for value in event["envelope_hashes"]})
            envelopes = []
            for envelope_hash in hashes:
                publications = [event["at"] for event in events if event["kind"] == "publication_accepted"
                                and envelope_hash in event["envelope_hashes"]]
                arrivals = [event["at"] for event in self.by_hash.get((recipient, envelope_hash), [])
                            if event["kind"] == "received" and event["at"] <= cutoff]
                envelope = {"hash": envelope_hash,
                            "publication_accepted_at": min(publications, default=None),
                            "received_at": min(arrivals, default=None)}
                if self.filter_index is not None:
                    envelope["filter_trace"] = self.filter_index.describe(envelope_hash, recipient, cutoff)
                envelopes.append(envelope)
            dispatched = any(event["kind"] == "dispatch" for event in events)
            result.append({"attempt_id": attempt_id, "started_at": min(event["at"] for event in events),
                           "envelopes": envelopes,
                           "all_envelopes_published": dispatched and bool(envelopes) and all(env["publication_accepted_at"] is not None for env in envelopes),
                           "all_envelopes_arrived": dispatched and bool(envelopes) and all(env["received_at"] is not None for env in envelopes)})
        return sorted(result, key=lambda attempt: attempt["started_at"])

    def _status(self, outgoing: list, incoming: list, data: list, state: list,
                recipient: str, cutoff: float) -> str:
        def kinds(items):
            return {event["kind"] for event in items if event["at"] <= cutoff}

        state_kinds, incoming_kinds, data_kinds = kinds(state), kinds(incoming), kinds(data)
        if "mvds_state_removed" in state_kinds:
            return "pending_state_removed"
        if "mvds_state_remove_failed" in state_kinds:
            return "ack_state_remove_failed"
        if "mvds_state_not_found" in state_kinds and "mvds_payload_received" in incoming_kinds:
            return "ack_state_not_found"
        for stage, status in (
                ("mvds_packet_consumed", "ack_consumed_state_update_unobserved"),
                ("mvds_packet_dropped", "ack_dropped_at_sender"),
                ("mvds_packet_skipped", "ack_skipped_at_sender"),
                ("mvds_packet_queued", "ack_queued_not_consumed"),
                ("mvds_payload_received", "ack_decoded_not_queued")):
            if stage in incoming_kinds:
                return status
        publications = self._publications(outgoing, recipient, cutoff)
        if any(attempt["all_envelopes_arrived"] for attempt in publications):
            return "ack_arrived_not_decoded"
        if any(env["received_at"] is not None for attempt in publications for env in attempt["envelopes"]):
            return "ack_envelopes_partially_arrived"
        if any(attempt["all_envelopes_published"] for attempt in publications):
            return "ack_publication_to_sender_gap"
        outgoing_kinds = kinds(outgoing)
        if "publication_failed" in outgoing_kinds or "attempt_failed" in outgoing_kinds:
            return "ack_publication_or_dispatch_failed"
        if any(env["publication_accepted_at"] is not None for attempt in publications for env in attempt["envelopes"]):
            return "ack_publication_incomplete"
        if outgoing_kinds:
            return "ack_dispatch_publication_unobserved"
        if "mvds_packet_consumed" in data_kinds:
            return "receiver_consumed_message_ack_dispatch_unobserved"
        if "mvds_packet_dropped" in data_kinds:
            return "receiver_message_packet_dropped"
        if "mvds_packet_skipped" in data_kinds:
            return "receiver_message_packet_skipped"
        if "mvds_packet_queued" in data_kinds:
            return "receiver_message_queued_not_consumed"
        if "mvds_payload_received" in data_kinds:
            return "receiver_message_decoded_not_queued"
        return "receiver_mvds_payload_unobserved"

    def describe(self, row: dict, sender_key: str, receiver_key: str) -> dict:
        unsupported = sorted({row["sender"], row["receiver"]} - self.ack_nodes)
        if unsupported:
            return {"supported": False, "unsupported_nodes": unsupported,
                    "reason": "ACK tracing requires message.trace version 2 on both endpoints."}
        identifiers = sorted(self.message_ids.get((row["sender"], row["message_id"], receiver_key), ()))
        if not identifiers:
            return {"supported": False, "reason": "No correlated MVDS IDs; this send may be non-MVDS or its trace is incomplete."}
        retries = [attempt["attempt_started_at"] for attempt in row["attempts"][1:]]
        details = []
        first_retry_statuses = set()
        for identifier in identifiers:
            outgoing = [event for event in self.by_ack.get((row["receiver"], sender_key, identifier), [])
                        if event.get("recipient") == sender_key]
            incoming = [event for event in self.by_ack.get((row["sender"], receiver_key, identifier), [])
                        if event["kind"] in _PACKET_STAGES]
            data = [event for event in self.by_mvds.get((row["receiver"], sender_key, identifier), [])
                    if event["kind"] in _PACKET_STAGES]
            state = [event for event in self.by_mvds.get((row["sender"], receiver_key, identifier), [])
                     if event["kind"] in _STATE_STAGES]
            before_retries = []
            for at in retries:
                status = self._status(outgoing, incoming, data, state, row["sender"], at)
                if not before_retries:
                    first_retry_statuses.add(status)
                before_retries.append({"retry_started_at": at, "status": status,
                                       "ack_publications": self._publications(outgoing, row["sender"], at)})
            timeline = sorted(outgoing + incoming + data + state, key=lambda event: (event["at"], event["sequence"]))
            details.append({
                "mvds_message_id": identifier,
                "status_at_observation_end": self._status(outgoing, incoming, data, state, row["sender"], float("inf")),
                "before_retries": before_retries,
                "ack_publications": self._publications(outgoing, row["sender"], float("inf")),
                "events": [{key: event[key] for key in ("node", "kind", "at", "envelope_hashes", "error",
                                                       "queue_depth", "queue_capacity") if key in event} for event in timeline],
            })
        first_status = next(iter(first_retry_statuses)) if len(first_retry_statuses) == 1 else "multiple_mvds_ids"
        if first_status == "pending_state_removed":
            first_status = "retry_after_pending_state_removed"
        healthy = row.get("diagnostic_streams_healthy", True)
        return {"supported": True, "messages": details,
                "diagnostic_streams_healthy": healthy,
                "observed_status_before_first_retry": first_status if retries else None,
                "status_before_first_retry": (first_status if healthy else "ack_trace_incomplete") if retries else None}

    def summary(self, rows: list) -> dict:
        repeated = [row for row in rows if row["mvds_retry_candidate"]]
        return {"supported": self.supported, "nodes_with_ack_tracing": sorted(self.ack_nodes),
                "repeated_message_recipient_pairs": len(repeated),
                "retry_findings": dict(sorted(Counter(
                    row["ack_trace"].get("status_before_first_retry", "ack_trace_unavailable")
                    for row in repeated).items())),
                "queue_drops_by_node": dict(sorted(self.queue_drops.items())),
                "queue_skips_by_node": dict(sorted(self.queue_skips.items())),
                "state_remove_errors_by_node": dict(sorted(self.state_errors.items()))}
