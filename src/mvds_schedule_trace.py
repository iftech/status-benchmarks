"""Observe MVDS scheduling by explicit message/peer IDs and successful operations."""

from collections import Counter, defaultdict


SCHEDULE_STAGES = {
    "mvds_peer_online", "mvds_state_added", "mvds_state_add_failed",
    "mvds_state_scheduled", "mvds_peer_epoch_reset",
    "mvds_schedule_operation_completed", "mvds_schedule_operation_failed",
}
_STATE_STAGES = {"mvds_state_added", "mvds_state_add_failed", "mvds_state_scheduled", "mvds_peer_epoch_reset"}


class MVDSScheduleIndex:
    def __init__(self, events: list, collection: dict):
        self.nodes = {event["node"] for event in events if event.get("trace_version", 1) >= 4}
        self.collection = collection
        self.states = defaultdict(list)
        self.queued = defaultdict(list)
        self.online = defaultdict(list)
        self.notifications = defaultdict(Counter)
        self.reset_operations = Counter()
        self.reset_states = Counter()
        self.failures = Counter()
        for event in events:
            kind, node = event["kind"], event["node"]
            peer = event.get("recipient") or event.get("peer")
            if kind in _STATE_STAGES:
                for identifier in event.get("mvds_message_ids", []):
                    self.states[node, peer, identifier].append(event)
            elif kind == "mvds_message_queued":
                for identifier in event.get("mvds_message_ids", []):
                    self.queued[node, peer, identifier].append(event["at"])
            elif kind == "mvds_peer_online":
                self.online[node, peer].append(event)
                self.notifications[event.get("reason") or "unspecified"][node] += 1
            if kind == "mvds_schedule_operation_completed" and event.get("reason") == "peer_online":
                self.reset_operations[node] += 1
            if kind == "mvds_peer_epoch_reset":
                self.reset_states[node] += 1
            if kind in {"mvds_state_add_failed", "mvds_schedule_operation_failed"}:
                self.failures[node] += 1
        for items in self.states.values():
            items.sort(key=lambda event: (event.get("operation_completed_at") or event["at"], event["sequence"]))

    @staticmethod
    def _compact(event):
        return {key: event[key] for key in (
            "kind", "at", "sequence", "reason", "error", "schedule_operation_id", "state_type",
            "previous_send_count", "send_count", "previous_send_epoch", "send_epoch", "current_epoch",
            "epoch_duration_seconds", "operation_completed_at", "peer_event_time", "queue_depth", "queue_capacity",
        ) if event.get(key) is not None}

    def describe(self, row: dict, recipient: str) -> dict:
        sender = row["sender"]
        if sender not in self.nodes:
            return {"supported": False, "reason": "MVDS scheduling tracing requires message.trace version 4 on the sender."}
        identifiers = [message["mvds_message_id"] for message in row.get("ack_trace", {}).get("messages", [])]
        if not identifiers or not recipient:
            return {"supported": False, "reason": "No correlated MVDS message/recipient IDs."}
        first_dispatch = min((attempt["attempt_started_at"] for attempt in row["attempts"]), default=None)
        last_dispatch = max((attempt["attempt_started_at"] for attempt in row["attempts"]), default=first_dispatch)
        findings = set()
        messages = []
        for identifier in identifiers:
            key = sender, recipient, identifier
            items = self.states.get(key, [])
            queued_at = min(self.queued.get(key, []), default=None)
            resets = []
            for event in items:
                if event["kind"] != "mvds_peer_epoch_reset" or event.get("state_type") != "message":
                    continue
                previous, current = event.get("previous_send_epoch"), event.get("send_epoch")
                count, completed_at = event.get("previous_send_count"), event.get("operation_completed_at")
                if previous is None or current is None or count is None or completed_at is None:
                    continue
                change = self._compact(event)
                change["operation_completed_before_first_dispatch"] = (
                    completed_at <= first_dispatch if first_dispatch is not None else None
                )
                difference = current - previous
                change["deadline_shift_cycles"] = difference
                duration = event.get("epoch_duration_seconds")
                change["nominal_deadline_shift_seconds"] = difference * duration if duration is not None else None
                classification = None
                if difference:
                    phase = "first_send" if count == 0 else "retry"
                    classification = phase + ("_deadline_postponed" if difference > 0 else "_deadline_advanced")
                    findings.add(classification)
                change["classification"] = classification
                # Inspect the next selection, rather than searching later selections
                # for a coincidentally matching epoch. A successful UPDATE can affect
                # zero rows if an ACK concurrently removed the state.
                selection = min((item for item in items if item["kind"] == "mvds_state_scheduled"
                                 and item["at"] >= completed_at), key=lambda item: (item["at"], item["sequence"]), default=None)
                change["next_selection_matches_reset_deadline"] = (
                    selection.get("previous_send_epoch") == current
                    and selection.get("previous_send_count") == event.get("send_count")
                    if selection is not None else None
                )
                change["next_selection"] = self._compact(selection) if selection is not None else None
                resets.append(change)
            messages.append({
                "mvds_message_id": identifier, "queued_at": queued_at,
                "first_dispatch_at": first_dispatch,
                "enqueue_to_first_dispatch_seconds": max(0.0, first_dispatch - queued_at)
                if first_dispatch is not None and queued_at is not None else None,
                "state_events": [self._compact(event) for event in items], "resets": resets,
            })
        notifications = [self._compact(event) for event in self.online.get((sender, recipient), [])
                         if row["api_send_started_at"] - 1 <= event["at"] <= (last_dispatch or row["api_send_started_at"]) + 1]
        return {"supported": True,
                "diagnostic_streams_healthy": self.collection.get(sender, {}).get("status") == "collected",
                "findings": sorted(findings), "messages": messages,
                "peer_online_notifications": notifications,
                "notification_association": "same peer/time window; no shared per-notification operation ID"}

    def summary(self, rows: list) -> dict:
        return {
            "supported": bool(self.nodes), "nodes_with_schedule_tracing": sorted(self.nodes),
            "peer_online_notifications_by_reason": {reason: dict(sorted(nodes.items()))
                                                    for reason, nodes in sorted(self.notifications.items())},
            "reset_operations_by_node": dict(sorted(self.reset_operations.items())),
            "reset_state_observations_by_node": dict(sorted(self.reset_states.items())),
            "operation_failures_by_node": dict(sorted(self.failures.items())),
            "findings": dict(sorted(Counter(finding for row in rows
                                            for finding in row.get("mvds_schedule", {}).get("findings", [])).items())),
        }
