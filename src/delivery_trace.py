"""Correlate structured message.trace signals without inferring loss from an ACK timeout."""

import json
import logging
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from src.enums import SignalType
from src.filter_trace import FILTER_STAGES, FilterTraceIndex
from src.mvds_trace import MVDSAckIndex
from src.mvds_schedule_trace import MVDSScheduleIndex, SCHEDULE_STAGES

logger = logging.getLogger(__name__)
TRACE_SIGNAL = SignalType.MESSAGE_TRACE.value
_SIGNALS = ("envelope.sent", "envelope.expired", "message.delivered")


def _identifier(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return "0x" + value.lower().removeprefix("0x")


def _identifiers(value: Any) -> list[str]:
    values = value if isinstance(value, list) else [value]
    return [identifier for item in values if (identifier := _identifier(item))]


def _seconds(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if math.isfinite(value) and 0 <= value < 253_402_300_800 else None
    return None


def parse_trace_signal(node: str, signal: dict) -> dict[str, Any] | None:
    """Use the source timestamp, rather than the WebSocket emission timestamp."""
    try:
        fields = signal["event"]
        timestamp = _seconds(int(fields["timestampNs"]) / 1_000_000_000)
        kind = fields["stage"]
        if timestamp is None or kind not in FILTER_STAGES | SCHEDULE_STAGES | {
            "heartbeat", "attempt_started", "attempt_failed", "dispatch", "queue_entered",
            "publish_started", "publication_retry_scheduled", "publication_accepted", "publication_failed", "received",
            "processing", "processing_finished", "processing_failed",
            "mvds_message_queued", "mvds_payload_received", "mvds_packet_queued",
            "mvds_packet_dropped", "mvds_packet_skipped", "mvds_packet_consumed",
            "mvds_state_removed", "mvds_state_not_found", "mvds_state_remove_failed",
        }:
            return None
        event = {
            "node": node, "at": timestamp, "kind": kind,
            "observed_at": _seconds(signal.get("_received_at")),
            "sequence": int(fields["sequence"]), "dropped": int(fields.get("dropped", 0)),
            "trace_version": int(fields.get("version", 1)),
            "message_ids": _identifiers(fields.get("messageIds")),
            "mvds_message_ids": _identifiers(fields.get("mvdsMessageIds")),
            "ack_ids": _identifiers(fields.get("ackIds")),
            "peer": _identifier(fields.get("peer")),
            "envelope_hashes": _identifiers(fields.get("envelopeHashes")),
            "attempt_id": fields.get("attemptId"),
            "recipient": _identifier(fields.get("recipient")),
            "source": fields.get("source"), "error": fields.get("error"),
            "reason": fields.get("reason"),
            "reliability": fields.get("reliability"),
            "duration_seconds": fields.get("durationNs", 0) / 1_000_000_000,
            "publication_attempt": int(fields["publicationAttempt"]) if "publicationAttempt" in fields else None,
            "retry_delay_seconds": fields.get("retryDelayNs", 0) / 1_000_000_000,
            "queue_depth": int(fields.get("queueDepth", 0)) if "queueCapacity" in fields else None,
            "queue_capacity": int(fields["queueCapacity"]) if "queueCapacity" in fields else None,
        }
        if kind in SCHEDULE_STAGES:
            for source, target in (
                    ("scheduleOperationId", "schedule_operation_id"), ("stateType", "state_type")):
                if source in fields:
                    event[target] = str(fields[source])
            for source, target in (
                    ("previousSendCount", "previous_send_count"), ("sendCount", "send_count"),
                    ("previousSendEpoch", "previous_send_epoch"), ("sendEpoch", "send_epoch"),
                    ("currentEpoch", "current_epoch"), ("stateCount", "state_count"),
                    ("peerEventTime", "peer_event_time")):
                if source in fields:
                    event[target] = int(fields[source])
            if "epochDurationNs" in fields:
                event["epoch_duration_seconds"] = int(fields["epochDurationNs"]) / 1_000_000_000
            if "operationCompletedAtNs" in fields:
                event["operation_completed_at"] = _seconds(int(fields["operationCompletedAtNs"]) / 1_000_000_000)
        if kind in FILTER_STAGES:
            for source, target in (
                    ("localPeerId", "local_peer_id"), ("remotePeerId", "remote_peer_id"),
                    ("filterTraceId", "filter_trace_id"), ("streamId", "stream_id"),
                    ("pubsubTopic", "pubsub_topic"), ("contentTopic", "content_topic"),
                    ("subscriptionId", "subscription_id"), ("reason", "reason")):
                value = fields.get(source)
                if isinstance(value, str) and value:
                    event[target] = value
            event["matching_peers"] = [value for value in fields.get("matchingPeers", []) if isinstance(value, str)]
            if "rate" in fields:
                event["rate"] = float(fields["rate"])
            if "burst" in fields:
                event["burst"] = int(fields["burst"])
        return event
    except (ValueError, TypeError, AttributeError, KeyError, OverflowError):
        return None


def _signal_snapshot(status_nodes: dict) -> dict:
    result = {}
    for node, backend in status_nodes.items():
        events = []
        for kind in _SIGNALS:
            queue = backend.signal.signal_queues.get(kind)
            if queue is None:
                continue
            signals = queue.all_signals() if hasattr(queue, "all_signals") else queue.recent()
            for signal in signals:
                payload = signal.get("event") or {}
                events.append({
                    "type": kind, "at": _seconds(signal.get("timestamp")),
                    "message_ids": _identifiers(payload.get("ids", payload.get("messageID"))),
                    "error": payload.get("message"),
                })
        result[node] = events
    return result


def build_delivery_trace(
        sent_messages: list, expected_pairs: set, delivery_records: list,
        public_keys: dict, signals: dict, events: list, collection: dict,
        processing_delay_threshold: float = 10.0,
) -> dict:
    """Build evidence per message/recipient, preserving every observed dispatch.

    Missing signals are not proof of non-arrival. Repeated reliable dispatches are
    evidence of a retry candidate, not proof that an initial message was lost.
    """
    by_hash = defaultdict(list)
    dispatches = defaultdict(lambda: defaultdict(list))
    for event in sorted(events, key=lambda item: item["at"]):
        for envelope_hash in event["envelope_hashes"]:
            by_hash[event["node"], envelope_hash].append(event)
        if event.get("attempt_id"):
            for message_id in event["message_ids"]:
                dispatches[event["node"], message_id, event["recipient"]][event["attempt_id"]].append(event)
    filter_index = FilterTraceIndex(events, by_hash, collection)
    ack_index = MVDSAckIndex(events, by_hash, filter_index)
    schedule_index = MVDSScheduleIndex(events, collection)
    messages = {message.key: message for message in sent_messages}
    received = {(row["message_key"], row["receiver"]): row["received_at"] for row in delivery_records}
    rows = []
    for message_key, receiver in sorted(expected_pairs):
        message = messages[message_key]
        message_id = _identifier(getattr(message, "message_id", None))
        receiver_key = _identifier(public_keys.get(receiver))
        application_at = received.get((message_key, receiver))
        sender_signals = [event for event in signals.get(message.sender, [])
                          if message_id and message_id in event["message_ids"]]
        attempts = []
        observed_attempts = dispatches.get((message.sender, message_id, receiver_key), {})
        ordered_attempts = sorted(observed_attempts.items(), key=lambda pair: min(event["at"] for event in pair[1]))
        for index, (attempt_id, attempt_events) in enumerate(ordered_attempts):
            dispatch = next((event for event in attempt_events if event["kind"] == "dispatch"), None)
            envelope_hashes = list(dict.fromkeys(
                envelope_hash for event in attempt_events for envelope_hash in event["envelope_hashes"]
            ))
            started_at = min(event["at"] for event in attempt_events)
            envelopes = []
            for envelope_hash in envelope_hashes:
                sender_events = [event for event in by_hash[message.sender, envelope_hash]
                                 if event.get("attempt_id") == attempt_id]
                receiver_events = by_hash[receiver, envelope_hash]
                arrivals = [event for event in receiver_events if event["kind"] == "received"]
                accepted = [event["at"] for event in sender_events if event["kind"] == "publication_accepted"]
                errors = [event for event in sender_events if event["kind"] == "publication_failed"]
                starts = [event["at"] for event in sender_events if event["kind"] == "publish_started"]
                processing = [event["at"] for event in receiver_events if event["kind"] == "processing"]
                entered = [event["at"] for event in sender_events if event["kind"] == "queue_entered"]
                finished = [event for event in sender_events
                            if event["kind"] in ("publication_accepted", "publication_failed")]
                envelopes.append({
                    "hash": envelope_hash,
                    "queue_entered_at": min(entered, default=None),
                    "publish_started_at": min(starts, default=None),
                    "publication_accepted_at": min(accepted, default=None),
                    "publication_duration_seconds": finished[0]["duration_seconds"] if finished else None,
                    "publication_attempts": finished[0].get("publication_attempt") if finished else None,
                    "publication_retries": [
                        {"at": event["at"], "publication_attempt": event.get("publication_attempt"),
                         "delay_seconds": event.get("retry_delay_seconds"), "error": event["error"]}
                        for event in sender_events if event["kind"] == "publication_retry_scheduled"
                    ],
                    "publication_errors": [{"at": event["at"], "error": event["error"]} for event in errors],
                    "receiver_arrivals": [{"at": event["at"], "source": event["source"]} for event in arrivals],
                    "receiver_processing_started_at": min(processing, default=None),
                    "receiver_processing_errors": [{"at": event["at"], "error": event["error"]}
                                                   for event in receiver_events if event["kind"] == "processing_failed"],
                    "filter_trace": filter_index.describe(envelope_hash, receiver),
                })
            first_arrivals = [min((item["at"] for item in envelope["receiver_arrivals"]), default=None)
                              for envelope in envelopes]
            complete_at = max(first_arrivals) if first_arrivals and all(at is not None for at in first_arrivals) else None
            attempts.append({
                "observed_attempt": index + 1,
                "attempt_id": attempt_id,
                "reliability": attempt_events[0].get("reliability"),
                "attempt_started_at": started_at,
                "dispatch_at": dispatch["at"] if dispatch else None,
                "seconds_since_previous_attempt": started_at - attempts[-1]["attempt_started_at"] if attempts else None,
                "attempt_errors": [{"at": event["at"], "error": event["error"]}
                                   for event in attempt_events if event["kind"] == "attempt_failed"],
                "receiver_all_envelopes_arrived_at": complete_at,
                "envelopes": envelopes,
            })
        complete = [(index, attempt["receiver_all_envelopes_arrived_at"])
                    for index, attempt in enumerate(attempts)
                    if attempt["receiver_all_envelopes_arrived_at"] is not None]
        # Signal timestamps are floored to whole seconds. Allow that one-second
        # uncertainty before linking a raw arrival to application delivery.
        before_delivery = [(index, at) for index, at in complete
                           if application_at is not None and at < application_at + 1]
        first_complete = min(before_delivery, key=lambda pair: pair[1], default=None)
        processing_delay = max(0.0, application_at - first_complete[1]) if first_complete else None
        failed_attempts = sum(bool(attempt["attempt_errors"]) or any(envelope["publication_errors"] for envelope in attempt["envelopes"])
                              for attempt in attempts)
        failed_publications = sum(any(envelope["publication_errors"] for envelope in attempt["envelopes"])
                                  for attempt in attempts)
        published = any(attempt["envelopes"] and all(envelope["publication_accepted_at"] is not None
                            for envelope in attempt["envelopes"])
                        for attempt in attempts)
        signals_available = all(collection.get(node, {}).get("status") == "collected"
                             for node in (message.sender, receiver))
        if not message_id or not receiver_key:
            classification = "uncorrelated_message"
        elif application_at is not None:
            if processing_delay is not None and processing_delay >= processing_delay_threshold:
                classification = "receiver_processing_delay_observed"
            elif first_complete and first_complete[0] > 0:
                classification = "later_attempt_delivery_observed"
            elif first_complete:
                classification = "delivery_observed"
            else:
                classification = "application_delivery_observed_trace_incomplete"
        elif complete:
            classification = "receiver_envelopes_observed_application_missing"
        elif failed_publications:
            classification = "publication_failure_observed"
        elif failed_attempts:
            classification = "send_attempt_failure_observed"
        elif published and signals_available:
            classification = "publication_to_receiver_gap"
        else:
            classification = "inconclusive"
        rows.append({
            "message_key": message_key, "message_id": message_id,
            "sender": message.sender, "receiver": receiver,
            "api_send_started_at": message.sent_at,
            "first_application_received_at": application_at,
            "classification": classification,
            "repeated_dispatch_observed": len(attempts) > 1,
            "mvds_retry_candidate": sum(attempt["reliability"] == "mvds" for attempt in attempts) > 1,
            "failed_send_attempts_observed": failed_attempts,
            "failed_publication_attempts_observed": failed_publications,
            "diagnostic_streams_healthy": signals_available,
            "receiver_processing_delay_lower_bound_seconds": processing_delay,
            "sender_signals": sender_signals,
            "sender_signal_scope": "message_id_only; group recipient and attempt are not present in these signals",
            "attempts": attempts,
        })
    for row in rows:
        row["ack_trace"] = ack_index.describe(row, _identifier(public_keys.get(row["sender"])),
                                             _identifier(public_keys.get(row["receiver"])))
        row["mvds_schedule"] = schedule_index.describe(row, _identifier(public_keys.get(row["receiver"])))
    return {
        "summary": dict(sorted(Counter(row["classification"] for row in rows).items())),
        "collection": collection,
        "mvds_ack_summary": ack_index.summary(rows),
        "mvds_schedule_summary": schedule_index.summary(rows),
        "filter_summary": filter_index.summary(),
        "publication_retry_summary": {
            "scheduled_retries": sum(event["kind"] == "publication_retry_scheduled" for event in events),
            "recovered_publications": sum(event["kind"] == "publication_accepted"
                                          and (event.get("publication_attempt") or 0) > 1 for event in events),
            "failed_publications_after_retry": sum(event["kind"] == "publication_failed"
                                                  and (event.get("publication_attempt") or 0) > 1 for event in events),
        },
        "processing_delay_threshold_seconds": processing_delay_threshold,
        "limitations": [
            "Publication acceptance does not establish a completed network stream write or recipient delivery.",
            "Absent receiver signals do not prove non-arrival; consult per-node heartbeat, sequence-gap, and dropped-event metadata.",
            "Repeated reliable dispatches identify retry candidates; elapsed time alone does not establish a retry.",
            "Attempt IDs identify individual sends; observed ordinals are not the internal MVDS SendCount.",
            "Node and benchmark clocks must be synchronized for cross-node timings; standard application signals have one-second precision.",
            "Receiver completion means every envelope of one observed dispatch arrived, including segmented payloads.",
            "ACKs are correlated only by explicit MVDS IDs, peer identity, and envelope hashes; timing does not identify ACK contents.",
            "MVDS queue consumption precedes ACK application; state-removed signals observe the actual persistence operation, not UI delivery confirmation.",
            "Filter push writes do not confirm client admission. Pre-decode rejection candidates match only peer identities and time, not an envelope hash.",
            "Scheduling state events observe callbacks from successful persistence operations; concurrent ACK removal can make an UPDATE affect zero rows. Inspect the next selection for the reset deadline.",
            "Epoch durations describe the configured minimum loop interval; nominal deadline shifts are not wall-clock delivery predictions.",
        ],
        "deliveries": rows,
        "uncorrelated_api_attempts": [
            {"message_key": message.key, "sender": message.sender,
             "api_status": message.send_status, "error": message.send_error}
            for message in sent_messages if not getattr(message, "message_id", None)
        ],
    }


async def collect_delivery_trace(
        sent_messages: list, status_nodes: dict, expected_pairs: set,
        delivery_records: list, since: float, until: float, output_dir: str,
        prefix: str, namespace: str = "status-go-test",
) -> dict:
    """Snapshot the already-open /signals connections; no Kubernetes log access."""
    signals = _signal_snapshot(status_nodes)
    public_keys = {node: backend.public_key for node, backend in status_nodes.items()}
    events = []
    collection = {}
    for node, backend in status_nodes.items():
        queue = backend.signal.signal_queues.get(TRACE_SIGNAL)
        raw_signals = queue.all_signals() if queue is not None else []
        parsed = [parse_trace_signal(node, signal) for signal in raw_signals]
        node_events = [event for event in parsed if event is not None
                       and since <= (event["observed_at"] if event["observed_at"] is not None else event["at"]) <= until]
        events.extend(node_events)
        sequences = sorted({event["sequence"] for event in node_events})
        gaps = sum(max(0, current - previous - 1) for previous, current in zip(sequences, sequences[1:]))
        dropped = [event["dropped"] for event in node_events]
        newly_dropped = max(dropped) - min(dropped) if dropped else 0
        last_observed = max((event["observed_at"] or event["at"] for event in node_events), default=None)
        stale = last_observed is not None and until - last_observed > 5
        listener = getattr(backend.signal, "listener_task", None)
        disconnected = listener is not None and listener.done()
        status = "collected" if node_events else "unavailable"
        invalid_signals = sum(event is None for event in parsed)
        if node_events and (gaps or newly_dropped or stale or disconnected or invalid_signals):
            status = "incomplete"
        collection[node] = {
            "status": status, "event_count": len(node_events),
            "invalid_signals": invalid_signals,
            "missing_sequences": gaps, "dropped_during_observed_window": newly_dropped,
            "dropped_total": max(dropped, default=0), "last_signal_observed_at": last_observed,
            "stale_stream": stale, "listener_stopped": disconnected,
        }
        if not node_events:
            collection[node]["error"] = "No message.trace signals: deploy the trace-enabled status-go image with STATUS_MESSAGE_TRACE=1."
    report = build_delivery_trace(
        sent_messages, expected_pairs, delivery_records, public_keys, signals, events,
        collection,
    )
    report["window"] = {"since": since, "until": until, "namespace": namespace, "source": "/signals message.trace"}
    try:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        events_path = destination / f"{prefix}_events.jsonl"
        report_path = destination / f"{prefix}_trace.json"
        report["events_file"] = str(events_path.resolve())
        report["report_file"] = str(report_path.resolve())
        with events_path.open("w", encoding="utf-8") as output:
            for event in sorted(events, key=lambda item: (item["at"], item["node"])):
                output.write(json.dumps(event) + "\n")
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as error:
        report["artifact_error"] = str(error)
        logger.warning("Cannot save delivery trace: %s", error)
    return report
