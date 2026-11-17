# Python Imports
import asyncio
import logging
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

# Project Imports
import src.logger
from src.enums import SignalType
from src.delivery_artifacts import save_delivery_metrics
from src.status_backend import StatusBackend

logger = logging.getLogger(__name__)

DeliveryPair = tuple[str, str]
DeliveryPairResolver = Callable[[Any, str], DeliveryPair | None]
ReceiverTypeResolver = Callable[[Any, str], str]


@dataclass(frozen=True)
class DeliveryScan:
    delivery_counts: dict[DeliveryPair, int]
    first_delivery_at: dict[DeliveryPair, float]
    first_deliveries_by_receiver_sender: dict[tuple[str, str], list[tuple[float, int, int, str]]]


@dataclass
class SendCadence:
    """Schedule sequential API calls on fixed monotonic deadlines."""

    interval: float
    start_at: float
    missed_send_slots: int = field(default=0, init=False)
    max_deadline_lateness_seconds: float = field(default=0.0, init=False)
    _deadline: float = field(init=False, repr=False)
    _last_started_at: float | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("Send interval must be a finite positive number")
        if not math.isfinite(self.start_at):
            raise ValueError("Schedule start must be finite")
        self._deadline = self.start_at

    def _skip_before(self, earliest: float) -> None:
        skipped = max(0, math.ceil((earliest - self._deadline) / self.interval))
        self._deadline += skipped * self.interval
        self.missed_send_slots += skipped

    async def wait_for_next(self) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        lateness = max(0.0, now - self._deadline)
        self.max_deadline_lateness_seconds = max(self.max_deadline_lateness_seconds, lateness)
        # Allow ordinary timer jitter without skipping an entire send slot.
        tolerance = min(0.05, self.interval * 0.05)
        if self._last_started_at is not None and lateness > tolerance:
            self._skip_before(now)
        if self._deadline > now:
            await asyncio.sleep(self._deadline - now)
        now = loop.time()
        if self._last_started_at is not None:
            earliest = self._last_started_at + self.interval - tolerance
            if now < earliest:
                # A late wake-up must not cause the following sends to bunch up.
                self._skip_before(earliest)
                if self._deadline > now:
                    await asyncio.sleep(self._deadline - now)
                now = loop.time()
        self.max_deadline_lateness_seconds = max(
            self.max_deadline_lateness_seconds, max(0.0, now - self._deadline),
        )
        self._last_started_at = now
        self._deadline += self.interval

    def summary(self) -> dict[str, int | float]:
        return {
            "missed_send_slots": self.missed_send_slots,
            "max_deadline_lateness_seconds": self.max_deadline_lateness_seconds,
        }


def build_send_cadence_summary(by_node: dict[str, dict[str, int | float]]) -> dict[str, Any]:
    return {
        "clock": "monotonic",
        "overrun_policy": "skip missed time slots; preserve target message count",
        "missed_send_slots": sum(node["missed_send_slots"] for node in by_node.values()),
        "max_deadline_lateness_seconds": max(
            (node["max_deadline_lateness_seconds"] for node in by_node.values()), default=0.0,
        ),
        "by_node": by_node,
    }


def node_type(node_name: str) -> str:
    if "light" in node_name:
        return "light"
    return "relay"


def calculate_stagger_between_senders(
        delay_between_messages: float,
        sender_count: int,
        sender_label: str = "senders",
) -> float:
    if sender_count <= 0:
        raise ValueError(f"Cannot calculate sender stagger without {sender_label}")
    return delay_between_messages / sender_count


def benchmark_message_text(run_id: str, node_name: str, sequence: int) -> str:
    """Build a 100-character message while preserving its complete identifiers."""
    prefix = f"MVDS-TEST | {node_name} | Seq-{sequence:06d} | Run-{run_id} | "
    if len(prefix) > 100:
        raise ValueError("Benchmark message identifiers exceed the 100-character limit")
    body = "The quick brown fox jumps over the lazy dog. Reliability test message."
    return (prefix + body)[:100]


def private_message_text(run_id: str, node_name: str, sequence: int) -> str:
    return benchmark_message_text(run_id, node_name, sequence)


def extract_first_message_id(response: dict) -> str | None:
    messages = response.get("result", {}).get("messages", [])
    if not messages:
        return None
    return messages[0].get("id")


def expected_group_delivery_pairs(sent_messages: list[Any], group_members: list[str]) -> set[DeliveryPair]:
    """Expand only sends accepted by the node API to their expected recipients."""
    return {
        (message.key, receiver)
        for message in sent_messages
        if message.send_status == "sent"
        for receiver in group_members
        if receiver != message.sender
    }


def expected_private_delivery_pairs(sent_messages: list[Any]) -> set[DeliveryPair]:
    """Count an expected delivery only when the node API accepted the send."""
    return {
        (message.key, message.receiver)
        for message in sent_messages
        if message.send_status == "sent"
    }


def group_delivery_pair(message: Any, receiver: str) -> DeliveryPair | None:
    if receiver == message.sender:
        return None
    return message.key, receiver


def private_delivery_pair(message: Any, receiver: str) -> DeliveryPair | None:
    if receiver != message.receiver:
        return None
    return message.key, receiver


def group_receiver_type(_: Any, receiver: str) -> str:
    return node_type(receiver)


def private_receiver_type(message: Any, _: str) -> str:
    return message.receiver_type


def _normalize_signal_timestamp(timestamp: float | int) -> float:
    """Read the envelope's Unix seconds, not the message payload's millisecond timestamp.

    status-go/internal/signal/signals.go:newEnvelope uses time.Now().Unix().
    Reject values outside years 1970-9999 rather than guessing a different unit.
    """
    if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not 0 <= timestamp < 253_402_300_800
    ):
        raise ValueError(f"Expected messages.new envelope timestamp in Unix seconds, got {timestamp!r}")
    return float(timestamp)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None

    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percentile))
    return ordered[index]


def _latency_summary(latencies: list[float]) -> dict[str, float | int | None]:
    if not latencies:
        return {
            "count": 0,
            "min_seconds": None,
            "max_seconds": None,
            "avg_seconds": None,
            "p50_seconds": None,
            "p95_seconds": None,
        }

    return {
        "count": len(latencies),
        "min_seconds": min(latencies),
        "max_seconds": max(latencies),
        "avg_seconds": statistics.fmean(latencies),
        "p50_seconds": statistics.median(latencies),
        "p95_seconds": _percentile(latencies, 0.95),
    }


def _empty_delivery_bucket() -> dict[str, Any]:
    return {
        "expected_message_deliveries": 0,
        "successful_deliveries": 0,
        "lost_deliveries": 0,
        "duplicate_deliveries": 0,
        "out_of_order_deliveries": 0,
        "delivery_reliability_percent": None,
        "message_delivery_latency": _latency_summary([]),
    }


def _latency_distribution(delivery_records: list[dict[str, Any]]) -> dict[str, Any]:
    buckets = {
        "0_to_10_seconds": 0,
        "10_to_60_seconds": 0,
        "60_plus_seconds": 0,
    }

    for record in delivery_records:
        latency = record["latency_seconds"]
        if latency <= 10:
            buckets["0_to_10_seconds"] += 1
        elif latency < 60:
            buckets["10_to_60_seconds"] += 1
        else:
            buckets["60_plus_seconds"] += 1

    total = len(delivery_records)
    percentages = {
        bucket: count / total * 100 if total else 0.0
        for bucket, count in buckets.items()
    }

    return {
        "buckets": buckets,
        "percentages": percentages,
        "mvds_resend_pattern_candidate": buckets["60_plus_seconds"] > 0 and buckets["10_to_60_seconds"] == 0,
    }


def _index_messages_by_text(sent_messages: list[Any]) -> dict[str, Any]:
    """Fail explicitly if the queue's text-only matching would be ambiguous."""
    sent_by_text: dict[str, Any] = {}
    for message in sent_messages:
        if message.text in sent_by_text:
            previous = sent_by_text[message.text]
            raise ValueError(
                "Delivery matching requires unique message text: "
                f"messages {previous.key!r} and {message.key!r} share the same text"
            )
        sent_by_text[message.text] = message
    return sent_by_text


def _successful_delivery_pairs(
        sent_messages: list[Any],
        status_nodes: dict[str, StatusBackend],
        receivers: list[str],
        expected_pairs: set[DeliveryPair],
        delivery_pair_resolver: DeliveryPairResolver,
) -> set[DeliveryPair]:
    sent_by_text = _index_messages_by_text(sent_messages)
    successful_pairs: set[DeliveryPair] = set()

    for receiver in receivers:
        queue = status_nodes[receiver].signal.signal_queues[SignalType.MESSAGES_NEW.value]
        for _, text in queue.messages:
            sent_message = sent_by_text.get(text)
            if sent_message is None:
                continue

            pair = delivery_pair_resolver(sent_message, receiver)
            if pair in expected_pairs:
                successful_pairs.add(pair)

    return successful_pairs


def _scan_deliveries(
        sent_messages: list[Any],
        status_nodes: dict[str, StatusBackend],
        receivers: list[str],
        expected_pairs: set[DeliveryPair],
        delivery_pair_resolver: DeliveryPairResolver,
) -> DeliveryScan:
    sent_by_text = _index_messages_by_text(sent_messages)
    delivery_counts: dict[DeliveryPair, int] = defaultdict(int)
    first_delivery_at: dict[DeliveryPair, float] = {}
    first_deliveries_by_receiver_sender: dict[tuple[str, str], list[tuple[float, int, int, str]]] = defaultdict(list)

    for receiver in receivers:
        queue = status_nodes[receiver].signal.signal_queues[SignalType.MESSAGES_NEW.value]
        for arrival_index, (timestamp, text) in enumerate(queue.messages):
            sent_message = sent_by_text.get(text)
            if sent_message is None:
                continue

            pair = delivery_pair_resolver(sent_message, receiver)
            if pair not in expected_pairs:
                continue

            received_at = _normalize_signal_timestamp(timestamp)
            delivery_counts[pair] += 1
            if pair not in first_delivery_at:
                first_delivery_at[pair] = received_at
                first_deliveries_by_receiver_sender[(receiver, sent_message.sender)].append(
                    (received_at, arrival_index, sent_message.sequence, sent_message.key)
                )

    return DeliveryScan(
        delivery_counts=dict(delivery_counts),
        first_delivery_at=first_delivery_at,
        first_deliveries_by_receiver_sender=dict(first_deliveries_by_receiver_sender),
    )


def _delivery_records(
        successful_pairs: set[DeliveryPair],
        sent_messages_by_key: dict[str, Any],
        first_delivery_at: dict[DeliveryPair, float],
        receiver_type_resolver: ReceiverTypeResolver,
        delivery_counts: dict[DeliveryPair, int],
        out_of_order_pairs: set[DeliveryPair],
) -> list[dict[str, Any]]:
    records = []

    for message_key, receiver in successful_pairs:
        sent_message = sent_messages_by_key[message_key]
        received_at = first_delivery_at[(message_key, receiver)]
        records.append({
            "message_key": message_key,
            "sender": sent_message.sender,
            "sender_type": sent_message.sender_type,
            "receiver": receiver,
            "receiver_type": receiver_type_resolver(sent_message, receiver),
            "sequence": sent_message.sequence,
            "sent_at": sent_message.sent_at,
            "received_at": received_at,
            "latency_seconds": max(0.0, received_at - sent_message.sent_at),
            "duplicates": max(0, delivery_counts[(message_key, receiver)] - 1),
            "out_of_order": (message_key, receiver) in out_of_order_pairs,
        })

    records.sort(key=lambda record: record["latency_seconds"])
    return records


def _out_of_order_pairs(
        first_deliveries_by_receiver_sender: dict[tuple[str, str], list[tuple[float, int, int, str]]],
) -> set[DeliveryPair]:
    """Count inversions distinguishable by application emission timestamp.

    messages.new arrays have no ordering contract, and their timestamps have
    one-second precision. Equal timestamps cannot establish delivery order.
    """
    out_of_order: set[DeliveryPair] = set()

    for (receiver, _), deliveries in first_deliveries_by_receiver_sender.items():
        by_timestamp: dict[float, list[tuple[int, str]]] = defaultdict(list)
        for received_at, _, sequence, message_key in deliveries:
            by_timestamp[received_at].append((sequence, message_key))
        max_seen_sequence = -1
        for _, batch in sorted(by_timestamp.items()):
            for sequence, message_key in batch:
                if sequence < max_seen_sequence:
                    out_of_order.add((message_key, receiver))
            max_seen_sequence = max(max_seen_sequence, max(sequence for sequence, _ in batch))

    return out_of_order


def _missing_delivery_diagnostics(
        missing_pairs: set[DeliveryPair],
        sent_messages_by_key: dict[str, Any],
        status_nodes: dict[str, StatusBackend],
) -> dict[str, Any]:
    missing_by_receiver: dict[str, int] = defaultdict(int)
    missing_by_sender: dict[str, int] = defaultdict(int)
    missing_by_message: dict[str, int] = defaultdict(int)
    send_failures_by_sender: dict[str, int] = defaultdict(int)
    send_failures_by_message: dict[str, str] = {}
    missing_details = []

    for message in sent_messages_by_key.values():
        if message.send_status == "failed":
            send_failures_by_sender[message.sender] += 1
            send_failures_by_message[message.key] = message.send_error

    for message_key, receiver in missing_pairs:
        sent_message = sent_messages_by_key[message_key]
        missing_by_receiver[receiver] += 1
        missing_by_sender[sent_message.sender] += 1
        missing_by_message[message_key] += 1
        missing_details.append(_missing_delivery_detail(sent_message, receiver, status_nodes))

    return {
        "missing_deliveries_by_receiver": dict(sorted(missing_by_receiver.items())),
        "missing_deliveries_by_sender": dict(sorted(missing_by_sender.items())),
        "missing_deliveries_by_message": dict(sorted(missing_by_message.items())),
        "send_failures_by_sender": dict(sorted(send_failures_by_sender.items())),
        "send_failures_by_message": dict(sorted(send_failures_by_message.items())),
        "missing_delivery_debug": sorted(
            missing_details,
            key=lambda detail: (detail["receiver"], detail["sender"], detail["sequence"]),
        ),
    }


def _missing_delivery_detail(
        sent_message: Any,
        receiver: str,
        status_nodes: dict[str, StatusBackend],
) -> dict[str, Any]:
    message_id = getattr(sent_message, "message_id", None)

    return {
        "message_key": sent_message.key,
        "message_id": message_id,
        "sender": sent_message.sender,
        "sender_type": sent_message.sender_type,
        "receiver": receiver,
        "receiver_type": node_type(receiver),
        "sequence": sent_message.sequence,
        "sender_envelope_sent": _node_has_signal_containing(
            status_nodes,
            sent_message.sender,
            SignalType.ENVELOPE_SENT,
            message_id,
        ),
        "sender_envelope_expired": _node_has_signal_containing(
            status_nodes,
            sent_message.sender,
            SignalType.ENVELOPE_EXPIRED,
            message_id,
        ),
        "sender_message_delivered": _node_has_signal_containing(
            status_nodes,
            sent_message.sender,
            SignalType.MESSAGE_DELIVERED,
            message_id,
        ),
        "receiver_history_request_started_count": len(_recent_signals(
            status_nodes,
            receiver,
            SignalType.HISTORY_REQUEST_STARTED,
        )),
        "receiver_history_request_completed_count": len(_recent_signals(
            status_nodes,
            receiver,
            SignalType.HISTORY_REQUEST_COMPLETED,
        )),
        "receiver_local_notification_seen": _node_has_signal_containing(
            status_nodes,
            receiver,
            SignalType.LOCAL_NOTIFICATIONS,
            sent_message.text,
        ),
    }


def _node_has_signal_containing(
        status_nodes: dict[str, StatusBackend],
        node_name: str,
        signal_type: SignalType,
        token: str | None,
) -> bool:
    if not token:
        return False
    return any(token in str(signal) for signal in _recent_signals(status_nodes, node_name, signal_type))


def _recent_signals(
        status_nodes: dict[str, StatusBackend],
        node_name: str,
        signal_type: SignalType,
) -> list[dict[str, Any]]:
    queue = status_nodes[node_name].signal.signal_queues.get(signal_type.value)
    if queue is None:
        return []
    return queue.all_signals() if hasattr(queue, "all_signals") else queue.recent()

def _overall_metrics(
        sent_messages: list[Any],
        expected_pairs: set[DeliveryPair],
        successful_pairs: set[DeliveryPair],
        delivery_counts: dict[DeliveryPair, int],
        first_delivery_at: dict[DeliveryPair, float],
        sent_messages_by_key: dict[str, Any],
        out_of_order_pairs: set[DeliveryPair],
) -> dict[str, Any]:
    messages_attempted = len(sent_messages)
    messages_sent = sum(message.send_status == "sent" for message in sent_messages)
    expected_deliveries = len(expected_pairs)
    successful_deliveries = len(successful_pairs)
    duplicate_deliveries = sum(max(0, count - 1) for count in delivery_counts.values())
    latencies = [
        max(0.0, first_delivery_at[pair] - sent_messages_by_key[pair[0]].sent_at)
        for pair in successful_pairs
    ]

    return {
        "messages_attempted": messages_attempted,
        "messages_sent": messages_sent,
        "send_failures": sum(message.send_status == "failed" for message in sent_messages),
        "send_success_rate_percent": messages_sent / messages_attempted * 100 if messages_attempted else None,
        "expected_message_deliveries": expected_deliveries,
        "successful_deliveries": successful_deliveries,
        "lost_deliveries": expected_deliveries - successful_deliveries,
        "duplicate_deliveries": duplicate_deliveries,
        "out_of_order_deliveries": len(out_of_order_pairs),
        "delivery_reliability_percent": (
            successful_deliveries / expected_deliveries * 100 if expected_deliveries else None
        ),
        "message_delivery_latency": _latency_summary(latencies),
    }


def _metrics_by_receiver_type(
        expected_pairs: set[DeliveryPair],
        successful_pairs: set[DeliveryPair],
        delivery_counts: dict[DeliveryPair, int],
        first_delivery_at: dict[DeliveryPair, float],
        sent_messages_by_key: dict[str, Any],
        out_of_order_pairs: set[DeliveryPair],
        receiver_type_resolver: ReceiverTypeResolver,
) -> dict[str, dict[str, Any]]:
    by_node_type = {
        "light": _empty_delivery_bucket(),
        "relay": _empty_delivery_bucket(),
    }
    latencies_by_type: dict[str, list[float]] = {"light": [], "relay": []}

    for message_key, receiver in expected_pairs:
        sent_message = sent_messages_by_key[message_key]
        receiver_type = receiver_type_resolver(sent_message, receiver)
        by_node_type.setdefault(receiver_type, _empty_delivery_bucket())
        latencies_by_type.setdefault(receiver_type, [])
        by_node_type[receiver_type]["expected_message_deliveries"] += 1

        pair = (message_key, receiver)
        if pair in successful_pairs:
            by_node_type[receiver_type]["successful_deliveries"] += 1
            latency = max(0.0, first_delivery_at[pair] - sent_message.sent_at)
            latencies_by_type[receiver_type].append(latency)
        else:
            by_node_type[receiver_type]["lost_deliveries"] += 1

        if pair in out_of_order_pairs:
            by_node_type[receiver_type]["out_of_order_deliveries"] += 1

    for (message_key, receiver), count in delivery_counts.items():
        sent_message = sent_messages_by_key[message_key]
        receiver_type = receiver_type_resolver(sent_message, receiver)
        by_node_type.setdefault(receiver_type, _empty_delivery_bucket())
        by_node_type[receiver_type]["duplicate_deliveries"] += max(0, count - 1)

    for receiver_type, bucket in by_node_type.items():
        expected = bucket["expected_message_deliveries"]
        successful = bucket["successful_deliveries"]
        bucket["delivery_reliability_percent"] = successful / expected * 100 if expected else None
        bucket["message_delivery_latency"] = _latency_summary(latencies_by_type.get(receiver_type, []))

    return by_node_type


def _latency_distribution_by_receiver_type(delivery_records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "light": _latency_distribution([
            record for record in delivery_records if record["receiver_type"] == "light"
        ]),
        "relay": _latency_distribution([
            record for record in delivery_records if record["receiver_type"] == "relay"
        ]),
    }


async def wait_for_delivery_progress(
        sent_messages: list[Any],
        status_nodes: dict[str, StatusBackend],
        receivers: list[str],
        expected_pairs: set[DeliveryPair],
        delivery_pair_resolver: DeliveryPairResolver,
        log_label: str,
        min_wait: float,
        max_wait: float,
        quiet_time: float,
        poll_interval: float,
) -> dict[str, Any]:
    expected_deliveries = len(expected_pairs)
    started_at = time.monotonic()
    last_progress_at = started_at
    last_successful_deliveries = -1

    while True:
        successful_deliveries = len(_successful_delivery_pairs(
            sent_messages,
            status_nodes,
            receivers,
            expected_pairs,
            delivery_pair_resolver,
        ))
        now = time.monotonic()

        if successful_deliveries > last_successful_deliveries:
            logger.info(
                "Observed %s/%s expected %s deliveries",
                successful_deliveries,
                expected_deliveries,
                log_label,
            )
            last_successful_deliveries = successful_deliveries
            last_progress_at = now

        elapsed = now - started_at
        seconds_since_progress = now - last_progress_at

        if not expected_deliveries:
            reason = "no_accepted_sends"
            break
        if successful_deliveries >= expected_deliveries:
            reason = "all_expected_deliveries_observed"
            break
        if elapsed >= max_wait:
            reason = "max_wait_reached"
            break
        if elapsed >= min_wait + quiet_time and seconds_since_progress >= quiet_time:
            reason = "delivery_progress_quiet"
            break

        await asyncio.sleep(poll_interval)

    return {
        "reason": reason,
        "waited_seconds": elapsed,
        "successful_deliveries_at_stop": successful_deliveries,
        "expected_message_deliveries": expected_deliveries,
        "seconds_since_last_progress": seconds_since_progress,
        "min_wait_seconds": min_wait,
        "max_wait_seconds": max_wait,
        "quiet_time_seconds": quiet_time,
        "poll_interval_seconds": poll_interval,
    }


def build_delivery_metrics(
        sent_messages: list[Any],
        status_nodes: dict[str, StatusBackend],
        receivers: list[str],
        expected_pairs: set[DeliveryPair],
        delivery_pair_resolver: DeliveryPairResolver,
        receiver_type_resolver: ReceiverTypeResolver,
) -> dict[str, Any]:
    """Measure delivery of API-accepted sends and report API send success separately.

    API failures do not establish whether a message reached the node (e.g. a response
    can time out after acceptance). They remain in send diagnostics, outside delivery
    loss and latency metrics. Rates without an eligible denominator are None.
    """
    sent_messages_by_key = {message.key: message for message in sent_messages}
    scan = _scan_deliveries(
        sent_messages,
        status_nodes,
        receivers,
        expected_pairs,
        delivery_pair_resolver,
    )
    successful_pairs = set(scan.first_delivery_at.keys())
    missing_pairs = expected_pairs - successful_pairs
    out_of_order = _out_of_order_pairs(scan.first_deliveries_by_receiver_sender)
    delivery_records = _delivery_records(
        successful_pairs,
        sent_messages_by_key,
        scan.first_delivery_at,
        receiver_type_resolver,
        scan.delivery_counts,
        out_of_order,
    )

    return {
        "overall": _overall_metrics(
            sent_messages,
            expected_pairs,
            successful_pairs,
            scan.delivery_counts,
            scan.first_delivery_at,
            sent_messages_by_key,
            out_of_order,
        ),
        "reliability_by_node_type": _metrics_by_receiver_type(
            expected_pairs,
            successful_pairs,
            scan.delivery_counts,
            scan.first_delivery_at,
            sent_messages_by_key,
            out_of_order,
            receiver_type_resolver,
        ),
        "delivery_latency_distribution": _latency_distribution(delivery_records),
        "delivery_latency_distribution_by_receiver_type": _latency_distribution_by_receiver_type(delivery_records),
        "delivery_latency_table": delivery_records,
        "delivery_diagnostics": _missing_delivery_diagnostics(missing_pairs, sent_messages_by_key, status_nodes),
        "delivery_order_measurement": {
            "basis": "first application signal emission timestamp, with one-second precision",
            "equal_timestamp_order": "unknown; response-array order is not used",
            "limitation": "out_of_order_deliveries counts only inversions across distinct timestamps",
        },
    }


def _summarize_counts(counts: dict, limit: int = 5) -> str:
    """Keep console output bounded even when many nodes or routes are affected."""
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    summary = ", ".join(f"{name}={count}" for name, count in items[:limit])
    if len(items) > limit:
        summary += f" (+{len(items) - limit} more)"
    return summary or "none"


def report_delivery_results(metrics: dict, label: str, output_dir: str, prefix: str) -> None:
    """Save requirements and detailed reports with CSV evidence; log totals and problems."""
    try:
        save_delivery_metrics(metrics, output_dir, prefix)
    except OSError as error:
        metrics.pop("metrics_file", None)
        metrics.pop("summary_file", None)
        metrics["metrics_file_error"] = str(error)
        logger.error("%s: cannot save metrics in %s: %s", label, output_dir, error)

    overall = metrics["overall"]
    has_delivery_issues = any(overall[key] for key in (
        "send_failures", "lost_deliveries", "duplicate_deliveries", "out_of_order_deliveries",
    ))
    logger.log(
        logging.WARNING if has_delivery_issues else logging.INFO,
        "%s result: sends=%s/%s; deliveries=%s/%s; send_failures=%s; missing=%s; duplicates=%s; out_of_order=%s",
        label, overall["messages_sent"], overall["messages_attempted"],
        overall["successful_deliveries"], overall["expected_message_deliveries"],
        overall["send_failures"], overall["lost_deliveries"],
        overall["duplicate_deliveries"], overall["out_of_order_deliveries"],
    )
    diagnostics = metrics["delivery_diagnostics"]
    cadence = metrics.get("send_cadence", {})
    if cadence.get("missed_send_slots"):
        logger.warning(
            "%s send cadence: missed_slots=%s; max_deadline_lateness=%.3fs; nodes: %s",
            label, cadence["missed_send_slots"], cadence["max_deadline_lateness_seconds"],
            _summarize_counts({node: result["missed_send_slots"]
                               for node, result in cadence["by_node"].items() if result["missed_send_slots"]}),
        )
    if overall["send_failures"]:
        logger.warning("%s API send failures by sender: %s", label,
                       _summarize_counts(diagnostics["send_failures_by_sender"]))
    if overall["lost_deliveries"]:
        logger.warning(
            "%s missing deliveries at observation end (%s): receivers: %s; senders: %s",
            label, metrics["observation"]["reason"],
            _summarize_counts(diagnostics["missing_deliveries_by_receiver"]),
            _summarize_counts(diagnostics["missing_deliveries_by_sender"]),
        )

    slow = [row for row in metrics["delivery_latency_table"] if row["latency_seconds"] > 10]
    if slow:
        logger.warning(
            "%s: %s deliveries took over 10s (max=%.2fs); affected routes: %s",
            label, len(slow), max(row["latency_seconds"] for row in slow),
            _summarize_counts(Counter(f"{row['sender']} -> {row['receiver']}" for row in slow)),
        )

    if metrics.get("summary_file"):
        logger.info("%s requirements summary: %s", label, metrics["summary_file"])
    if metrics.get("metrics_file"):
        logger.info("%s detailed metrics: %s", label, metrics["metrics_file"])
        for name, artifact in metrics.get("artifacts", {}).items():
            logger.info("%s %s: %s (%s records)", label, name, artifact["path"], artifact["records"])
