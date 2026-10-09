# Python Imports
import asyncio
import logging
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

# Project Imports
import src.logger
from src.enums import SignalType
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


def private_message_text(run_id: str, node_name: str, sequence: int) -> str:
    return (
        f"MVDS-TEST | {node_name} | Seq-{sequence:06d} | Run-{run_id} | "
        "The quick brown fox jumps over the lazy dog."
    )


def expected_group_delivery_pairs(sent_messages: list[Any], group_members: list[str]) -> set[DeliveryPair]:
    return {
        (message.key, receiver)
        for message in sent_messages
        for receiver in group_members
        if receiver != message.sender
    }


def expected_private_delivery_pairs(sent_messages: list[Any]) -> set[DeliveryPair]:
    return {(message.key, message.receiver) for message in sent_messages}


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
    if timestamp > 1_000_000_000_000:
        return timestamp / 1000
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
        "delivery_reliability_percent": 0.0,
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


def _successful_delivery_pairs(
        sent_messages: list[Any],
        status_nodes: dict[str, StatusBackend],
        receivers: list[str],
        expected_pairs: set[DeliveryPair],
        delivery_pair_resolver: DeliveryPairResolver,
) -> set[DeliveryPair]:
    sent_by_text = {message.text: message for message in sent_messages}
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
    sent_by_text = {message.text: message for message in sent_messages}
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
        })

    records.sort(key=lambda record: record["latency_seconds"])
    return records


def _out_of_order_pairs(
        first_deliveries_by_receiver_sender: dict[tuple[str, str], list[tuple[float, int, int, str]]],
) -> set[DeliveryPair]:
    out_of_order: set[DeliveryPair] = set()

    for (receiver, _), deliveries in first_deliveries_by_receiver_sender.items():
        max_seen_sequence = -1
        for _, _, sequence, message_key in sorted(deliveries):
            if sequence < max_seen_sequence:
                out_of_order.add((message_key, receiver))
            max_seen_sequence = max(max_seen_sequence, sequence)

    return out_of_order


def _missing_delivery_diagnostics(
        missing_pairs: set[DeliveryPair],
        sent_messages_by_key: dict[str, Any],
) -> dict[str, dict[str, int]]:
    missing_by_receiver: dict[str, int] = defaultdict(int)
    missing_by_sender: dict[str, int] = defaultdict(int)
    missing_by_message: dict[str, int] = defaultdict(int)

    for message_key, receiver in missing_pairs:
        sent_message = sent_messages_by_key[message_key]
        missing_by_receiver[receiver] += 1
        missing_by_sender[sent_message.sender] += 1
        missing_by_message[message_key] += 1

    return {
        "missing_deliveries_by_receiver": dict(sorted(missing_by_receiver.items())),
        "missing_deliveries_by_sender": dict(sorted(missing_by_sender.items())),
        "missing_deliveries_by_message": dict(sorted(missing_by_message.items())),
    }


def _overall_metrics(
        sent_messages: list[Any],
        expected_pairs: set[DeliveryPair],
        successful_pairs: set[DeliveryPair],
        delivery_counts: dict[DeliveryPair, int],
        first_delivery_at: dict[DeliveryPair, float],
        sent_messages_by_key: dict[str, Any],
        out_of_order_pairs: set[DeliveryPair],
) -> dict[str, Any]:
    expected_deliveries = len(expected_pairs)
    successful_deliveries = len(successful_pairs)
    duplicate_deliveries = sum(max(0, count - 1) for count in delivery_counts.values())
    latencies = [
        max(0.0, first_delivery_at[pair] - sent_messages_by_key[pair[0]].sent_at)
        for pair in successful_pairs
    ]

    return {
        "messages_sent": len(sent_messages),
        "expected_message_deliveries": expected_deliveries,
        "successful_deliveries": successful_deliveries,
        "lost_deliveries": expected_deliveries - successful_deliveries,
        "duplicate_deliveries": duplicate_deliveries,
        "out_of_order_deliveries": len(out_of_order_pairs),
        "delivery_reliability_percent": (
            successful_deliveries / expected_deliveries * 100 if expected_deliveries else 0.0
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
        bucket["delivery_reliability_percent"] = successful / expected * 100 if expected else 0.0
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
        "delivery_diagnostics": _missing_delivery_diagnostics(missing_pairs, sent_messages_by_key),
    }
