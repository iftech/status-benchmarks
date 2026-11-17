"""Save metric summaries and the compact records needed to verify deliveries."""

import csv
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable


@contextmanager
def _atomic_text_file(path: Path):
    temporary = path.with_name(path.name + ".partial")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as output:
            yield output
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json_file(path: Path, value: Any) -> None:
    with _atomic_text_file(path) as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def write_jsonl_file(path: Path, records: Iterable[dict]) -> None:
    with _atomic_text_file(path) as output:
        for record in records:
            output.write(json.dumps(record, separators=(",", ":")) + "\n")


def build_delivery_summary(metrics: dict) -> dict:
    """Keep requirement-level results without node lists or per-message records."""
    scenario = metrics["scenario"]
    is_group = "group_id" in scenario
    participants = scenario["members"] if is_group else scenario["participants"]
    overall_fields = (
        "messages_attempted", "messages_sent", "send_failures",
        "expected_message_deliveries", "successful_deliveries", "lost_deliveries",
        "duplicate_deliveries", "out_of_order_deliveries", "message_delivery_latency",
        "delivery_reliability_percent",
    )
    conversation = {"type": "group" if is_group else "private", "is_group": is_group}
    if is_group:
        conversation["group_size"] = len(participants)
    else:
        conversation["private_conversations"] = len(scenario["pairs"])

    return {
        "format_version": 1,
        "run_id": scenario["run_id"],
        "namespace": scenario.get("namespace"),
        "scenario": conversation,
        "nodes": {
            "total": len(participants),
            "relay": len(scenario["relay_members"]),
            "light": len(scenario["light_members"]),
        },
        "configured_send_duration_seconds": scenario["configured_send_duration_seconds"],
        "message_length_characters": scenario["message_length_characters"],
        "message_cadence": {
            "per_node_interval_seconds": scenario["delay_between_messages_seconds"],
            "initial_stagger_between_senders_seconds": scenario["initial_stagger_between_senders_seconds"],
            "missed_send_slots": metrics["send_cadence"]["missed_send_slots"],
        },
        "message_identifiers": {
            "file": Path(metrics["artifacts"]["message_attempts"]["path"]).name,
            "column": "message_key",
            "format": "<sender>:<sequence>; unique within run_id",
        },
        "overall": {key: metrics["overall"][key] for key in overall_fields},
        "reliability_by_receiver_type": metrics["reliability_by_node_type"],
        "measurement_notes": {
            "messages_sent": "API-accepted sends; API failures are excluded from expected deliveries",
            "latency": "send API call start to recipient application signal; receive timestamp has one-second precision",
            "out_of_order": "per sender/recipient; equal receive timestamps cannot be ordered",
            "lost_deliveries": "not observed by the end of the delivery observation window",
        },
        "files": {
            "detailed_metrics": Path(metrics["metrics_file"]).name,
            "deliveries": Path(metrics["artifacts"]["deliveries"]["path"]).name,
        },
    }


def save_delivery_metrics(metrics: dict, output_dir: str, prefix: str) -> Path:
    """Save a requirements summary, detailed metrics and plain CSV evidence."""
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    artifacts = {}

    messages = metrics.get("attempted_messages", metrics.get("sent_messages", []))
    messages_path = destination / f"{prefix}_messages.csv"
    with _atomic_text_file(messages_path) as output:
        writer = csv.writer(output)
        writer.writerow(["message_key", "sent_at", "send_status", "send_error"])
        for message in messages:
            writer.writerow([message["key"], message["sent_at"], message["send_status"],
                             message.get("send_error")])
    artifacts["message_attempts"] = {
        "path": str(messages_path), "format": "csv", "records": len(messages),
        "message_key_format": "<sender>:<sequence>; the run ID is in scenario.run_id",
    }

    deliveries = metrics["delivery_latency_table"]
    diagnostics = metrics["delivery_diagnostics"]
    missing = diagnostics.get("missing_delivery_debug", [])
    deliveries_path = destination / f"{prefix}_deliveries.csv"
    with _atomic_text_file(deliveries_path) as output:
        writer = csv.writer(output)
        writer.writerow(["message_key", "receiver", "received_at", "latency_seconds",
                         "duplicates", "out_of_order"])
        for record in deliveries:
            writer.writerow([record["message_key"], record["receiver"], record["received_at"],
                             record["latency_seconds"], record.get("duplicates", 0),
                             int(record.get("out_of_order", False))])
        for record in missing:
            writer.writerow([record["message_key"], record["receiver"], None, None, 0, 0])
    artifacts["deliveries"] = {
        "path": str(deliveries_path), "format": "csv", "records": len(deliveries) + len(missing),
        "missing_delivery_encoding": "empty received_at and latency_seconds",
    }

    metrics_path = destination / f"{prefix}_metrics.json"
    summary_path = destination / f"{prefix}_summary.json"
    metrics["metrics_file"] = str(metrics_path)
    metrics["summary_file"] = str(summary_path)
    metrics["artifacts"] = artifacts
    report = {key: value for key, value in metrics.items() if key not in {
        "attempted_messages", "sent_messages", "delivery_latency_table", "delivery_trace",
    }}
    report["format_version"] = 3
    report["delivery_diagnostics"] = {key: value for key, value in diagnostics.items() if key not in {
        "missing_delivery_debug", "missing_deliveries_by_message", "send_failures_by_message",
    }}
    if metrics.get("delivery_trace") is not None:
        report["delivery_trace"] = {key: value for key, value in metrics["delivery_trace"].items()
                                    if key not in {"deliveries", "uncorrelated_api_attempts"}}
    write_json_file(metrics_path, report)
    write_json_file(summary_path, build_delivery_summary(metrics))
    return metrics_path
