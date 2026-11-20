"""Plot typical delivery times and slow deliveries without hiding either population."""

import argparse
import csv
import json
import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any


RECEIVER_COLORS = {"light": "#2166ac", "relay": "#d95f02", "unknown": "#777777"}
TIMELINE_WINDOW_SECONDS = 300
LATENCY_PERCENTILES = (
    ("p10 (s)", 0.10),
    ("p25 (s)", 0.25),
    ("p40 (s)", 0.40),
    ("Median (s)", 0.50),
    ("p95 (s)", 0.95),
    ("p99 (s)", 0.99),
)


def _load_plotting():
    try:
        import matplotlib
    except ModuleNotFoundError as error:
        raise RuntimeError("Plot generation requires matplotlib to be installed") from error

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.style.use("default")
    return plt


def _percentile(values: list[float], fraction: float) -> float:
    """Linearly interpolate a percentile in an already sorted, nonempty sample."""
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _percentage(value: float) -> str:
    return f"{value:.3f}%" if 0 < value < 1 else f"{value:.1f}%"


def _reliability_label(results: dict) -> str:
    expected = results.get("expected_message_deliveries")
    successful = results.get("successful_deliveries")
    if not expected or successful is None:
        return "n/a"
    if successful == expected:
        return "100%"
    percentage = f"{successful / expected * 100:.4f}"
    # A rare loss must never round up to a claim of complete delivery.
    return "<100%" if percentage == "100.0000" else percentage.rstrip("0").rstrip(".") + "%"


def _latency_figure(title: str, series: dict[str, list[float]], run_id: str,
                    note: str, results_by_type: dict[str, dict], panels: int = 1):
    plt = _load_plotting()
    fig = plt.figure(figsize=(16, 9.5 if panels > 1 else 8.5))
    fig.suptitle(title, x=0.09, y=0.97, ha="left", fontsize=16, fontweight="bold")
    layout = fig.add_gridspec(2, 1, height_ratios=[1.3, 4.5],
                              left=0.09, right=0.97, bottom=0.20, top=0.91, hspace=0.24)
    summary_ax = fig.add_subplot(layout[0])
    summary_ax.set_axis_off()
    rows = []
    for name in dict.fromkeys([*series, *results_by_type]):
        values = series.get(name, [])
        statistics = ([f"{_percentile(values, fraction):.2f}" for _, fraction in LATENCY_PERCENTILES]
                      + [f"{values[-1]:.2f}"]) if values else ["n/a"] * (len(LATENCY_PERCENTILES) + 1)
        results = results_by_type.get(name, {})
        delivery_counts = [f"{results[field]:,}" if results.get(field) is not None else "n/a"
                           for field in ("duplicate_deliveries", "out_of_order_deliveries")]
        rows.append([name.capitalize(), f"{len(values):,}",
                     _reliability_label(results), *delivery_counts, *statistics])
    table = summary_ax.table(
        cellText=rows,
        colLabels=["Recipients", "Deliveries", "Reliability", "Duplicates", "Out of order",
                   *[label for label, _ in LATENCY_PERCENTILES], "Max (s)"],
        cellLoc="center", bbox=[0, 0, 1, 1],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    for (row, _), cell in table.get_celld().items():
        cell.set_edgecolor("#d8dee5")
        cell.set_facecolor("#edf1f5" if row == 0 else "white")
        if row == 0:
            cell.set_text_props(weight="bold")

    body = layout[1].subgridspec(panels, 1, hspace=0.36)
    axes = [fig.add_subplot(body[index]) for index in range(panels)]
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", color="#d8dee5", alpha=0.7)
        ax.set_axisbelow(True)
    fig.text(0.09, 0.12, note, fontsize=9, color="#444444")
    fig.text(0.09, 0.085, "Reliability = successful / expected deliveries. Latencies use successful deliveries; receive precision is one second.",
             fontsize=9, color="#444444")
    fig.text(0.09, 0.05, "Duplicates count extra receipts at the same recipient. Order is per sender/recipient; same-second order is unknown.",
             fontsize=9, color="#444444")
    fig.text(0.09, 0.02, f"Run {run_id}", fontsize=9, color="#666666")
    return fig, axes


def _finish_plot(fig, filename: Path) -> None:
    plt = _load_plotting()
    try:
        fig.savefig(filename, dpi=160)
    finally:
        plt.close(fig)


def generate_delivery_plots(
        metrics: dict[str, Any],
        output_dir: str | Path = "plots",
        prefix: str = "delivery",
) -> dict[str, str]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    records = metrics.get("delivery_latency_table", [])
    by_type: dict[str, list[float]] = defaultdict(list)
    for record in records:
        latency = record["latency_seconds"]
        if not math.isfinite(latency) or latency < 0:
            raise ValueError("Successful deliveries must have finite, nonnegative latencies")
        by_type[record.get("receiver_type", "unknown")].append(latency)
    series = {"overall": sorted(record["latency_seconds"] for record in records)}
    series.update({name: sorted(values) for name, values in sorted(by_type.items())})
    results_by_type = {"overall": metrics.get("overall", {}), **metrics.get("reliability_by_node_type", {})}
    scenario = metrics.get("scenario", {})
    run_id = scenario.get("run_id", prefix)
    label = f"{len(scenario['members'])}-member group" if "members" in scenario else "Private chats"
    plot_paths = {
        name: output_path / f"{prefix}_{name}.png"
        for name in ("latency_buckets", "latency_tail", "latency_timeline", "reliability_by_receiver_type")
    }
    if records:
        _save_latency_buckets(series, plot_paths["latency_buckets"], label, run_id, results_by_type)
        _save_latency_tail(series, plot_paths["latency_tail"], label, run_id, results_by_type)
        _save_latency_timeline(metrics, records, series, plot_paths["latency_timeline"], label, run_id)
    else:
        for name in ("latency_buckets", "latency_tail", "latency_timeline"):
            fig, (ax,) = _latency_figure(f"{label}: {name.replace('_', ' ')}", series, run_id,
                                        "No successful deliveries; latency is unavailable.", results_by_type)
            ax.set_axis_off()
            _finish_plot(fig, plot_paths[name])
    _save_reliability_by_receiver_type(metrics.get("reliability_by_node_type", {}),
                                       plot_paths["reliability_by_receiver_type"])
    return {name: str(path) for name, path in plot_paths.items()}


def _save_empty_plot(filename: Path, title: str, message: str) -> None:
    plt = _load_plotting()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", transform=ax.transAxes)
    ax.set_axis_off()
    _finish_plot(fig, filename)


def _save_latency_buckets(series: dict[str, list[float]], filename: Path, label: str,
                          run_id: str, results_by_type: dict[str, dict]) -> None:
    fig, (ax,) = _latency_figure(
        f"{label}: delivery latency buckets", series, run_id,
        "Percentages are within each recipient type. Labels show percentage and delivery count.",
        results_by_type,
    )
    from matplotlib.ticker import PercentFormatter

    groups = [(name, values) for name, values in series.items() if name != "overall"]
    width = 0.8 / len(groups)
    for index, (name, values) in enumerate(groups):
        # Boundaries are [0, 1), [1, 2), [2, 5), [5, 10], (10, infinity).
        cuts = [0, bisect_left(values, 1), bisect_left(values, 2),
                bisect_left(values, 5), bisect_right(values, 10), len(values)]
        counts = [right - left for left, right in zip(cuts, cuts[1:])]
        percentages = [count / len(values) * 100 for count in counts]
        positions = [bucket - 0.4 + width * (index + 0.5) for bucket in range(5)]
        bars = ax.bar(positions, percentages, width=width, label=name.capitalize(),
                       color=RECEIVER_COLORS.get(name, RECEIVER_COLORS["unknown"]))
        ax.bar_label(bars, labels=[f"{_percentage(pct)}\n{count:,}" for pct, count in zip(percentages, counts)],
                      padding=4, fontsize=9)
    ax.set_xticks(range(5), ["<1 s", "1–<2 s", "2–<5 s", "5–10 s", ">10 s"])
    ax.set_ylabel("Share of successful deliveries")
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    ax.set_ylim(0, 112)
    ax.legend(loc="upper right", frameon=False)
    _finish_plot(fig, filename)


def _save_latency_tail(series: dict[str, list[float]], filename: Path, label: str,
                       run_id: str, results_by_type: dict[str, dict]) -> None:
    values = series["overall"]
    slow_count = len(values) - bisect_right(values, 10)
    fig, (ax,) = _latency_figure(
        f"{label}: delivery latency tail", series, run_id,
        f"Above 10 s: {slow_count:,}/{len(values):,} ({_percentage(slow_count / len(values) * 100)}). "
        "Zero percentages are omitted on the logarithmic axis.",
        results_by_type,
    )
    from matplotlib.ticker import FuncFormatter, MaxNLocator

    for name, latencies in series.items():
        if name == "overall":
            continue
        # At each distinct threshold, the curve is P(latency > threshold).
        thresholds = sorted(set([0.0, *latencies]))
        percentages = [(len(latencies) - bisect_right(latencies, value)) / len(latencies) * 100
                       for value in thresholds]
        # Explicit horizontal endpoints retain the last positive step at the maximum.
        x_values, y_values = [], []
        for index, threshold in enumerate(thresholds):
            if index and percentages[index - 1] > 0:
                x_values.append(threshold)
                y_values.append(percentages[index - 1])
            if percentages[index] > 0:
                x_values.append(threshold)
                y_values.append(percentages[index])
        ax.plot(x_values, y_values, linewidth=2, label=f"{name.capitalize()} (n={len(latencies):,})",
                 color=RECEIVER_COLORS.get(name, RECEIVER_COLORS["unknown"]))
    ax.set_yscale("log")
    ax.set_ylim(10 ** math.floor(math.log10(100 / max(len(sample) for sample in series.values()))), 120)
    ax.set_xlim(0, max(2, math.ceil(values[-1])))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}%"))
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlabel("Latency threshold (seconds)")
    ax.set_ylabel("Deliveries taking longer than threshold (log scale)")
    ax.legend(loc="upper right", frameon=False)
    if values[-1] == 0:
        ax.text(0.5, 0.5, "All recorded latencies are 0 s", transform=ax.transAxes, ha="center")
    _finish_plot(fig, filename)


def _save_latency_timeline(metrics: dict, records: list[dict], series: dict[str, list[float]],
                           filename: Path, label: str, run_id: str) -> None:
    groups = [name for name in series if name != "overall"]
    if any("sent_at" not in record for record in records):
        _save_empty_plot(filename, f"{label}: delivery latency over time", "Send timestamps unavailable")
        return
    start = min((message["sent_at"] for message in metrics.get("attempted_messages", [])),
                default=min(record["sent_at"] for record in records))
    windows: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for record in records:
        window = math.floor((record["sent_at"] - start) / TIMELINE_WINDOW_SECONDS)
        windows[record.get("receiver_type", "unknown")][window].append(record["latency_seconds"])
    last_window = max(window for group in windows.values() for window in group)
    hours = [(window + 0.5) * TIMELINE_WINDOW_SECONDS / 3600 for window in range(last_window + 1)]
    duration = max(metrics.get("scenario", {}).get("configured_send_duration_seconds", 0) / 3600,
                   (last_window + 1) * TIMELINE_WINDOW_SECONDS / 3600)
    fig, axes = _latency_figure(
        f"{label}: delivery latency over time", series, run_id,
        "Five-minute windows, grouped by message send time. Gaps mean no successful deliveries in that window.",
        {"overall": metrics.get("overall", {}), **metrics.get("reliability_by_node_type", {})},
        panels=len(groups),
    )
    for name, ax in zip(groups, axes):
        samples = [sorted(windows[name].get(window, [])) for window in range(last_window + 1)]
        for fraction, statistic, color, style in (
                (0.5, "Median", "#2166ac", "-"), (0.95, "p95", "#168477", "--"),
                (1.0, "Maximum", "#d95f02", ":")):
            points = [_percentile(sample, fraction) if sample else math.nan for sample in samples]
            ax.plot(hours, points, label=statistic, color=color, linestyle=style, linewidth=1.8,
                     marker="." if len(hours) == 1 else None)
        ax.set_title(f"{name.capitalize()} recipients", loc="left", fontsize=11)
        ax.set_ylabel("Latency (seconds)")
        ax.set_xlim(0, duration)
        ax.set_ylim(0, max(1, series["overall"][-1] * 1.3))
        ax.legend(loc="upper left", ncol=3, frameon=False, fontsize=9)
    axes[-1].set_xlabel("Elapsed time since first send (hours)")
    _finish_plot(fig, filename)


def _save_reliability_by_receiver_type(receiver_metrics: dict[str, dict[str, Any]], filename: Path) -> None:
    receiver_types = sorted(receiver_metrics)
    if not receiver_types:
        _save_empty_plot(filename, "Reliability by recipient type", "No recipient metrics")
        return
    plt = _load_plotting()
    fig, ax = plt.subplots(figsize=(10, 6))
    width = 0.25
    for offset, field, label in ((-width, "expected_message_deliveries", "Expected"),
                                  (0, "successful_deliveries", "Successful"),
                                  (width, "lost_deliveries", "Lost")):
        ax.bar([index + offset for index in range(len(receiver_types))],
                 [receiver_metrics[name][field] for name in receiver_types], width=width, label=label)
    ax.set_title("Reliability by recipient type")
    ax.set_ylabel("Deliveries")
    ax.set_xticks(range(len(receiver_types)), [name.capitalize() for name in receiver_types])
    ax.legend()
    fig.tight_layout()
    _finish_plot(fig, filename)


def regenerate_delivery_plots(metrics_file: str | Path, output_dir: str | Path | None = None) -> dict[str, str]:
    """Replot saved CSV evidence; update only the detailed report's plot references."""
    from src.delivery_artifacts import write_json_file

    metrics_path = Path(metrics_file)
    with metrics_path.open(encoding="utf-8") as source:
        report = json.load(source)

    def artifact_path(name: str) -> Path:
        original = Path(report["artifacts"][name]["path"])
        local = metrics_path.parent / original.name
        return local if local.is_file() else original

    with artifact_path("message_attempts").open(encoding="utf-8", newline="") as source:
        messages = [{"key": row["message_key"], "sent_at": float(row["sent_at"])}
                    for row in csv.DictReader(source)]
    sent_at = {message["key"]: message["sent_at"] for message in messages}
    receiver_types = {node: kind for kind in ("light", "relay")
                      for node in report["scenario"][f"{kind}_members"]}
    records = []
    with artifact_path("deliveries").open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            if not row["received_at"] and not row["latency_seconds"]:
                continue
            if not row["received_at"] or not row["latency_seconds"]:
                raise ValueError("Delivery CSV has an incomplete successful-delivery row")
            records.append({
                "latency_seconds": float(row["latency_seconds"]),
                "sent_at": sent_at[row["message_key"]],
                "receiver_type": receiver_types[row["receiver"]],
            })
    if len(records) != report["overall"]["successful_deliveries"]:
        raise ValueError("Delivery CSV count does not match the metrics report")
    scenario = report["scenario"]
    prefix = metrics_path.stem.removesuffix("_metrics")
    destination = output_dir if output_dir is not None else (
        Path("plots") / (scenario.get("namespace") or "default") / ("group" if "group_id" in scenario else "private")
    )
    report["plots"] = generate_delivery_plots(
        {**report, "attempted_messages": messages, "delivery_latency_table": records}, destination, prefix,
    )
    write_json_file(metrics_path, report)
    return report["plots"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Regenerate delivery plots from a metrics JSON and its CSV files.")
    parser.add_argument("metrics_file", type=Path)
    parser.add_argument("--output-dir", type=Path)
    arguments = parser.parse_args()
    for plot_name, plot_path in regenerate_delivery_plots(arguments.metrics_file, arguments.output_dir).items():
        print(f"{plot_name}: {plot_path}")
