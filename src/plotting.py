from pathlib import Path
from typing import Any


def _load_plotting():
    try:
        import matplotlib
    except ModuleNotFoundError as e:
        raise RuntimeError("Plot generation requires matplotlib to be installed") from e

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.style.use("ggplot")
    return plt


def generate_delivery_plots(
        metrics: dict[str, Any],
        output_dir: str | Path = "plots",
        prefix: str = "delivery",
) -> dict[str, str]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    plot_paths = {
        "latency_histogram": output_path / f"{prefix}_latency_histogram.png",
        "latency_cdf": output_path / f"{prefix}_latency_cdf.png",
        "latency_by_receiver_type": output_path / f"{prefix}_latency_by_receiver_type.png",
        "reliability_by_receiver_type": output_path / f"{prefix}_reliability_by_receiver_type.png",
    }

    delivery_records = metrics.get("delivery_latency_table", [])
    receiver_metrics = metrics.get("reliability_by_node_type", {})

    _save_latency_histogram(delivery_records, plot_paths["latency_histogram"])
    _save_latency_cdf(delivery_records, plot_paths["latency_cdf"])
    _save_latency_box_by_receiver_type(delivery_records, plot_paths["latency_by_receiver_type"])
    _save_reliability_by_receiver_type(receiver_metrics, plot_paths["reliability_by_receiver_type"])

    return {name: str(path) for name, path in plot_paths.items()}


def _save_empty_plot(filename: Path, title: str, message: str) -> None:
    plt = _load_plotting()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", transform=ax.transAxes)
    ax.set_axis_off()
    fig.savefig(filename, bbox_inches="tight")
    plt.close(fig)


def _save_latency_histogram(delivery_records: list[dict[str, Any]], filename: Path) -> None:
    latencies = [record["latency_seconds"] for record in delivery_records]
    if not latencies:
        _save_empty_plot(filename, "Delivery Latency Histogram", "No successful deliveries")
        return

    plt = _load_plotting()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(latencies, bins=min(30, max(5, len(latencies) // 2)), edgecolor="black")
    ax.set_title("Delivery Latency Histogram")
    ax.set_xlabel("Latency seconds")
    ax.set_ylabel("Deliveries")
    fig.savefig(filename, bbox_inches="tight")
    plt.close(fig)


def _save_latency_cdf(delivery_records: list[dict[str, Any]], filename: Path) -> None:
    latencies = sorted(record["latency_seconds"] for record in delivery_records)
    if not latencies:
        _save_empty_plot(filename, "Delivery Latency CDF", "No successful deliveries")
        return

    plt = _load_plotting()
    y_values = [(index + 1) / len(latencies) * 100 for index in range(len(latencies))]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.step(latencies, y_values, where="post")
    ax.set_title("Delivery Latency CDF")
    ax.set_xlabel("Latency seconds")
    ax.set_ylabel("Deliveries received (%)")
    ax.set_ylim(0, 100)
    ax.grid(True, axis="both", alpha=0.3)
    fig.savefig(filename, bbox_inches="tight")
    plt.close(fig)


def _save_latency_box_by_receiver_type(delivery_records: list[dict[str, Any]], filename: Path) -> None:
    records_by_type: dict[str, list[float]] = {}
    for record in delivery_records:
        records_by_type.setdefault(record["receiver_type"], []).append(record["latency_seconds"])

    if not records_by_type:
        _save_empty_plot(filename, "Latency By Receiver Type", "No successful deliveries")
        return

    plt = _load_plotting()
    ordered_items = sorted(records_by_type.items())

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.boxplot([values for _, values in ordered_items])
    ax.set_title("Latency By Receiver Type")
    ax.set_xlabel("Receiver type")
    ax.set_ylabel("Latency seconds")
    ax.set_xticks(range(1, len(ordered_items) + 1))
    ax.set_xticklabels([name for name, _ in ordered_items])
    fig.savefig(filename, bbox_inches="tight")
    plt.close(fig)


def _save_reliability_by_receiver_type(receiver_metrics: dict[str, dict[str, Any]], filename: Path) -> None:
    receiver_types = sorted(receiver_metrics.keys())
    if not receiver_types:
        _save_empty_plot(filename, "Reliability By Receiver Type", "No receiver metrics")
        return

    plt = _load_plotting()
    expected = [receiver_metrics[name]["expected_message_deliveries"] for name in receiver_types]
    successful = [receiver_metrics[name]["successful_deliveries"] for name in receiver_types]
    lost = [receiver_metrics[name]["lost_deliveries"] for name in receiver_types]
    x_positions = list(range(len(receiver_types)))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([x - width for x in x_positions], expected, width=width, label="Expected")
    ax.bar(x_positions, successful, width=width, label="Successful")
    ax.bar([x + width for x in x_positions], lost, width=width, label="Lost")
    ax.set_title("Reliability By Receiver Type")
    ax.set_xlabel("Receiver type")
    ax.set_ylabel("Deliveries")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(receiver_types)
    ax.legend()
    fig.savefig(filename, bbox_inches="tight")
    plt.close(fig)
