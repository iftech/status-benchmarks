import argparse
import asyncio
import logging
import math
from pathlib import Path

import src.logger
from src.benchmark_scenarios.messenger_new import send_group_message_new
from src.benchmark_scenarios.private_new import send_one_to_one_message_new

logger = logging.getLogger("src.runner")


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a positive integer") from error
    if number <= 0:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return number


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a finite positive number") from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Expected a finite positive number")
    return number


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one Messenger reliability experiment per process.")
    parser.add_argument("--scenario", choices=("group", "private"), required=True)
    parser.add_argument("--namespace", help="Defaults to status-bench-<scenario>.")
    parser.add_argument("--num-messages", type=positive_int,
                        help="Messages per node for a nominal 8-hour run: 480 for group or 960 for private.")
    parser.add_argument("--interval", type=positive_float,
                        help="Seconds between send starts; defaults to 60 for group or 30 for private.")
    parser.add_argument("--output-dir", help="Defaults to logs/delivery-results/<namespace>.")
    parser.add_argument("--no-plots", action="store_true", help="Skip plot generation.")
    return parser.parse_args(argv)


async def run_scenario(args: argparse.Namespace) -> dict:
    is_group = args.scenario == "group"
    namespace = args.namespace or f"status-bench-{args.scenario}"
    num_messages = args.num_messages if args.num_messages is not None else (480 if is_group else 960)
    interval = args.interval if args.interval is not None else (60 if is_group else 30)
    output_dir = Path(args.output_dir) if args.output_dir else Path("logs/delivery-results") / namespace
    plots_dir = None if args.no_plots else str(Path("plots") / namespace / args.scenario)
    runner = send_group_message_new if is_group else send_one_to_one_message_new
    logger.info("Starting %s experiment: namespace=%s; messages/node=%s; interval=%ss; results=%s",
                args.scenario, namespace, num_messages, interval, output_dir)
    return await runner(
        num_messages=num_messages,
        delay_between_messages=interval,
        namespace=namespace,
        results_output_dir=str(output_dir),
        plots_output_dir=plots_dir,
    )


if __name__ == "__main__":
    asyncio.run(run_scenario(parse_args()))
