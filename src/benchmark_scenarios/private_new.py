# Python Imports
import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, replace
from typing import Any

# Project Imports
import src.logger
from src.benchmark_scenarios.message_delivery import (
    build_delivery_metrics,
    calculate_stagger_between_senders,
    expected_private_delivery_pairs,
    node_type,
    private_delivery_pair,
    private_message_text,
    private_receiver_type,
    wait_for_delivery_progress,
)
from src.benchmark_scenarios.scenario_utils import (
    initialize_relay_light_nodes,
    send_friend_requests_util,
    shutdown_status_nodes,
)
from src.setup_status import accept_friend_requests
from src.status_backend import StatusBackend

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SentPrivateMessage:
    """A send attempt, with sent_at measured before calling the send API."""

    key: str
    text: str
    sender: str
    sender_type: str
    receiver: str
    receiver_type: str
    sequence: int
    sent_at: float
    send_order: int
    send_status: str = "pending"
    send_error: str | None = None


async def _add_pair_contacts(
        status_nodes: dict[str, StatusBackend],
        pairs: list[tuple[str, str]],
) -> None:
    await asyncio.gather(*[
        status_nodes[left].wakuext_service.add_contact(status_nodes[right].public_key, right)
        for left, right in pairs
    ], *[
        status_nodes[right].wakuext_service.add_contact(status_nodes[left].public_key, left)
        for left, right in pairs
    ])


async def _send_member_messages(
        node_name: str,
        node: StatusBackend,
        receiver_name: str,
        receiver_public_key: str,
        run_id: str,
        num_messages: int,
        delay_between_messages: float,
        initial_delay: float,
        sent_messages: list[SentPrivateMessage],
        sent_messages_lock: asyncio.Lock,
) -> None:
    if initial_delay:
        await asyncio.sleep(initial_delay)

    for sequence in range(num_messages):
        text = private_message_text(run_id, node_name, sequence)
        async with sent_messages_lock:
            send_order = len(sent_messages)
            message = SentPrivateMessage(
                key=f"{node_name}:{sequence}",
                text=text,
                sender=node_name,
                sender_type=node_type(node_name),
                receiver=receiver_name,
                receiver_type=node_type(receiver_name),
                sequence=sequence,
                sent_at=time.time(),
                send_order=send_order,
            )
            sent_messages.append(message)

        try:
            logger.debug(f"Sending private message {sequence} from {node_name} to {receiver_name}")
            await node.wakuext_service.send_one_to_one_message(receiver_public_key, text)
        except Exception as e:
            sent_messages[send_order] = replace(
                message, send_status="failed", send_error=f"{type(e).__name__}: {e}",
            )
            logger.error(f"Error sending private message {sequence} from {node_name} to {receiver_name}: {e}")
        else:
            sent_messages[send_order] = replace(message, send_status="sent")
            if sequence == 0:
                logger.info(f"{node_name} successfully began sending {num_messages} private messages")

        if sequence < num_messages - 1:
            await asyncio.sleep(delay_between_messages)

    logger.info(f"{node_name} finished sending private messages")


async def send_one_to_one_message_new(
        consumers: int = 4,
        num_messages: int = 3,
        delay_between_messages: float = 60,
        settle_time: int = 120,
        max_delivery_wait: int = 240,
        quiet_time: int = 30,
        poll_interval: float = 5,
) -> dict[str, Any]:
    """
    Run 10 independent 1:1 private chats with 10 relay and 10 light nodes.

    Each member sends one text-only message every 60 seconds for 3 minutes.
    Every send attempt has one expected delivery, including send API failures.
    Sender start times are staggered across the cadence window instead of synchronized.
    """
    status_nodes: dict[str, StatusBackend] = {}

    try:
        initialized_nodes = await initialize_relay_light_nodes(relay_count=10, light_count=10)
        status_nodes = initialized_nodes.status_nodes
        relay_members = initialized_nodes.relay_members
        light_members = initialized_nodes.light_members

        pairs = list(zip(relay_members, light_members))
        participants = [member for pair in pairs for member in pair]
        peer_by_member = {
            relay: light for relay, light in pairs
        } | {
            light: relay for relay, light in pairs
        }

        logger.info("Setting up 10 independent 1:1 private chats")
        contact_delays = await send_friend_requests_util(
            status_nodes,
            relay_members,
            light_members,
            accept_friend_requests,
            cap_num_receivers=1,
            consumers=consumers,
        )
        logger.info(f"Private chat contact request accept delays in ms: ({len(contact_delays)}) - {contact_delays}")

        await _add_pair_contacts(status_nodes, pairs)
        await asyncio.sleep(30)

        for node in status_nodes.values():
            node.signal.cleanup_signal_queues()

        run_id = str(int(time.time() * 1000))
        sent_messages: list[SentPrivateMessage] = []
        sent_messages_lock = asyncio.Lock()
        initial_delay_step = calculate_stagger_between_senders(delay_between_messages, len(participants))
        configured_send_duration = num_messages * delay_between_messages

        await asyncio.gather(*[
            _send_member_messages(
                member,
                status_nodes[member],
                peer_by_member[member],
                status_nodes[peer_by_member[member]].public_key,
                run_id,
                num_messages,
                delay_between_messages,
                member_index * initial_delay_step,
                sent_messages,
                sent_messages_lock,
            )
            for member_index, member in enumerate(participants)
        ])

        logger.info(
            "Waiting for private-message deliveries: min=%ss max=%ss quiet=%ss",
            settle_time,
            max_delivery_wait,
            quiet_time,
        )
        expected_pairs = expected_private_delivery_pairs(sent_messages)
        receivers = sorted({message.receiver for message in sent_messages})
        observation = await wait_for_delivery_progress(
            sent_messages,
            status_nodes,
            receivers,
            expected_pairs,
            private_delivery_pair,
            "private-message",
            min_wait=settle_time,
            max_wait=max_delivery_wait,
            quiet_time=quiet_time,
            poll_interval=poll_interval,
        )

        metrics = build_delivery_metrics(
            sent_messages,
            status_nodes,
            receivers,
            expected_pairs,
            private_delivery_pair,
            private_receiver_type,
        )
        metrics["observation"] = observation
        metrics["scenario"] = {
            "name": "10 independent 1:1 private chats, half relay half light",
            "pairs": [{"relay": relay, "light": light} for relay, light in pairs],
            "participants": participants,
            "relay_members": relay_members,
            "light_members": light_members,
            "messages_per_member_target": num_messages,
            "delay_between_messages_seconds": delay_between_messages,
            "configured_send_duration_seconds": configured_send_duration,
            "initial_stagger_between_senders_seconds": initial_delay_step,
            "minimum_settle_time_seconds": settle_time,
            "max_delivery_wait_seconds": max_delivery_wait,
            "quiet_time_seconds": quiet_time,
            "poll_interval_seconds": poll_interval,
            "message_template": "MVDS-TEST | <node> | Seq-<sequence> | Run-<run_id> | deterministic text",
        }
        metrics["attempted_messages"] = [asdict(message) for message in sent_messages]
        metrics["sent_messages"] = [
            asdict(message) for message in sent_messages if message.send_status == "sent"
        ]

        logger.info("Private chat delivery metrics:\n%s", json.dumps(metrics, indent=2, sort_keys=True))
        return metrics
    finally:
        await shutdown_status_nodes(status_nodes)
        logger.info("Finished private_new.send_one_to_one_message")
