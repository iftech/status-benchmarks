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
    expected_group_delivery_pairs,
    group_delivery_pair,
    group_receiver_type,
    node_type,
    wait_for_delivery_progress,
)
from src.benchmark_scenarios.scenario_utils import (
    initialize_relay_light_nodes,
    send_friend_requests_util,
    shutdown_status_nodes,
)
from src.setup_status import (
    accept_friend_requests,
    add_contacts,
    create_group_chat,
)
from src.status_backend import StatusBackend

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SentGroupMessage:
    """A send attempt, with sent_at measured before calling the send API."""

    key: str
    text: str
    sender: str
    sender_type: str
    sequence: int
    sent_at: float
    send_order: int
    send_status: str = "pending"
    send_error: str | None = None


async def _send_member_messages(
        node_name: str,
        node: StatusBackend,
        group_id: str,
        run_id: str,
        num_messages: int,
        delay_between_messages: float,
        initial_delay: float,
        sent_messages: list[SentGroupMessage],
        sent_messages_lock: asyncio.Lock,
) -> None:
    if initial_delay:
        await asyncio.sleep(initial_delay)

    for sequence in range(num_messages):
        text = f"GroupBenchmark|run={run_id}|sender={node_name}|seq={sequence}"
        async with sent_messages_lock:
            send_order = len(sent_messages)
            message = SentGroupMessage(
                key=f"{node_name}:{sequence}",
                text=text,
                sender=node_name,
                sender_type=node_type(node_name),
                sequence=sequence,
                sent_at=time.time(),
                send_order=send_order,
            )
            sent_messages.append(message)

        try:
            logger.debug(f"Sending group message {sequence} from {node_name}")
            await node.wakuext_service.send_group_chat_message(group_id, text)
        except Exception as e:
            sent_messages[send_order] = replace(
                message, send_status="failed", send_error=f"{type(e).__name__}: {e}",
            )
            logger.exception(f"Error sending group message {sequence} from {node_name}: {e}")
        else:
            sent_messages[send_order] = replace(message, send_status="sent")
            if sequence == 0:
                logger.info(f"{node_name} successfully began sending {num_messages} group messages")

        if sequence < num_messages - 1:
            await asyncio.sleep(delay_between_messages)

    logger.info(f"{node_name} finished sending group messages")


async def send_group_message_new(
        consumers: int = 4,
        num_messages: int = 3,
        delay_between_messages: float = 60,
        settle_time: int = 120,
        max_delivery_wait: int = 240,
        quiet_time: int = 30,
        poll_interval: float = 5,
) -> dict[str, Any]:
    """
    Run a 20-member private group benchmark with 10 relay and 10 light nodes.

    Reliability by node type is grouped by expected recipient type. Each message
    accepted by the send API has 19 expected deliveries. API failures are reported
    separately and do not count as lost node-to-node deliveries.
    Sender start times are staggered across the cadence window instead of synchronized.
    """
    # 20-member private group: 10 relay nodes and 10 light nodes.
    # Every member sends one message every 60 seconds for 3 minutes.
    status_nodes: dict[str, StatusBackend] = {}

    try:
        initialized_nodes = await initialize_relay_light_nodes(relay_count=10, light_count=10)
        status_nodes = initialized_nodes.status_nodes
        relay_members = initialized_nodes.relay_members
        light_members = initialized_nodes.light_members
        group_members = relay_members + light_members

        admin = relay_members[0]
        invited_members = [member for member in group_members if member != admin]
        invited_member_pub_keys = [status_nodes[member].public_key for member in invited_members]

        logger.info(f"Setting up 20-member group chat with admin {admin}")
        contact_delays = await send_friend_requests_util(
            status_nodes,
            [admin],
            invited_members,
            accept_friend_requests,
            consumers=consumers,
        )
        logger.info(f"Group setup contact request accept delays in ms: ({len(contact_delays)}) - {contact_delays}")

        await add_contacts(status_nodes, [admin], invited_members)
        await asyncio.sleep(30)

        group_id = await create_group_chat(status_nodes[admin], invited_member_pub_keys)
        logger.info(f"Waiting 30 seconds after creating group {group_id}")
        await asyncio.sleep(30)

        for node in status_nodes.values():
            node.signal.cleanup_signal_queues()

        run_id = str(int(time.time() * 1000))
        sent_messages: list[SentGroupMessage] = []
        sent_messages_lock = asyncio.Lock()
        initial_delay_step = calculate_stagger_between_senders(
            delay_between_messages,
            len(group_members),
            sender_label="group members",
        )
        configured_send_duration = num_messages * delay_between_messages

        await asyncio.gather(*[
            _send_member_messages(
                member,
                status_nodes[member],
                group_id,
                run_id,
                num_messages,
                delay_between_messages,
                member_index * initial_delay_step,
                sent_messages,
                sent_messages_lock,
            )
            for member_index, member in enumerate(group_members)
        ])

        logger.info(
            "Waiting for group deliveries: min=%ss max=%ss quiet=%ss",
            settle_time,
            max_delivery_wait,
            quiet_time,
        )
        expected_pairs = expected_group_delivery_pairs(sent_messages, group_members)
        observation = await wait_for_delivery_progress(
            sent_messages,
            status_nodes,
            group_members,
            expected_pairs,
            group_delivery_pair,
            "group",
            min_wait=settle_time,
            max_wait=max_delivery_wait,
            quiet_time=quiet_time,
            poll_interval=poll_interval,
        )

        metrics = build_delivery_metrics(
            sent_messages,
            status_nodes,
            group_members,
            expected_pairs,
            group_delivery_pair,
            group_receiver_type,
        )
        metrics["observation"] = observation
        metrics["scenario"] = {
            "name": "20-member group chat, half relay half light",
            "group_id": group_id,
            "members": group_members,
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
        }
        metrics["attempted_messages"] = [asdict(message) for message in sent_messages]
        metrics["sent_messages"] = [
            asdict(message) for message in sent_messages if message.send_status == "sent"
        ]

        logger.info("Group chat delivery metrics:\n%s", json.dumps(metrics, indent=2, sort_keys=True))
        return metrics
    finally:
        await shutdown_status_nodes(status_nodes)
        logger.info("Finished messenger_new.send_group_message")
