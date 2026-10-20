# Python Imports
import asyncio
import random
import logging
import string
from dataclasses import dataclass
from typing import List, Callable, Optional, Awaitable

# Project Imports
import src.logger
from src import kube_utils
from src.async_utils import CollectedItem, cleanup_queue_on_event
from src.setup_status import request_join_nodes_to_community, NodesInformation, \
    send_friend_requests, initialize_nodes_application
from src.status_backend import StatusBackend

logger = logging.getLogger(__name__)


Action = Callable[[NodesInformation, asyncio.Queue[CollectedItem|None], int], Awaitable[asyncio.Queue[float]]]


@dataclass(frozen=True)
class CommunitySetupResult:
    community_id: str
    chat_id: str
    join_delays: list[float]


@dataclass(frozen=True)
class RelayLightNodes:
    relay_nodes: dict[str, StatusBackend]
    light_nodes: dict[str, StatusBackend]
    status_nodes: dict[str, StatusBackend]
    relay_members: list[str]
    light_members: list[str]


async def initialize_relay_light_nodes(
        relay_count: int,
        light_count: int,
        namespace: str = "status-go-test",
        init_wait_seconds: int = 30,
) -> RelayLightNodes:
    kube_utils.setup_kubernetes_client()
    backend_relay_pods = kube_utils.get_pods("status-backend-relay", namespace)
    backend_light_pods = kube_utils.get_pods("status-backend-light", namespace)

    relay_nodes, light_nodes = await asyncio.gather(
        initialize_nodes_application(backend_relay_pods[:relay_count]),
        initialize_nodes_application(backend_light_pods[:light_count], wakuV2LightClient=True),
    )
    status_nodes = {**relay_nodes, **light_nodes}

    if init_wait_seconds:
        logger.info(f"Waiting {init_wait_seconds} seconds after nodes initialization")
        await asyncio.sleep(init_wait_seconds)

    relay_members = sorted(relay_nodes.keys())[:relay_count]
    light_members = sorted(light_nodes.keys())[:light_count]

    if len(relay_members) != relay_count or len(light_members) != light_count:
        raise RuntimeError(
            f"Expected {relay_count} relay and {light_count} light nodes, "
            f"got {len(relay_members)} relay and {len(light_members)} light"
        )

    return RelayLightNodes(
        relay_nodes=relay_nodes,
        light_nodes=light_nodes,
        status_nodes=status_nodes,
        relay_members=relay_members,
        light_members=light_members,
    )


async def shutdown_status_nodes(status_nodes: dict[str, StatusBackend]) -> None:
    if not status_nodes:
        return

    logger.info("Shutting down node connections")
    await asyncio.gather(
        *[node.shutdown() for node in status_nodes.values()],
        return_exceptions=True,
    )


async def create_community_util(status_nodes: NodesInformation, owner: str, to_include: List[str],
                                action: Action, intermediate_delay: int = 1,
                                consumers: int = 4) -> Optional[CommunitySetupResult]:
    """
    Utility function to create a community specifying and owner, and a list of nodes to send requests. Action will
    be performed by the invited nodes, which will answer to the requests with the action. At the moment,
    this can be either accept the request, reject the request, or simply ignore the request.
    Consumers represent the number of asyncio tasks that will answer to the requests with the action. Because this is
    heavily I/O dependent, there is no need to do fine granular concurrent operations.

    :param status_nodes: Nodes information for community setup
    :param owner: Node owner for community creation
    :param to_include: List of nodes to include in the community
    :param action: Optional action to perform during community setup
    :param intermediate_delay: Delay between community node requests in seconds
    :param consumers: Number of asyncio tasks that will answer to the requests with the action
    :return: Community setup result or None if setup fails
    """
    name = f"test_community_{''.join(random.choices(string.ascii_letters, k=10))}"
    logger.info(f"Creating community {name}")
    node_owner = status_nodes[owner]
    response = await node_owner.wakuext_service.create_community(name)
    community_id = response["result"]["communities"][0]["id"]
    chat_id = response["result"]["chats"][0]["id"]
    logger.info(f"Community {name} created with ID {community_id}")

    results_accept_queue: asyncio.Queue[CollectedItem | None] = asyncio.Queue()
    finished_accept_evt = asyncio.Event()

    send_to_accept_task = asyncio.create_task(
        request_join_nodes_to_community(status_nodes, results_accept_queue, to_include,
                                        community_id, finished_accept_evt,
                                        intermediate_delay))

    if action is None:
        return None

    accept_task = asyncio.create_task(action(node_owner, results_accept_queue, consumers))
    cleanup_task = asyncio.create_task(cleanup_queue_on_event(finished_accept_evt, results_accept_queue, consumers))

    _, delays_queue, _ = await asyncio.gather(send_to_accept_task, accept_task, cleanup_task)

    join_delays: list[float] = []
    while not delays_queue.empty():
        join_delays.append(delays_queue.get_nowait())

    logger.info(f"All nodes successfully joined community {community_id}. Delays are: {join_delays}")
    logger.info(f"Waiting 10 seconds")
    await asyncio.sleep(10)

    return CommunitySetupResult(community_id=community_id, chat_id=chat_id, join_delays=join_delays)


async def send_friend_requests_util(relay_nodes: NodesInformation, from_nodes, to_nodes, action: Action,
                                    cap_num_receivers: Optional[int] = None, consumers: int = 4) -> List[float]:
    results_queue: asyncio.Queue[CollectedItem | None] = asyncio.Queue()
    finished_evt = asyncio.Event()

    send_task = asyncio.create_task(
        send_friend_requests(relay_nodes, results_queue, from_nodes, to_nodes, finished_evt, cap_num_receivers))

    if action is None:
        return []

    accept_task = asyncio.create_task(action(relay_nodes, results_queue, consumers))
    cleanup_task = asyncio.create_task(cleanup_queue_on_event(finished_evt, results_queue, consumers))
    _, delays_queue, _ = await asyncio.gather(send_task, accept_task, cleanup_task)

    delays: list[float] = []
    while not delays_queue.empty():
        delays.append(delays_queue.get_nowait())

    return delays
