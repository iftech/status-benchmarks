# Python Imports
import logging
import json
import time
from typing import List, Dict, cast
from aiohttp import ClientSession, ClientTimeout

# Project Imports
from src.account_service import AccountAsyncService
from src.enums import SignalType
from src.logger import TraceLogger
from src.rpc_client import AsyncRpcClient
from src.signal_client import AsyncSignalClient
from src.wakuext_service import WakuextAsyncService
from src.wallet_service import WalletAsyncService

logger = cast(TraceLogger, logging.getLogger(__name__))


class StatusBackend:
    def __init__(self, url: str, await_signals: List[str] = None):
        self.base_url = url
        self.api_url = f"{url}/statusgo"
        self.ws_url = url.replace("http", "ws")
        self.rpc_url = f"{url}/statusgo/CallRPC"
        self.public_key = ""

        self.rpc = AsyncRpcClient(self.rpc_url)
        self.signal = AsyncSignalClient(self.ws_url, await_signals)
        self.session = ClientSession(timeout=ClientTimeout(total=10))

        self.wakuext_service = WakuextAsyncService(self.rpc)
        self.wallet_service = WalletAsyncService(self.rpc)
        self.accounts_service = AccountAsyncService(self.rpc)

    async def __aenter__(self):
        await self.rpc.__aenter__()
        await self.signal.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Let the caller handle shutdown
        pass

    async def shutdown(self):
        await self.logout()
        await self.signal.__aexit__(None, None, None)
        await self.rpc.__aexit__(None, None, None)
        await self.session.close()

    async def call_rpc(self, method: str, params: List = None):
        return await self.rpc.rpc_valid_request(method, params or [])

    async def api_request(self, method: str, data: Dict) -> dict:
        url = f"{self.api_url}/{method}"
        logger.trace(f"Sending POST to {url} with data: {data}")
        async with self.session.post(url, json=data) as response:
            logger.trace(f"Received response from {method}: {response.status}")

            if response.status != 200:
                body = await response.text()
                raise AssertionError(f"Bad HTTP status: {response.status}, body: {body}")

            try:
                json_data = await response.json()
            except json.JSONDecodeError:
                body = await response.text()
                raise AssertionError(f"Invalid JSON in response: {body}")

            if json_data.get("error"):
                raise AssertionError(f"API error: {json_data['error']}")

            return json_data

    async def api_valid_request(self, method: str, data: Dict) -> dict:
        json_data = await self.api_request(method, data)
        logger.trace(f"Valid response from {method}: {json_data}")
        return json_data

    async def start_status_backend(self) -> dict:
        await self.__aenter__()
        try:
            await self.logout()
        except AssertionError:
            logger.debug(f"Failed to log out in {self.base_url}")

        method = "InitializeApplication"
        data = {
            "dataDir": "/usr/status-user",
            "logEnabled": True,
            "logLevel": "DEBUG",
            "apiLogging": True,
            "wakuFleetsConfigFilePath": "/static/configs/config.json"
            # TODO check wakuFleetsConfigFilePath?
        }
        return await self.api_valid_request(method, data)

    def _set_networks(self, data: Dict):
        anvil_network = {
            "ChainID": 31337,
            "ChainName": "Anvil",
            "IsActive": True,
            "IsTest": False,
            "Layer": 1,
            "NativeCurrencyDecimals": 18,
            "NativeCurrencyName": "Ether",
            "NativeCurrencySymbol": "ETH",
            "RpcProviders": [{
                "authType": "no-auth",
                "chainId": 31337,
                "enableRpsLimiter": False,
                "enabled": True,
                "name": "Anvil Direct",
                "type": "embedded-direct",
                "url": "http://127.0.0.1:8545"
            }],
            "ShortName": "eth"
        }
        data["testNetworksEnabled"] = False
        data["networkId"] = 31337
        data["networksOverride"] = [anvil_network]

    def _create_account_request(self, **kwargs) -> Dict:
        data = {
            "rootDataDir": "/usr/status-user",
            "kdfIterations": 256000,
            # Profile config
            "displayName": "",
            "password": "Strong12345", # TODO SAVE PASSWORDS FOR DIFFERENT USERS?
            "customizationColor": "red",
            # Logs config
            "logEnabled": True,
            "logToStderr": True,
            "logLevel": "DEBUG",
            # Waku config
            "wakuV2LightClient": kwargs.get("wakuV2LightClient", False),
            "wakuV2Fleet": "dst.dev",
        }
        self._set_networks(data)
        return data

    async def create_account_and_login(self, **kwargs) -> dict | None:
        response = await self.api_valid_request("CreateAccountAndLogin", self._create_account_request(**kwargs))

        signal = await self.signal.wait_for_login()

        self.set_public_key(signal)
        self.signal.node_login_event = signal
        return response

    async def login(self, key_uid: str) -> dict:
        response = await self.api_valid_request("LoginAccount", {
            "password": "Strong12345",
            "keyUid": key_uid,
            "kdfIterations": 256000,
        })
        signal = await self.signal.wait_for_login()
        self.set_public_key(signal)
        self.last_login = int(time.time())
        return response

    async def logout(self, clean_signals = False) -> dict:
        json_response = await self.api_valid_request("Logout", {})
        _ = await self.signal.wait_for_logout()
        logger.debug(f"Successfully logged out in {self.base_url}")
        if clean_signals:
            self.signal.cleanup_signal_queues()

        return json_response

    def set_public_key(self, signal_data: dict):
        self.public_key = signal_data.get("event", {}).get("settings", {}).get("public-key")

    def find_key_uid(self) -> str:
        recent = self.signal.get_recent_signals(SignalType.NODE_LOGIN.value)
        if not recent:
            raise RuntimeError("No login signal received to extract key UID")
        return recent[-1].get("event", {}).get("account", {}).get("key-uid")
