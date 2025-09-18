import asyncio
from typing import Dict, Optional

from loguru import logger

from .messages import Message


class Router:
    def __init__(self) -> None:
        self._agents: Dict[str, asyncio.Queue[Message]] = {}

    def register_agent(self, name: str, inbox: asyncio.Queue[Message]) -> None:
        self._agents[name] = inbox
        logger.info(f"Registered agent: {name}")

    def unregister_agent(self, name: str) -> None:
        if name in self._agents:
            del self._agents[name]
            logger.info(f"Unregistered agent: {name}")

    async def route(self, message: Message) -> bool:
        receiver_inbox = self._agents.get(message.receiver)
        if receiver_inbox is None:
            logger.warning(f"Agent '{message.receiver}' not found for message routing")
            return False

        try:
            await receiver_inbox.put(message)
            logger.debug(
                f"Routed message from {message.sender} to {message.receiver} "
                f"(type: {message.content_type})"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to route message to {message.receiver}: {e}")
            return False

    def get_agent_names(self) -> list[str]:
        return list(self._agents.keys())

    def is_agent_registered(self, name: str) -> bool:
        return name in self._agents