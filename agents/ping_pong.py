import asyncio
import time
from typing import Optional

from loguru import logger

from bus import Message, Performative, Router
from .base import BaseAgent


class PingerAgent(BaseAgent):
    def __init__(self, router: Router) -> None:
        super().__init__("pinger", router)
        self.response_received = False
        self.response_message: Optional[Message] = None
        self.ping_sent_time: Optional[float] = None
        self.setup_handlers()

    def setup_handlers(self) -> None:
        @self.on("pong")
        async def handle_pong(message: Message) -> None:
            logger.info(f"PingerAgent received pong: {message.content}")
            self.response_received = True
            self.response_message = message

    async def send_ping(self, conversation_id: Optional[str] = None) -> bool:
        self.ping_sent_time = time.time()
        return await self.send(
            receiver="ponger",
            performative=Performative.REQUEST,
            content_type="ping",
            content={"message": "ping", "timestamp": self.ping_sent_time},
            conversation_id=conversation_id,
        )

    def reset(self) -> None:
        self.response_received = False
        self.response_message = None
        self.ping_sent_time = None


class PongerAgent(BaseAgent):
    def __init__(self, router: Router) -> None:
        super().__init__("ponger", router)
        self.setup_handlers()

    def setup_handlers(self) -> None:
        @self.on("ping")
        async def handle_ping(message: Message) -> None:
            logger.info(f"PongerAgent received ping: {message.content}")

            # Reply with pong
            await self.reply(
                message,
                Performative.INFORM,
                "pong",
                {
                    "message": "pong",
                    "original_timestamp": message.content.get("timestamp"),
                    "reply_timestamp": time.time(),
                },
            )