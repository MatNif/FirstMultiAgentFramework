import asyncio
import uuid
from typing import Any, Callable, Dict, Optional

from loguru import logger

from bus import Message, Performative, Router


class BaseAgent:
    def __init__(self, name: str, router: Router) -> None:
        self.name = name
        self.router = router
        self.inbox: asyncio.Queue[Message] = asyncio.Queue()
        self._handlers: Dict[str, Callable[[Message], Any]] = {}
        self._running = False

        router.register_agent(name, self.inbox)

    def on(self, content_type: str) -> Callable[[Callable[[Message], Any]], Callable[[Message], Any]]:
        def decorator(handler: Callable[[Message], Any]) -> Callable[[Message], Any]:
            self._handlers[content_type] = handler
            logger.debug(f"Registered handler for content type '{content_type}' in agent '{self.name}'")
            return handler
        return decorator

    async def send(
        self,
        receiver: str,
        performative: Performative,
        content_type: str,
        content: Dict[str, Any],
        conversation_id: Optional[str] = None,
    ) -> bool:
        if conversation_id is None:
            conversation_id = str(uuid.uuid4())

        message = Message.create(
            performative=performative,
            sender=self.name,
            receiver=receiver,
            conversation_id=conversation_id,
            content_type=content_type,
            content=content,
        )

        return await self.router.route(message)

    async def reply(
        self,
        original_message: Message,
        performative: Performative,
        content_type: str,
        content: Dict[str, Any],
    ) -> bool:
        reply_message = original_message.reply(
            performative=performative,
            content_type=content_type,
            content=content,
            sender=self.name,
        )

        return await self.router.route(reply_message)

    async def handle_message(self, message: Message) -> None:
        handler = self._handlers.get(message.content_type)
        if handler:
            try:
                await handler(message)
            except Exception as e:
                logger.error(f"Error handling message in {self.name}: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": str(e), "original_content_type": message.content_type},
                )
        else:
            logger.warning(
                f"No handler for content type '{message.content_type}' in agent '{self.name}'"
            )

    async def run(self) -> None:
        self._running = True
        logger.info(f"Agent {self.name} started")

        while self._running:
            try:
                message = await asyncio.wait_for(self.inbox.get(), timeout=1.0)
                await self.handle_message(message)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in agent {self.name}: {e}")

        logger.info(f"Agent {self.name} stopped")

    def stop(self) -> None:
        self._running = False

    async def shutdown(self) -> None:
        self.stop()
        self.router.unregister_agent(self.name)