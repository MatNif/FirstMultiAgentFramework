#!/usr/bin/env python3
"""
Test the exact CLI flow to identify the timeout issue
"""

import asyncio
import uuid
from loguru import logger

from agents import ChatAgent, DatabaseManagerAgent, TranslatorAgent
from bus import Message, Performative, Router
from db import DAO


class TestCEAAssistant:
    def __init__(self) -> None:
        self.router = Router()
        self.dao = DAO("test_cli_flow.db")
        self.agents: list = []
        self.response_received = False
        self.final_response = None

    async def initialize(self) -> None:
        """Initialize the assistant and its components"""
        logger.info("Initializing CEA Assistant...")

        # Initialize database
        await self.dao.initialize()

        # Create agents
        chat_agent = ChatAgent(self.router)
        dbm_agent = DatabaseManagerAgent(self.router, self.dao)
        translator_agent = TranslatorAgent(self.router)

        self.agents = [chat_agent, dbm_agent, translator_agent]

        # Set up response handler for chat agent
        @chat_agent.on("response")
        async def handle_response(message: Message) -> None:
            self.final_response = message.content.get("answer", "No response")
            self.response_received = True

        logger.info("CEA Assistant initialized successfully")

    async def start_agents(self) -> list:
        """Start all agents"""
        tasks = [asyncio.create_task(agent.run()) for agent in self.agents]
        await asyncio.sleep(0.1)  # Give agents time to start
        return tasks

    async def stop_agents(self) -> None:
        """Stop all agents"""
        for agent in self.agents:
            await agent.shutdown()

    async def _refresh_catalog(self) -> dict:
        """Refresh the script catalog"""
        logger.info("Starting refresh catalog...")

        conversation_id = str(uuid.uuid4())

        # Find dbm agent
        dbm_agent = None
        for agent in self.agents:
            logger.info(f"Checking agent: {agent.name}")
            if agent.name == "dbm":
                dbm_agent = agent
                break

        if not dbm_agent:
            logger.error("Database manager agent not found")
            return {"error": "Database manager agent not found"}

        logger.info(f"Found DBM agent: {dbm_agent}")

        # Create a temporary inbox for responses
        refresh_inbox = asyncio.Queue()

        # Register this temporary inbox to receive responses
        self.router.register_agent("refresh_handler", refresh_inbox)

        logger.info(f"Registered agents after adding handler: {self.router.get_agent_names()}")

        try:
            # Send refresh request
            refresh_message = Message.create(
                performative=Performative.REQUEST,
                sender="refresh_handler",
                receiver="dbm",
                conversation_id=conversation_id,
                content_type="refresh_catalog",
                content={},
            )

            logger.info(f"Sending refresh message: {refresh_message}")
            await self.router.route(refresh_message)

            # Wait for response
            timeout = 30.0  # 30 seconds timeout for catalog refresh
            try:
                logger.info("Waiting for response...")
                response_message = await asyncio.wait_for(refresh_inbox.get(), timeout=timeout)
                logger.info(f"Received response: {response_message}")

                if response_message.content_type == "catalog_refreshed":
                    return {"status": "completed", **response_message.content}
                elif response_message.content_type == "error":
                    return {"status": "error", **response_message.content}
                else:
                    return {"error": f"Unexpected response type: {response_message.content_type}"}

            except asyncio.TimeoutError:
                logger.error("Catalog refresh timed out")
                return {"error": "Catalog refresh timed out"}

        finally:
            # Clean up temporary inbox
            self.router.unregister_agent("refresh_handler")


async def test_cli_refresh():
    """Test the CLI refresh functionality"""

    # Enable detailed logging
    logger.add("test_cli_flow.log", level="DEBUG")

    print("Testing CLI refresh flow...")

    assistant = TestCEAAssistant()
    await assistant.initialize()

    # Start agents
    agent_tasks = await assistant.start_agents()

    try:
        # Test refresh
        refresh_result = await assistant._refresh_catalog()
        print(f"Refresh result: {refresh_result}")

    finally:
        # Stop agents
        await assistant.stop_agents()
        # Cancel agent tasks
        for task in agent_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(test_cli_refresh())