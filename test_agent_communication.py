#!/usr/bin/env python3
"""
Test agent communication for refresh catalog functionality
"""

import asyncio
import uuid
from loguru import logger

from agents import DatabaseManagerAgent
from bus import Message, Performative, Router
from db import DAO


async def test_refresh_communication():
    """Test direct communication with DatabaseManagerAgent"""
    print("Testing agent communication...")

    # Enable detailed logging
    logger.add("test_communication.log", level="DEBUG")

    # Initialize components
    router = Router()
    dao = DAO("test_communication.db")
    await dao.initialize()

    # Create and start agent
    dbm_agent = DatabaseManagerAgent(router, dao)

    # Start agent in background
    agent_task = asyncio.create_task(dbm_agent.run())
    await asyncio.sleep(0.1)  # Let agent start

    # Create temporary handler
    response_queue = asyncio.Queue()
    router.register_agent("test_handler", response_queue)

    print(f"Registered agents: {router.get_agent_names()}")

    try:
        # Send refresh request
        conversation_id = str(uuid.uuid4())
        message = Message.create(
            performative=Performative.REQUEST,
            sender="test_handler",
            receiver="dbm",
            conversation_id=conversation_id,
            content_type="refresh_catalog",
            content={},
        )

        print(f"Sending message: {message}")
        success = await router.route(message)
        print(f"Message routed successfully: {success}")

        # Wait for response with timeout
        try:
            response = await asyncio.wait_for(response_queue.get(), timeout=15.0)
            print(f"Received response: {response}")
            print(f"Response type: {response.content_type}")
            print(f"Response content: {response.content}")

        except asyncio.TimeoutError:
            print("TIMEOUT: No response received")

    finally:
        # Cleanup
        router.unregister_agent("test_handler")
        await dbm_agent.shutdown()
        agent_task.cancel()

        try:
            await agent_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(test_refresh_communication())