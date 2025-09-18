#!/usr/bin/env python3
"""
Simple Ping/Pong Demo

A simplified version of the ping/pong demo for testing.
"""

import asyncio
import time
import uuid
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from bus import Router
from agents import PingerAgent, PongerAgent
from loguru import logger


async def simple_ping_pong_demo(rounds: int = 3) -> None:
    """Run a simple ping/pong demo"""

    # Suppress logs for cleaner output
    logger.remove()

    print("Starting Simple Ping/Pong Demo...")
    print("=" * 40)

    # Create router and agents
    router = Router()
    pinger = PingerAgent(router)
    ponger = PongerAgent(router)

    # Start agents
    pinger_task = asyncio.create_task(pinger.run())
    ponger_task = asyncio.create_task(ponger.run())

    round_trip_times = []

    try:
        # Give agents time to start
        await asyncio.sleep(0.1)

        for round_num in range(rounds):
            print(f"\nRound {round_num + 1}:")

            # Reset pinger state
            pinger.reset()

            # Generate conversation ID
            conversation_id = str(uuid.uuid4())[:8]
            print(f"  Conversation ID: {conversation_id}")

            # Send ping
            start_time = time.time()
            success = await pinger.send_ping(conversation_id)

            if success:
                print(f"  >>> Ping sent")

                # Wait for pong response
                timeout = 1.0
                elapsed = 0
                while not pinger.response_received and elapsed < timeout:
                    await asyncio.sleep(0.001)
                    elapsed = time.time() - start_time

                if pinger.response_received:
                    round_trip_ms = elapsed * 1000
                    round_trip_times.append(round_trip_ms)

                    print(f"  <<< Pong received ({round_trip_ms:.2f}ms)")

                    # Verify correlation
                    response = pinger.response_message
                    assert response.conversation_id == conversation_id
                    assert response.performative.value == "inform"
                    assert response.content_type == "pong"

                    print(f"  Message correlation verified")
                else:
                    print(f"  Timeout after {elapsed:.2f}s")
            else:
                print(f"  Failed to send ping")

            # Small delay between rounds
            if round_num < rounds - 1:
                await asyncio.sleep(0.5)

    finally:
        # Stop agents
        pinger.stop()
        ponger.stop()

        # Cancel tasks
        pinger_task.cancel()
        ponger_task.cancel()

        try:
            await pinger_task
        except asyncio.CancelledError:
            pass

        try:
            await ponger_task
        except asyncio.CancelledError:
            pass

    # Print summary
    print("\n" + "=" * 40)
    print("Demo Summary:")

    if round_trip_times:
        avg_time = sum(round_trip_times) / len(round_trip_times)
        min_time = min(round_trip_times)
        max_time = max(round_trip_times)

        print(f"  * Successful round trips: {len(round_trip_times)}/{rounds}")
        print(f"  * Average time: {avg_time:.2f}ms")
        print(f"  * Fastest: {min_time:.2f}ms")
        print(f"  * Slowest: {max_time:.2f}ms")
        print(f"  * All under 200ms: {'Yes' if max_time < 200 else 'No'}")
        print("  * Demo completed successfully!")
    else:
        print("  * No successful round trips completed")


if __name__ == "__main__":
    asyncio.run(simple_ping_pong_demo())