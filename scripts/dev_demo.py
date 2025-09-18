#!/usr/bin/env python3
"""
Ping/Pong Demo Script

Demonstrates the multi-agent messaging system with a visual ping/pong demo.
Shows real-time message exchange between two agents with timing information.
"""

import asyncio
import time
import uuid
from typing import List, Tuple

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.align import Align
from loguru import logger

# Add project root to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from bus import Router
from agents import PingerAgent, PongerAgent

console = Console()


class PingPongDemo:
    def __init__(self) -> None:
        self.router = Router()
        self.pinger = PingerAgent(self.router)
        self.ponger = PongerAgent(self.router)
        self.message_history: List[Tuple[float, str, str, str]] = []
        self.round_trip_times: List[float] = []
        self.running = False

    def create_display(self) -> Layout:
        """Create the rich display layout"""
        layout = Layout()

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=8)
        )

        layout["main"].split_row(
            Layout(name="agents", ratio=1),
            Layout(name="messages", ratio=2)
        )

        # Header
        layout["header"].update(
            Panel(
                Align.center(
                    Text("Ping/Pong Multi-Agent Demo", style="bold cyan"),
                    vertical="middle"
                ),
                style="bright_blue"
            )
        )

        # Agent status
        agent_table = Table(title="Agent Status", show_header=True)
        agent_table.add_column("Agent", style="cyan")
        agent_table.add_column("Status", style="green")
        agent_table.add_column("Messages", style="yellow")

        pinger_status = "Running" if self.running else "Stopped"
        ponger_status = "Running" if self.running else "Stopped"

        pinger_msgs = len([m for m in self.message_history if m[1] == "pinger"])
        ponger_msgs = len([m for m in self.message_history if m[1] == "ponger"])

        agent_table.add_row("Pinger", pinger_status, str(pinger_msgs))
        agent_table.add_row("Ponger", ponger_status, str(ponger_msgs))

        layout["agents"].update(Panel(agent_table, title="Agents"))

        # Message history
        msg_table = Table(title="Message History", show_header=True)
        msg_table.add_column("Time", style="dim")
        msg_table.add_column("From", style="cyan")
        msg_table.add_column("Type", style="magenta")
        msg_table.add_column("Content", style="white")

        # Show last 10 messages
        for timestamp, sender, msg_type, content in self.message_history[-10:]:
            time_str = f"{timestamp:.3f}s"
            prefix = ">>>" if msg_type == "ping" else "<<<"
            msg_table.add_row(time_str, sender, f"{prefix} {msg_type}", content)

        layout["messages"].update(Panel(msg_table, title="Messages"))

        # Statistics
        stats_table = Table(show_header=False)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="yellow")

        total_msgs = len(self.message_history)
        avg_time = sum(self.round_trip_times) / len(self.round_trip_times) if self.round_trip_times else 0
        min_time = min(self.round_trip_times) if self.round_trip_times else 0
        max_time = max(self.round_trip_times) if self.round_trip_times else 0

        stats_table.add_row("Total Messages", str(total_msgs))
        stats_table.add_row("Round Trips", str(len(self.round_trip_times)))
        stats_table.add_row("Avg Round-Trip", f"{avg_time:.1f}ms")
        stats_table.add_row("Min Round-Trip", f"{min_time:.1f}ms")
        stats_table.add_row("Max Round-Trip", f"{max_time:.1f}ms")

        layout["footer"].update(Panel(stats_table, title="Statistics"))

        return layout

    async def run_demo(self, rounds: int = 5, delay: float = 1.0) -> None:
        """Run the ping/pong demo with visual display"""

        # Suppress loguru logs for cleaner demo
        logger.remove()

        # Start agents
        pinger_task = asyncio.create_task(self.pinger.run())
        ponger_task = asyncio.create_task(self.ponger.run())
        self.running = True

        try:
            with Live(self.create_display(), refresh_per_second=10, screen=True) as live:
                # Give agents time to start
                await asyncio.sleep(0.1)

                for round_num in range(rounds):
                    # Reset pinger state
                    self.pinger.reset()

                    # Generate conversation ID
                    conversation_id = str(uuid.uuid4())[:8]  # Short ID for display

                    # Send ping
                    start_time = time.time()
                    await self.pinger.send_ping(conversation_id)

                    # Record ping message
                    self.message_history.append((
                        time.time() - start_time,
                        "pinger",
                        "ping",
                        f"Round {round_num + 1} (ID: {conversation_id})"
                    ))

                    # Wait for pong response
                    timeout = 1.0
                    elapsed = 0
                    while not self.pinger.response_received and elapsed < timeout:
                        await asyncio.sleep(0.01)
                        elapsed = time.time() - start_time
                        live.update(self.create_display())

                    if self.pinger.response_received:
                        # Record pong message and round-trip time
                        round_trip_ms = elapsed * 1000
                        self.round_trip_times.append(round_trip_ms)

                        self.message_history.append((
                            elapsed,
                            "ponger",
                            "pong",
                            f"Reply to Round {round_num + 1} ({round_trip_ms:.1f}ms)"
                        ))
                    else:
                        self.message_history.append((
                            elapsed,
                            "system",
                            "timeout",
                            f"Round {round_num + 1} timed out"
                        ))

                    # Update display
                    live.update(self.create_display())

                    # Wait before next round
                    if round_num < rounds - 1:
                        await asyncio.sleep(delay)

                # Final display with complete stats
                self.running = False
                live.update(self.create_display())

                # Show final summary
                await asyncio.sleep(2)

        finally:
            # Stop agents
            self.pinger.stop()
            self.ponger.stop()

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

    def print_summary(self) -> None:
        """Print final summary"""
        console.print("\n" + "="*60, style="bright_blue")
        console.print("Ping/Pong Demo Summary", style="bold cyan", justify="center")
        console.print("="*60, style="bright_blue")

        if self.round_trip_times:
            avg_time = sum(self.round_trip_times) / len(self.round_trip_times)
            min_time = min(self.round_trip_times)
            max_time = max(self.round_trip_times)

            console.print(f"\nPerformance Metrics:")
            console.print(f"  * Total round trips: {len(self.round_trip_times)}")
            console.print(f"  * Average time: {avg_time:.2f}ms")
            console.print(f"  * Fastest: {min_time:.2f}ms")
            console.print(f"  * Slowest: {max_time:.2f}ms")
            console.print(f"  * All under 200ms: {'Yes' if max_time < 200 else 'No'}")

            console.print(f"\nDemo completed successfully!")
        else:
            console.print("\nNo successful round trips completed")

        console.print()


async def main() -> None:
    """Main demo function"""
    console.print("Starting Ping/Pong Multi-Agent Demo...\n")

    demo = PingPongDemo()

    try:
        await demo.run_demo(rounds=10, delay=0.8)
        demo.print_summary()
    except KeyboardInterrupt:
        console.print("\nDemo interrupted by user")
    except Exception as e:
        console.print(f"\nDemo failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())