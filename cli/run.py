import asyncio
import json
import uuid
from pathlib import Path

import typer
from loguru import logger
from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.columns import Columns
from rich.align import Align

from agents import ChatAgent, DatabaseManagerAgent, QueryTranslatorAgent
from bus import Message, Performative, Router
from db import DAO, seed_database

app = typer.Typer(help="CEA Assistant - Multi-agent system for CEA helper bot")
console = Console()


class CEAAssistant:
    def __init__(self) -> None:
        self.router = Router()
        self.dao = DAO()
        self.agents: list = []

    async def initialize(self) -> None:
        """Initialize the assistant and its components"""
        logger.info("Initializing CEA Assistant...")

        # Initialize database
        await self.dao.initialize()

        # Check if database is empty and seed if needed
        scripts = await self.dao.search_scripts()
        if not scripts:
            logger.info("Database is empty, seeding with sample data...")
            await seed_database(self.dao)

        # Create agents
        chat_agent = ChatAgent(self.router)
        dbm_agent = DatabaseManagerAgent(self.router, self.dao)
        translator_agent = QueryTranslatorAgent(self.router, self.dao)

        self.agents = [chat_agent, dbm_agent, translator_agent]

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

    async def refresh_catalog(self) -> dict:
        """Refresh the script catalog"""
        logger.info("Starting catalog refresh...")

        conversation_id = str(uuid.uuid4())

        # Create a temporary inbox for responses
        refresh_inbox = asyncio.Queue()
        self.router.register_agent("refresh_handler", refresh_inbox)

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

            await self.router.route(refresh_message)

            # Wait for response
            timeout = 30.0  # 30 seconds timeout for catalog refresh
            try:
                response_message = await asyncio.wait_for(refresh_inbox.get(), timeout=timeout)

                if response_message.content_type == "catalog_refreshed":
                    return {"status": "completed", **response_message.content}
                elif response_message.content_type == "error":
                    return {"status": "error", **response_message.content}
                else:
                    return {"error": f"Unexpected response type: {response_message.content_type}"}

            except asyncio.TimeoutError:
                return {"error": "Catalog refresh timed out"}

        finally:
            # Clean up temporary inbox
            self.router.unregister_agent("refresh_handler")

    async def process_user_text(self, user_text: str, refresh_catalog: bool = False) -> dict:
        """Process user text and return the response"""
        conversation_id = str(uuid.uuid4())

        # Start agents
        agent_tasks = await self.start_agents()

        # Create a temporary inbox for responses
        query_inbox = asyncio.Queue()
        self.router.register_agent("query_handler", query_inbox)

        try:
            # Refresh catalog if requested
            if refresh_catalog:
                refresh_result = await self.refresh_catalog()
                if "error" in refresh_result:
                    return {
                        "type": "error",
                        "message": f"Catalog refresh failed: {refresh_result['error']}"
                    }

            # Send user_text to chat agent
            message = Message.create(
                performative=Performative.REQUEST,
                sender="query_handler",
                receiver="chat",
                conversation_id=conversation_id,
                content_type="user_text",
                content={"text": user_text},
            )

            await self.router.route(message)

            # Wait for response with timeout
            timeout = 10.0  # 10 seconds timeout
            try:
                response_message = await asyncio.wait_for(query_inbox.get(), timeout=timeout)

                if response_message.content_type == "plan":
                    # Handle plan response (success or failure)
                    plan_data = response_message.content.get("plan", {})
                    workflow_name = response_message.content.get("workflow_name", "Unknown")

                    if response_message.performative == Performative.FAILURE:
                        return {
                            "type": "failure",
                            "reason": response_message.content.get("reason", "Plan generation failed"),
                            "missing": response_message.content.get("missing", []),
                            "plan": plan_data
                        }
                    else:
                        return {
                            "type": "plan",
                            "plan": plan_data,
                            "workflow_name": workflow_name,
                            "workflow_id": response_message.content.get("workflow_id", "")
                        }

                elif response_message.content_type == "response":
                    # Handle regular response (FAQ, etc.)
                    return {
                        "type": "response",
                        "message": response_message.content.get("answer", "No response")
                    }
                else:
                    return {
                        "type": "error",
                        "message": f"Unexpected response type: {response_message.content_type}"
                    }

            except asyncio.TimeoutError:
                return {
                    "type": "error",
                    "message": "Timeout: No response received within 10 seconds"
                }

        finally:
            # Clean up temporary inbox
            self.router.unregister_agent("query_handler")
            # Stop agents
            await self.stop_agents()
            # Cancel agent tasks
            for task in agent_tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


def create_plan_table(plan_data: dict) -> Table:
    """Create a Rich table for execution plan steps"""
    table = Table(title="Execution Plan", show_header=True, header_style="bold magenta")
    table.add_column("Step", style="cyan", no_wrap=True, width=6)
    table.add_column("Script ID", style="green", width=25)
    table.add_column("Arguments", style="yellow")

    steps = plan_data.get("plan", [])
    for i, step in enumerate(steps, 1):
        script_id = step.get("script_id", "unknown")
        args = step.get("args", {})

        # Format arguments nicely
        if args:
            args_lines = []
            for k, v in args.items():
                args_lines.append(f"{k} = {v}")
            args_text = "\n".join(args_lines)
        else:
            args_text = "[dim]No arguments[/dim]"

        table.add_row(str(i), script_id, args_text)

    return table


def create_gaps_assumptions_panel(plan_data: dict, missing: list = None) -> Panel:
    """Create a Rich panel for gaps and assumptions"""
    content = Text()

    # Add missing inputs if any
    if missing:
        content.append("MISSING INPUTS:\n", style="bold red")
        for item in missing:
            content.append(f"  * {item}\n", style="red")
        content.append("\n")

    # Add assumptions
    assumptions = plan_data.get("assumptions", [])
    if assumptions:
        content.append("ASSUMPTIONS:\n", style="bold blue")
        for assumption in assumptions:
            content.append(f"  * {assumption}\n", style="blue")

    if not missing and not assumptions:
        content.append("No gaps or assumptions", style="green")

    return Panel(content, title="Gaps & Assumptions", border_style="blue")


def create_failure_panel(reason: str, missing: list) -> Panel:
    """Create a Rich panel for failure cases"""
    content = Text()
    content.append("PLAN GENERATION FAILED\n\n", style="bold red")
    content.append(f"Reason: {reason}\n\n", style="red")

    if missing:
        content.append("Missing Required Inputs:\n", style="bold red")
        for item in missing:
            content.append(f"  * {item}\n", style="red")

    return Panel(content, title="Execution Failed", border_style="red")


def pretty_print_plan(result: dict):
    """Pretty print a plan using Rich formatting"""

    if result["type"] == "failure":
        # Show failure panel
        failure_panel = create_failure_panel(
            result.get("reason", "Unknown error"),
            result.get("missing", [])
        )
        console.print(failure_panel)

    elif result["type"] == "plan":
        # Show successful plan
        plan_data = result.get("plan", {})
        workflow_name = result.get("workflow_name", "Unknown")

        # Show plan table
        table = create_plan_table(plan_data)
        console.print(table)
        console.print()

        # Show gaps and assumptions
        gaps_panel = create_gaps_assumptions_panel(plan_data)
        console.print(gaps_panel)

        # Show explanation
        explain = plan_data.get("explain", "")
        if explain:
            console.print()
            explanation_panel = Panel(explain, title=f"Workflow: {workflow_name}", border_style="green")
            console.print(explanation_panel)


async def run_assistant(user_text: str, refresh: bool = False) -> dict:
    """Run the CEA assistant with a given user text"""
    assistant = CEAAssistant()
    await assistant.initialize()
    return await assistant.process_user_text(user_text, refresh_catalog=refresh)


@app.command()
def main(
    user_text: str = typer.Argument(..., help="Your question or request for the CEA assistant"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    json_output: bool = typer.Option(False, "--json", help="Output raw JSON instead of pretty formatting"),
    refresh: bool = typer.Option(False, "--refresh", help="Refresh script catalog before processing"),
) -> None:
    """
    CEA Assistant - Ask questions about CEA scripts and workflows

    Examples:
        cea-assistant "estimate district cooling demand from zone.geojson and weather.epw"
        cea-assistant "design cost optimal cooling system" --json
        cea-assistant "what scripts estimate cooling demand?" --refresh
    """
    # Configure logging
    if verbose:
        logger.add(
            "cea_assistant.log",
            rotation="10 MB",
            level="DEBUG",
            format="{time} | {level} | {message}",
        )
    else:
        logger.remove()
        logger.add(lambda msg: None)  # Suppress logs in non-verbose mode

    try:
        # Run the assistant
        result = asyncio.run(run_assistant(user_text, refresh))

        if json_output:
            # Raw JSON output
            console.print(JSON.from_data(result))
        else:
            # Pretty formatted output
            if result["type"] in ["failure", "plan"]:
                # Plan or failure response
                pretty_print_plan(result)

            elif result["type"] == "response":
                # Regular response (FAQ, etc.)
                response_panel = Panel(result["message"], title="Response", border_style="green")
                console.print(response_panel)

            elif result["type"] == "error":
                # Error case
                error_panel = Panel(result["message"], title="Error", border_style="red")
                console.print(error_panel)

    except Exception as e:
        error_msg = f"Error: {str(e)}"
        if json_output:
            result = {
                "type": "error",
                "message": error_msg,
                "user_text": user_text,
            }
            console.print(JSON.from_data(result))
        else:
            error_panel = Panel(error_msg, title="System Error", border_style="red")
            console.print(error_panel)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()