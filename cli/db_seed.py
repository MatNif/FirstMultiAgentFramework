#!/usr/bin/env python3
"""
Database Seeding CLI Tool

CLI tool to create, seed, and inspect the CEA assistant database.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from loguru import logger

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import DAO
from db.seed import seed_database, print_database_contents
from db.models import Script, Workflow

app = typer.Typer(help="CEA Assistant Database Seeding Tool")
console = Console()


@app.command()
def create(
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
    force: bool = typer.Option(False, "--force", "-f", help="Force recreate if database exists"),
) -> None:
    """Create database tables from schema.sql"""

    async def _create() -> None:
        dao = DAO(db_path)

        if force:
            console.print(f"[yellow]Recreating database tables in {db_path}[/yellow]")
            await dao.recreate_tables()
        else:
            console.print(f"[blue]Creating database tables in {db_path}[/blue]")
            await dao.initialize()

        console.print("[green]Database tables created successfully[/green]")

    asyncio.run(_create())


@app.command()
def seed(
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
    recreate: bool = typer.Option(True, "--recreate/--no-recreate", help="Recreate tables before seeding"),
) -> None:
    """Seed database with example CEA scripts and workflows"""

    async def _seed() -> None:
        dao = DAO(db_path)

        if recreate:
            console.print(f"[yellow]Recreating and seeding database: {db_path}[/yellow]")
            await dao.recreate_tables()
        else:
            console.print(f"[blue]Seeding existing database: {db_path}[/blue]")
            await dao.initialize()

        # Suppress loguru logs for cleaner output
        logger.remove()

        await seed_database(dao)

        console.print("[green]Database seeded successfully[/green]")

        # Show summary
        scripts = await dao.get_all_scripts()
        workflows = await dao.get_all_workflows()
        console.print(f"[cyan]Seeded {len(scripts)} scripts and {len(workflows)} workflows[/cyan]")

    asyncio.run(_seed())


@app.command()
def show(
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
    scripts: bool = typer.Option(True, "--scripts/--no-scripts", help="Show scripts"),
    workflows: bool = typer.Option(True, "--workflows/--no-workflows", help="Show workflows"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table or text"),
) -> None:
    """Show all scripts and workflows in the database"""

    async def _show() -> None:
        dao = DAO(db_path)

        try:
            await dao.initialize()
        except Exception as e:
            console.print(f"[red]Error accessing database: {e}[/red]")
            console.print(f"[yellow]Try running: python -m cli.db_seed create -d {db_path}[/yellow]")
            return

        if format == "text":
            # Use the existing text-based display
            await print_database_contents(dao)
            return

        # Rich table format
        if scripts:
            scripts_list = await dao.get_all_scripts()

            if scripts_list:
                console.print(f"\n[bold cyan]SCRIPTS ({len(scripts_list)} total)[/bold cyan]")

                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Name", style="cyan", no_wrap=True)
                table.add_column("Tags", style="yellow")
                table.add_column("Description", style="white")
                table.add_column("I/O", style="green", justify="center")

                for script in scripts_list:
                    tags_str = ", ".join(script.tags[:3])  # Show first 3 tags
                    if len(script.tags) > 3:
                        tags_str += f" (+{len(script.tags) - 3})"

                    description = script.doc[:60] + "..." if script.doc and len(script.doc) > 60 else script.doc or ""
                    io_count = f"{len(script.inputs)}/{len(script.outputs)}"

                    table.add_row(script.name, tags_str, description, io_count)

                console.print(table)
            else:
                console.print("[yellow]No scripts found in database[/yellow]")

        if workflows:
            workflows_list = await dao.get_all_workflows()

            if workflows_list:
                console.print(f"\n[bold cyan]WORKFLOWS ({len(workflows_list)} total)[/bold cyan]")

                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Name", style="cyan", no_wrap=True)
                table.add_column("Steps", style="green", justify="center")
                table.add_column("Tags", style="yellow")
                table.add_column("Description", style="white")

                for workflow in workflows_list:
                    tags_str = ", ".join(workflow.tags[:3])  # Show first 3 tags
                    if len(workflow.tags) > 3:
                        tags_str += f" (+{len(workflow.tags) - 3})"

                    description = workflow.description[:50] + "..." if workflow.description and len(workflow.description) > 50 else workflow.description or ""

                    table.add_row(workflow.name, str(len(workflow.steps)), tags_str, description)

                console.print(table)
            else:
                console.print("[yellow]No workflows found in database[/yellow]")

    asyncio.run(_show())


@app.command()
def search_scripts(
    tags: Optional[str] = typer.Option(None, "--tags", "-t", help="Comma-separated tags to search for"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Script name to search for"),
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
) -> None:
    """Search for scripts by tags or name"""

    async def _search() -> None:
        dao = DAO(db_path)
        await dao.initialize()

        if tags:
            tag_list = [tag.strip() for tag in tags.split(",")]
            scripts = await dao.find_scripts_by_tags(tag_list)
            console.print(f"[cyan]Found {len(scripts)} scripts matching tags: {tag_list}[/cyan]")
        elif name:
            from db.models import ScriptSearchCriteria
            criteria = ScriptSearchCriteria(name=name)
            scripts = await dao.search_scripts(criteria)
            console.print(f"[cyan]Found {len(scripts)} scripts matching name: '{name}'[/cyan]")
        else:
            console.print("[yellow]Please provide either --tags or --name parameter[/yellow]")
            return

        if scripts:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Name", style="cyan")
            table.add_column("Tags", style="yellow")
            table.add_column("Description", style="white")

            for script in scripts:
                tags_str = ", ".join(script.tags)
                description = script.doc[:80] + "..." if script.doc and len(script.doc) > 80 else script.doc or ""
                table.add_row(script.name, tags_str, description)

            console.print(table)
        else:
            console.print("[yellow]No scripts found matching criteria[/yellow]")

    asyncio.run(_search())


@app.command()
def stats(
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
) -> None:
    """Show database statistics"""

    async def _stats() -> None:
        dao = DAO(db_path)
        await dao.initialize()

        scripts = await dao.get_all_scripts()
        workflows = await dao.get_all_workflows()

        # Calculate statistics
        total_scripts = len(scripts)
        total_workflows = len(workflows)

        # Tag statistics
        all_script_tags = []
        for script in scripts:
            all_script_tags.extend(script.tags)

        unique_script_tags = set(all_script_tags)

        # Input/Output statistics
        total_inputs = sum(len(script.inputs) for script in scripts)
        total_outputs = sum(len(script.outputs) for script in scripts)

        # Workflow steps
        total_steps = sum(len(workflow.steps) for workflow in workflows)

        console.print(f"\n[bold cyan]DATABASE STATISTICS[/bold cyan]")
        console.print("-" * 40)
        console.print(f"Scripts: {total_scripts}")
        console.print(f"Workflows: {total_workflows}")
        console.print(f"Total workflow steps: {total_steps}")
        console.print(f"Unique script tags: {len(unique_script_tags)}")
        console.print(f"Total script inputs: {total_inputs}")
        console.print(f"Total script outputs: {total_outputs}")

        if unique_script_tags:
            console.print(f"\nMost common tags:")
            from collections import Counter
            tag_counts = Counter(all_script_tags)
            for tag, count in tag_counts.most_common(5):
                console.print(f"  {tag}: {count}")

    asyncio.run(_stats())


@app.command()
def reset(
    db_path: str = typer.Option("cea_assistant.db", "--db-path", "-d", help="Database file path"),
    confirm: bool = typer.Option(False, "--confirm", help="Confirm database reset"),
) -> None:
    """Reset database (drop all data and recreate with fresh seed)"""

    if not confirm:
        console.print("[yellow]This will delete all data in the database![/yellow]")
        console.print(f"[yellow]Run with --confirm to reset: {db_path}[/yellow]")
        return

    async def _reset() -> None:
        dao = DAO(db_path)

        console.print(f"[red]Resetting database: {db_path}[/red]")

        # Suppress loguru logs for cleaner output
        logger.remove()

        await dao.recreate_tables()
        await seed_database(dao)

        scripts = await dao.get_all_scripts()
        workflows = await dao.get_all_workflows()

        console.print("[green]Database reset and seeded successfully[/green]")
        console.print(f"[cyan]Contains {len(scripts)} scripts and {len(workflows)} workflows[/cyan]")

    asyncio.run(_reset())


if __name__ == "__main__":
    app()