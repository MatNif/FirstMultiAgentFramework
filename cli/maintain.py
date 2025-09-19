"""
CEA Assistant Database Maintenance CLI

Provides comprehensive database maintenance operations including:
- Backup and restore
- Schema migrations
- Data integrity checks
- Orphan cleanup
- Data canonicalization
- Performance optimization

All destructive operations support --dry-run (default) and --apply modes.
"""

import asyncio
import json
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import typer
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config import get_settings
from db.migrations import MigrationManager

app = typer.Typer(
    name="cea-maintain",
    help="CEA Assistant database maintenance operations",
    rich_markup_mode="rich"
)
console = Console()

# Global options that apply to all commands
DatabaseOption = typer.Option(
    None,
    "--db",
    help="Path to SQLite database file (default: from config)"
)
DryRunOption = typer.Option(
    True,
    "--dry-run/--apply",
    help="Show what would be done (dry-run) or execute changes (apply)"
)
ConversationIdOption = typer.Option(
    None,
    "--conversation-id",
    help="Conversation ID for structured logging"
)


class MaintenanceContext:
    """Context manager for database maintenance operations"""

    def __init__(self, db_path: str, conversation_id: Optional[str] = None):
        self.db_path = Path(db_path)
        self.conversation_id = conversation_id or f"maint_{int(time.time())}"
        self.conn: Optional[sqlite3.Connection] = None

        # Setup structured logging with conversation ID
        settings = get_settings()
        settings.setup_logging(self.conversation_id)

    def __enter__(self) -> sqlite3.Connection:
        """Open database connection with optimizations"""
        logger.info(f"Opening database: {self.db_path}")

        if not self.db_path.exists():
            raise typer.BadParameter(f"Database file not found: {self.db_path}")

        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        # Enable foreign keys and optimizations
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")

        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def get_db_path(db_path: Optional[str] = None) -> str:
    """Get database path from parameter or config"""
    if db_path:
        return db_path

    settings = get_settings()
    return str(settings.get_db_path())


def create_backup_dir() -> Path:
    """Create backups directory if it doesn't exist"""
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    return backup_dir


@app.command()
def backup(
    db_path: Optional[str] = DatabaseOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> str:
    """
    Create a timestamped backup of the database.

    Returns the path to the created backup file.
    """
    db_file = Path(get_db_path(db_path))

    if not db_file.exists():
        console.print(f"[red]Error: Database file not found: {db_file}[/red]")
        raise typer.Exit(1)

    # Create backup directory
    backup_dir = create_backup_dir()

    # Generate timestamped backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"cea_{timestamp}.sqlite"
    backup_path = backup_dir / backup_name

    try:
        # Copy database file
        shutil.copy2(db_file, backup_path)

        # Log the backup
        logger.info(f"Database backup created: {backup_path}")
        console.print(f"[green]✓[/green] Backup created: {backup_path}")

        return str(backup_path)

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        console.print(f"[red]✗[/red] Backup failed: {e}")
        raise typer.Exit(1)


@app.command()
def migrate(
    db_path: Optional[str] = DatabaseOption,
    dry_run: bool = DryRunOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Apply schema migrations to bring database to latest version.

    Creates backup before applying changes when --apply is used.
    """
    db_file = get_db_path(db_path)

    async def run_migration():
        migration_manager = MigrationManager(db_file)

        # Check if migration is needed
        needs_migration = await migration_manager.needs_migration()
        current_version = await migration_manager.get_schema_version()
        target_version = migration_manager.get_target_version()

        console.print(f"Current schema version: {current_version}")
        console.print(f"Target schema version: {target_version}")

        if not needs_migration:
            console.print("[green]✓[/green] Database is already up to date")
            return

        # Run migrations
        try:
            if not dry_run:
                # Create backup before migration
                backup_path = backup(db_path, conversation_id)
                console.print(f"Backup created: {backup_path}")

            console.print(f"\n[yellow]{'Migration Plan (DRY RUN):' if dry_run else 'Applying migrations:'}[/yellow]")

            operations = await migration_manager.migrate(dry_run=dry_run)
            for operation in operations:
                console.print(f"  {operation}")

            if not dry_run:
                console.print("[green]✓[/green] Migration completed successfully")
                logger.info("Database migration completed")

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            console.print(f"[red]✗[/red] Migration failed: {e}")
            if not dry_run:
                console.print("Database backup is available for recovery")
            raise typer.Exit(1)

    # Run the async migration
    asyncio.run(run_migration())


@app.command()
def integrity(
    db_path: Optional[str] = DatabaseOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Run comprehensive database integrity checks.

    Includes SQLite PRAGMA checks and custom referential integrity validation.
    """
    db_file = get_db_path(db_path)

    async def run_integrity_check():
        migration_manager = MigrationManager(db_file)

        try:
            console.print("[blue]Running database integrity checks...[/blue]")

            issues = await migration_manager.check_integrity()
            for issue in issues:
                console.print(f"  {issue}")

            if all(issue.startswith("✓") for issue in issues):
                console.print("[green]✓[/green] All integrity checks passed")
                logger.info("Database integrity check completed successfully")
            else:
                error_count = len([issue for issue in issues if not issue.startswith("✓")])
                console.print(f"[red]✗[/red] Found {error_count} integrity issues")
                raise typer.Exit(1)

        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
            console.print(f"[red]✗[/red] Integrity check failed: {e}")
            raise typer.Exit(1)

    # Run the async integrity check
    asyncio.run(run_integrity_check())


@app.command()
def report(
    db_path: Optional[str] = DatabaseOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Generate a comprehensive database report with statistics and health metrics.
    """
    db_file = get_db_path(db_path)

    with MaintenanceContext(db_file, conversation_id) as conn:
        console.print("[blue]Generating database report...[/blue]")

        # Basic counts
        counts = {}
        for table in ['scripts', 'workflows', 'schema_meta']:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0
            except sqlite3.OperationalError:
                counts[table] = "N/A (table missing)"

        # Create summary table
        summary_table = Table(title="Database Summary", show_header=True)
        summary_table.add_column("Metric", style="cyan", no_wrap=True)
        summary_table.add_column("Value", style="green")

        summary_table.add_row("Scripts", str(counts['scripts']))
        summary_table.add_row("Workflows", str(counts['workflows']))
        summary_table.add_row("Schema Version", str(get_schema_version(conn)))

        # Database size
        db_size = Path(db_file).stat().st_size / 1024 / 1024  # MB
        summary_table.add_row("Database Size", f"{db_size:.2f} MB")

        console.print(summary_table)

        # Tag histogram
        try:
            console.print("\\n[blue]Tag Analysis:[/blue]")
            tag_counts = {}

            # Count script tags
            cursor = conn.execute("SELECT tags FROM scripts WHERE tags IS NOT NULL")
            for row in cursor:
                if row[0]:
                    try:
                        tags = json.loads(row[0])
                        for tag in tags:
                            tag_counts[tag] = tag_counts.get(tag, 0) + 1
                    except json.JSONDecodeError:
                        pass

            if tag_counts:
                tag_table = Table(title="Most Common Tags", show_header=True)
                tag_table.add_column("Tag", style="cyan")
                tag_table.add_column("Count", style="green", justify="right")

                for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                    tag_table.add_row(tag, str(count))

                console.print(tag_table)
            else:
                console.print("No tags found")

        except Exception as e:
            console.print(f"[yellow]Warning: Could not analyze tags: {e}[/yellow]")

        # Most referenced scripts
        try:
            console.print("\\n[blue]Most Referenced Scripts:[/blue]")
            script_refs = {}

            cursor = conn.execute("SELECT steps FROM workflows WHERE steps IS NOT NULL")
            for row in cursor:
                if row[0]:
                    try:
                        steps = json.loads(row[0])
                        for step in steps:
                            if 'script_id' in step:
                                script_id = step['script_id']
                                script_refs[script_id] = script_refs.get(script_id, 0) + 1
                    except json.JSONDecodeError:
                        pass

            if script_refs:
                ref_table = Table(title="Script Usage", show_header=True)
                ref_table.add_column("Script ID", style="cyan")
                ref_table.add_column("References", style="green", justify="right")

                for script_id, count in sorted(script_refs.items(), key=lambda x: x[1], reverse=True)[:10]:
                    ref_table.add_row(script_id, str(count))

                console.print(ref_table)
            else:
                console.print("No script references found")

        except Exception as e:
            console.print(f"[yellow]Warning: Could not analyze script usage: {e}[/yellow]")


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Get current schema version, creating table if needed"""
    try:
        # Check if schema_meta table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        )
        if not cursor.fetchone():
            # Create schema_meta table
            conn.execute("""
                CREATE TABLE schema_meta (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL,
                    notes TEXT
                )
            """)
            conn.execute(
                "INSERT INTO schema_meta (version, applied_at, notes) VALUES (?, ?, ?)",
                (1, datetime.now().isoformat(), "Initial schema")
            )
            conn.commit()
            return 1

        # Get latest version
        cursor = conn.execute("SELECT MAX(version) FROM schema_meta")
        result = cursor.fetchone()
        return result[0] if result and result[0] else 1

    except Exception as e:
        logger.error(f"Error getting schema version: {e}")
        return 1


def get_pending_migrations(current_version: int, target_version: int) -> List[Dict[str, Any]]:
    """Get list of pending migrations to apply"""
    migrations = []

    if current_version < 2:
        migrations.append({
            'version': 2,
            'description': 'Add constraints, indexes, and FTS5 support',
            'sql': [
                # Enable foreign keys
                "PRAGMA foreign_keys = ON",

                # Add unique constraint on scripts
                "CREATE UNIQUE INDEX IF NOT EXISTS scripts_name_path_uidx ON scripts(name, path)",

                # Add JSON-aware indexes
                "CREATE INDEX IF NOT EXISTS scripts_tags_idx ON scripts(json_extract(tags, '$'))",
                "CREATE INDEX IF NOT EXISTS workflows_tags_idx ON workflows(json_extract(tags, '$'))",

                # Add JSON validation constraints (will be enforced in future SQLite versions)
                # For now, we'll validate in application code

                # Create FTS5 virtual table for scripts
                """CREATE VIRTUAL TABLE IF NOT EXISTS scripts_fts USING fts5(
                    name, doc, tags, content='scripts', content_rowid='rowid'
                )""",

                # Populate FTS table
                "INSERT OR REPLACE INTO scripts_fts(rowid, name, doc, tags) SELECT rowid, name, doc, tags FROM scripts",

                # Create triggers to keep FTS in sync
                """CREATE TRIGGER IF NOT EXISTS scripts_fts_insert AFTER INSERT ON scripts BEGIN
                    INSERT INTO scripts_fts(rowid, name, doc, tags) VALUES (new.rowid, new.name, new.doc, new.tags);
                END""",

                """CREATE TRIGGER IF NOT EXISTS scripts_fts_delete AFTER DELETE ON scripts BEGIN
                    DELETE FROM scripts_fts WHERE rowid = old.rowid;
                END""",

                """CREATE TRIGGER IF NOT EXISTS scripts_fts_update AFTER UPDATE ON scripts BEGIN
                    DELETE FROM scripts_fts WHERE rowid = old.rowid;
                    INSERT INTO scripts_fts(rowid, name, doc, tags) VALUES (new.rowid, new.name, new.doc, new.tags);
                END""",
            ]
        })

    return [m for m in migrations if m['version'] > current_version]


@app.command()
def canonicalize(
    db_path: Optional[str] = DatabaseOption,
    dry_run: bool = DryRunOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Normalize JSON columns to canonical format.

    - Tags: lowercase, snake_case, deduplicated, sorted
    - Inputs/outputs: normalize keys and common synonyms
    """
    db_file = get_db_path(db_path)

    with MaintenanceContext(db_file, conversation_id) as conn:
        if not dry_run:
            backup_path = backup(db_path, conversation_id)
            console.print(f"Backup created: {backup_path}")

        changes = []

        # Canonicalize script tags
        cursor = conn.execute("SELECT id, tags FROM scripts WHERE tags IS NOT NULL")
        for row in cursor:
            script_id, tags_json = row
            if tags_json:
                try:
                    original_tags = json.loads(tags_json)
                    canonical_tags = canonicalize_tags(original_tags)

                    if original_tags != canonical_tags:
                        changes.append({
                            'table': 'scripts',
                            'id': script_id,
                            'column': 'tags',
                            'old': original_tags,
                            'new': canonical_tags
                        })

                        if not dry_run:
                            conn.execute(
                                "UPDATE scripts SET tags = ? WHERE id = ?",
                                (json.dumps(canonical_tags), script_id)
                            )

                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in scripts.tags for id {script_id}")

        # Canonicalize workflow tags
        cursor = conn.execute("SELECT id, tags FROM workflows WHERE tags IS NOT NULL")
        for row in cursor:
            workflow_id, tags_json = row
            if tags_json:
                try:
                    original_tags = json.loads(tags_json)
                    canonical_tags = canonicalize_tags(original_tags)

                    if original_tags != canonical_tags:
                        changes.append({
                            'table': 'workflows',
                            'id': workflow_id,
                            'column': 'tags',
                            'old': original_tags,
                            'new': canonical_tags
                        })

                        if not dry_run:
                            conn.execute(
                                "UPDATE workflows SET tags = ? WHERE id = ?",
                                (json.dumps(canonical_tags), workflow_id)
                            )

                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in workflows.tags for id {workflow_id}")

        # Canonicalize script inputs/outputs
        for column in ['inputs', 'outputs']:
            cursor = conn.execute(f"SELECT id, {column} FROM scripts WHERE {column} IS NOT NULL")
            for row in cursor:
                script_id, data_json = row
                if data_json:
                    try:
                        original_data = json.loads(data_json)
                        canonical_data = canonicalize_io_data(original_data)

                        if original_data != canonical_data:
                            changes.append({
                                'table': 'scripts',
                                'id': script_id,
                                'column': column,
                                'old': original_data,
                                'new': canonical_data
                            })

                            if not dry_run:
                                conn.execute(
                                    f"UPDATE scripts SET {column} = ? WHERE id = ?",
                                    (json.dumps(canonical_data), script_id)
                                )

                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in scripts.{column} for id {script_id}")

        if dry_run:
            console.print(f"\\n[yellow]Canonicalization Plan (DRY RUN) - {len(changes)} changes:[/yellow]")
            for change in changes[:10]:  # Show first 10
                console.print(f"  {change['table']}.{change['column']} [{change['id']}]:")
                console.print(f"    Old: {change['old']}")
                console.print(f"    New: {change['new']}")
            if len(changes) > 10:
                console.print(f"    ... and {len(changes) - 10} more changes")
        else:
            if changes:
                conn.commit()
                console.print(f"[green]✓[/green] Canonicalized {len(changes)} items")
                logger.info(f"Canonicalized {len(changes)} database items")
            else:
                console.print("[green]✓[/green] No canonicalization needed")


@app.command()
def prune_orphans(
    db_path: Optional[str] = DatabaseOption,
    dry_run: bool = DryRunOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Remove or fix orphaned references and invalid data.

    - Workflows with missing script references
    - Invalid JSON data
    """
    db_file = get_db_path(db_path)

    with MaintenanceContext(db_file, conversation_id) as conn:
        if not dry_run:
            backup_path = backup(db_path, conversation_id)
            console.print(f"Backup created: {backup_path}")

        orphaned_workflows = []
        fixed_workflows = []

        # Find workflows with missing script references
        cursor = conn.execute("SELECT id, name, steps FROM workflows WHERE steps IS NOT NULL")
        for row in cursor:
            workflow_id, workflow_name, steps_json = row
            if steps_json:
                try:
                    steps = json.loads(steps_json)
                    new_steps = []
                    has_orphans = False

                    for step in steps:
                        if 'script_id' in step:
                            script_id = step['script_id']
                            script_exists = conn.execute(
                                "SELECT 1 FROM scripts WHERE id = ?", (script_id,)
                            ).fetchone()

                            if script_exists:
                                new_steps.append(step)
                            else:
                                # Try to find replacement by name
                                script_by_name = conn.execute(
                                    "SELECT id FROM scripts WHERE name LIKE ? LIMIT 1",
                                    (f"%{script_id.replace('-', ' ')}%",)
                                ).fetchone()

                                if script_by_name:
                                    # Replace with found script
                                    step['script_id'] = script_by_name[0]
                                    new_steps.append(step)
                                    fixed_workflows.append({
                                        'workflow_id': workflow_id,
                                        'workflow_name': workflow_name,
                                        'old_script': script_id,
                                        'new_script': script_by_name[0]
                                    })
                                    logger.info(f"Remapped script {script_id} to {script_by_name[0]} in workflow {workflow_name}")
                                else:
                                    has_orphans = True
                                    logger.warning(f"Orphaned script reference {script_id} in workflow {workflow_name}")

                    if has_orphans:
                        orphaned_workflows.append({
                            'workflow_id': workflow_id,
                            'workflow_name': workflow_name,
                            'workflow_data': {
                                'id': workflow_id,
                                'name': workflow_name,
                                'steps': steps
                            }
                        })
                    elif new_steps != steps:
                        # Update workflow with fixed steps
                        if not dry_run:
                            conn.execute(
                                "UPDATE workflows SET steps = ? WHERE id = ?",
                                (json.dumps(new_steps), workflow_id)
                            )

                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in workflow {workflow_id} steps")

        if dry_run:
            console.print(f"\\n[yellow]Prune Plan (DRY RUN):[/yellow]")
            if orphaned_workflows:
                console.print(f"  Would remove {len(orphaned_workflows)} orphaned workflows:")
                for orphan in orphaned_workflows:
                    console.print(f"    - {orphan['workflow_name']} (ID: {orphan['workflow_id']})")

            if fixed_workflows:
                console.print(f"  Would fix {len(fixed_workflows)} script references:")
                for fix in fixed_workflows:
                    console.print(f"    - {fix['workflow_name']}: {fix['old_script']} → {fix['new_script']}")

        else:
            # Archive orphaned workflows
            if orphaned_workflows:
                orphans_dir = create_backup_dir() / "orphans"
                orphans_dir.mkdir(exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                orphans_file = orphans_dir / f"orphaned_workflows_{timestamp}.json"

                with open(orphans_file, 'w') as f:
                    json.dump([o['workflow_data'] for o in orphaned_workflows], f, indent=2)

                # Remove orphaned workflows
                for orphan in orphaned_workflows:
                    conn.execute("DELETE FROM workflows WHERE id = ?", (orphan['workflow_id'],))

                console.print(f"[green]✓[/green] Removed {len(orphaned_workflows)} orphaned workflows")
                console.print(f"Archived to: {orphans_file}")

            if fixed_workflows:
                console.print(f"[green]✓[/green] Fixed {len(fixed_workflows)} script references")

            if orphaned_workflows or fixed_workflows:
                conn.commit()
                logger.info(f"Pruned {len(orphaned_workflows)} orphans, fixed {len(fixed_workflows)} references")
            else:
                console.print("[green]✓[/green] No orphans found")


@app.command()
def dedupe(
    db_path: Optional[str] = DatabaseOption,
    dry_run: bool = DryRunOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Merge duplicate scripts by (name, path) and update workflow references.
    """
    db_file = get_db_path(db_path)

    with MaintenanceContext(db_file, conversation_id) as conn:
        if not dry_run:
            backup_path = backup(db_path, conversation_id)
            console.print(f"Backup created: {backup_path}")

        # Find duplicate scripts
        cursor = conn.execute("""
            SELECT name, path, COUNT(*) as count, GROUP_CONCAT(id) as ids
            FROM scripts
            GROUP BY name, path
            HAVING COUNT(*) > 1
        """)

        duplicates = []
        for row in cursor:
            name, path, count, ids_str = row
            ids = ids_str.split(',')
            duplicates.append({
                'name': name,
                'path': path,
                'count': count,
                'ids': ids
            })

        if not duplicates:
            console.print("[green]✓[/green] No duplicate scripts found")
            return

        if dry_run:
            console.print(f"\\n[yellow]Deduplication Plan (DRY RUN) - {len(duplicates)} duplicate groups:[/yellow]")
            for dup in duplicates:
                console.print(f"  {dup['name']} at {dup['path']}: {dup['count']} duplicates")
                console.print(f"    IDs: {', '.join(dup['ids'])}")
        else:
            merged_count = 0
            for dup in duplicates:
                # Get full records for all duplicates
                records = []
                for script_id in dup['ids']:
                    record = conn.execute("SELECT * FROM scripts WHERE id = ?", (script_id,)).fetchone()
                    if record:
                        records.append(dict(record))

                # Choose the best record (longest doc, or first alphabetically)
                best_record = max(records, key=lambda r: len(r.get('doc', '') or ''))
                keep_id = best_record['id']

                # Update workflow references to point to the kept script
                for script_id in dup['ids']:
                    if script_id != keep_id:
                        # Update workflows that reference this script
                        cursor = conn.execute("SELECT id, steps FROM workflows WHERE steps IS NOT NULL")
                        for workflow_row in cursor:
                            workflow_id, steps_json = workflow_row
                            if steps_json:
                                try:
                                    steps = json.loads(steps_json)
                                    updated = False

                                    for step in steps:
                                        if step.get('script_id') == script_id:
                                            step['script_id'] = keep_id
                                            updated = True

                                    if updated:
                                        conn.execute(
                                            "UPDATE workflows SET steps = ? WHERE id = ?",
                                            (json.dumps(steps), workflow_id)
                                        )

                                except json.JSONDecodeError:
                                    pass

                        # Delete the duplicate script
                        conn.execute("DELETE FROM scripts WHERE id = ?", (script_id,))
                        logger.info(f"Merged script {script_id} into {keep_id}")

                merged_count += len(dup['ids']) - 1  # All but the kept one

            conn.commit()
            console.print(f"[green]✓[/green] Merged {merged_count} duplicate scripts")


@app.command()
def reindex(
    db_path: Optional[str] = DatabaseOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Create/refresh indexes and FTS virtual tables for optimal performance.
    """
    db_file = get_db_path(db_path)

    with MaintenanceContext(db_file, conversation_id) as conn:
        console.print("[blue]Rebuilding indexes and FTS...[/blue]")

        try:
            # Ensure we have the latest schema
            migrate(db_path, dry_run=False, conversation_id=conversation_id)

            # Rebuild FTS
            conn.execute("INSERT INTO scripts_fts(scripts_fts) VALUES('rebuild')")

            # Analyze tables for query optimizer
            conn.execute("ANALYZE")

            console.print("[green]✓[/green] Indexes and FTS rebuilt successfully")

            # Show query timing examples
            console.print("\\n[blue]Query Performance Test:[/blue]")

            import time

            # Test tag search
            start = time.time()
            conn.execute("SELECT COUNT(*) FROM scripts WHERE json_extract(tags, '$') LIKE '%cooling%'").fetchone()
            tag_time = (time.time() - start) * 1000

            # Test name search
            start = time.time()
            conn.execute("SELECT COUNT(*) FROM scripts WHERE name LIKE '%demand%'").fetchone()
            name_time = (time.time() - start) * 1000

            # Test FTS search
            try:
                start = time.time()
                conn.execute("SELECT COUNT(*) FROM scripts_fts WHERE scripts_fts MATCH 'cooling'").fetchone()
                fts_time = (time.time() - start) * 1000
            except sqlite3.OperationalError:
                fts_time = "N/A (FTS not available)"

            timing_table = Table(title="Query Performance", show_header=True)
            timing_table.add_column("Query Type", style="cyan")
            timing_table.add_column("Time (ms)", style="green", justify="right")

            timing_table.add_row("Tag search", f"{tag_time:.2f}")
            timing_table.add_row("Name search", f"{name_time:.2f}")
            timing_table.add_row("FTS search", f"{fts_time:.2f}" if isinstance(fts_time, float) else str(fts_time))

            console.print(timing_table)

        except Exception as e:
            logger.error(f"Reindex failed: {e}")
            console.print(f"[red]✗[/red] Reindex failed: {e}")
            raise typer.Exit(1)


@app.command()
def vacuum(
    db_path: Optional[str] = DatabaseOption,
    conversation_id: Optional[str] = ConversationIdOption,
) -> None:
    """
    Reclaim space and optimize database storage (VACUUM + ANALYZE).
    """
    db_file = get_db_path(db_path)
    db_path_obj = Path(db_file)

    # Get size before
    size_before = db_path_obj.stat().st_size / 1024 / 1024  # MB

    with MaintenanceContext(db_file, conversation_id) as conn:
        console.print("[blue]Running VACUUM and ANALYZE...[/blue]")

        try:
            # VACUUM reclaims space
            conn.execute("VACUUM")

            # ANALYZE updates query planner statistics
            conn.execute("ANALYZE")

            console.print("[green]✓[/green] Database optimized successfully")

        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
            console.print(f"[red]✗[/red] Vacuum failed: {e}")
            raise typer.Exit(1)

    # Get size after
    size_after = db_path_obj.stat().st_size / 1024 / 1024  # MB
    savings = size_before - size_after

    console.print(f"Database size: {size_before:.2f} MB → {size_after:.2f} MB")
    if savings > 0:
        console.print(f"[green]Space reclaimed: {savings:.2f} MB[/green]")
    else:
        console.print("No space reclaimed")


def canonicalize_tags(tags: List[str]) -> List[str]:
    """Convert tags to canonical format: lowercase, snake_case, deduplicated, sorted"""
    if not isinstance(tags, list):
        return []

    canonical = []
    for tag in tags:
        if isinstance(tag, str):
            # Convert to lowercase snake_case
            import re
            # Split on non-alphanumeric characters and join with underscores
            words = re.findall(r'[a-zA-Z0-9]+', tag.lower())
            canonical_tag = '_'.join(words)
            if canonical_tag:
                canonical.append(canonical_tag)

    # Remove duplicates and sort
    return sorted(list(set(canonical)))


def canonicalize_io_data(data: Any) -> Any:
    """Normalize inputs/outputs data structure and keys"""
    if not isinstance(data, dict):
        return data

    # Common synonym mappings
    synonyms = {
        'epw': 'weather_epw',
        'weather_file': 'weather_epw',
        'weather': 'weather_epw',
        'zone': 'zone_geojson',
        'geometry': 'zone_geojson',
        'district_geojson': 'zone_geojson',
        'cost': 'cost_params',
        'capex_opex': 'cost_params',
    }

    canonical = {}
    for key, value in data.items():
        # Convert key to snake_case
        import re
        words = re.findall(r'[a-zA-Z0-9]+', key.lower())
        canonical_key = '_'.join(words)

        # Apply synonym mapping
        canonical_key = synonyms.get(canonical_key, canonical_key)

        # Ensure value is JSON-serializable
        if isinstance(value, (str, int, float, bool, type(None))):
            canonical[canonical_key] = value
        elif isinstance(value, (list, dict)):
            canonical[canonical_key] = value
        else:
            canonical[canonical_key] = str(value)

    return canonical


if __name__ == "__main__":
    app()