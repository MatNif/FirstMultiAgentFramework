"""Database migration system with schema versioning."""

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import logging

from .dao import DAO

logger = logging.getLogger(__name__)

class MigrationManager:
    """Manages database schema migrations with versioning."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migrations_dir = Path(__file__).parent / "migrations"
        self.migrations_dir.mkdir(exist_ok=True)

    async def get_schema_version(self) -> int:
        """Get current schema version from database."""
        try:
            async with DAO(self.db_path) as dao:
                cursor = await dao.execute_query(
                    "SELECT MAX(version) FROM schema_version"
                )
                result = await cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception:
            # Table doesn't exist, assume version 0
            return 0

    async def set_schema_version(self, version: int, migration_name: str) -> None:
        """Set schema version in database."""
        async with DAO(self.db_path) as dao:
            # Create schema_version table if it doesn't exist
            await dao.execute_query("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL,
                    migration_name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Record this migration
            await dao.execute_query(
                "INSERT INTO schema_version (version, migration_name) VALUES (?, ?)",
                (version, migration_name)
            )

            # Commit the changes
            await dao.commit()

    async def needs_migration(self) -> bool:
        """Check if database needs migration."""
        current_version = await self.get_schema_version()
        target_version = self.get_target_version()
        return current_version < target_version

    def get_target_version(self) -> int:
        """Get the target schema version (highest available migration)."""
        return 2  # Current target version

    async def migrate(self, dry_run: bool = True) -> List[str]:
        """Run database migrations."""
        current_version = await self.get_schema_version()
        target_version = self.get_target_version()

        if current_version >= target_version:
            return ["Database is already at the latest version"]

        operations = []

        # Run migrations from current_version + 1 to target_version
        for version in range(current_version + 1, target_version + 1):
            migration_ops = await self._run_migration(version, dry_run)
            operations.extend(migration_ops)

        return operations

    async def _run_migration(self, version: int, dry_run: bool) -> List[str]:
        """Run a specific migration."""
        if version == 1:
            return await self._migrate_to_v1(dry_run)
        elif version == 2:
            return await self._migrate_to_v2(dry_run)
        else:
            raise ValueError(f"Unknown migration version: {version}")

    async def _migrate_to_v1(self, dry_run: bool) -> List[str]:
        """Migrate to schema version 1 - add schema_version table."""
        operations = ["Migration to v1: Add schema versioning"]

        if not dry_run:
            async with DAO(self.db_path) as dao:
                await dao.execute_query("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER NOT NULL,
                        migration_name TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                await dao.commit()

            await self.set_schema_version(1, "initial_versioning")
            operations.append("+ Created schema_version table")
        else:
            operations.append("- Would create schema_version table")

        return operations

    async def _migrate_to_v2(self, dry_run: bool) -> List[str]:
        """Migrate to schema version 2 - add FTS5 and performance improvements."""
        operations = ["Migration to v2: Add FTS5 and performance indexes"]

        if not dry_run:
            async with DAO(self.db_path) as dao:
                # Add FTS5 virtual tables
                await dao.execute_query("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS scripts_fts
                    USING fts5(
                        id, name, doc, tags, inputs, outputs,
                        content='scripts',
                        content_rowid='rowid'
                    )
                """)

                await dao.execute_query("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS workflows_fts
                    USING fts5(
                        id, name, description, tags, steps,
                        content='workflows',
                        content_rowid='rowid'
                    )
                """)

                # Create triggers to keep FTS in sync
                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS scripts_ai AFTER INSERT ON scripts BEGIN
                        INSERT INTO scripts_fts(rowid, id, name, doc, tags, inputs, outputs)
                        VALUES (new.rowid, new.id, new.name, new.doc, new.tags, new.inputs, new.outputs);
                    END
                """)

                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS scripts_ad AFTER DELETE ON scripts BEGIN
                        INSERT INTO scripts_fts(scripts_fts, rowid, id, name, doc, tags, inputs, outputs)
                        VALUES ('delete', old.rowid, old.id, old.name, old.doc, old.tags, old.inputs, old.outputs);
                    END
                """)

                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS scripts_au AFTER UPDATE ON scripts BEGIN
                        INSERT INTO scripts_fts(scripts_fts, rowid, id, name, doc, tags, inputs, outputs)
                        VALUES ('delete', old.rowid, old.id, old.name, old.doc, old.tags, old.inputs, old.outputs);
                        INSERT INTO scripts_fts(rowid, id, name, doc, tags, inputs, outputs)
                        VALUES (new.rowid, new.id, new.name, new.doc, new.tags, new.inputs, new.outputs);
                    END
                """)

                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS workflows_ai AFTER INSERT ON workflows BEGIN
                        INSERT INTO workflows_fts(rowid, id, name, description, tags, steps)
                        VALUES (new.rowid, new.id, new.name, new.description, new.tags, new.steps);
                    END
                """)

                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS workflows_ad AFTER DELETE ON workflows BEGIN
                        INSERT INTO workflows_fts(workflows_fts, rowid, id, name, description, tags, steps)
                        VALUES ('delete', old.rowid, old.id, old.name, old.description, old.tags, old.steps);
                    END
                """)

                await dao.execute_query("""
                    CREATE TRIGGER IF NOT EXISTS workflows_au AFTER UPDATE ON workflows BEGIN
                        INSERT INTO workflows_fts(workflows_fts, rowid, id, name, description, tags, steps)
                        VALUES ('delete', old.rowid, old.id, old.name, old.description, old.tags, old.steps);
                        INSERT INTO workflows_fts(rowid, id, name, description, tags, steps)
                        VALUES (new.rowid, new.id, new.name, new.description, new.tags, new.steps);
                    END
                """)

                # Populate FTS tables with existing data (handle missing columns gracefully)
                try:
                    await dao.execute_query("""
                        INSERT OR IGNORE INTO scripts_fts(rowid, id, name, doc, tags, inputs, outputs)
                        SELECT rowid, id, name,
                               COALESCE(doc, '') as doc,
                               COALESCE(tags, '[]') as tags,
                               COALESCE(inputs, '[]') as inputs,
                               COALESCE(outputs, '[]') as outputs
                        FROM scripts
                    """)
                except Exception:
                    # Fallback for minimal schema
                    await dao.execute_query("""
                        INSERT OR IGNORE INTO scripts_fts(rowid, id, name, doc, tags, inputs, outputs)
                        SELECT rowid, id, name, '', '[]', '[]', '[]' FROM scripts
                    """)

                try:
                    await dao.execute_query("""
                        INSERT OR IGNORE INTO workflows_fts(rowid, id, name, description, tags, steps)
                        SELECT rowid, id, name,
                               COALESCE(description, '') as description,
                               COALESCE(tags, '[]') as tags,
                               COALESCE(steps, '[]') as steps
                        FROM workflows
                    """)
                except Exception:
                    # Fallback for minimal schema
                    await dao.execute_query("""
                        INSERT OR IGNORE INTO workflows_fts(rowid, id, name, description, tags, steps)
                        SELECT rowid, id, name, '', '[]', '[]' FROM workflows
                    """)

                # Add performance indexes (handle missing columns gracefully)
                try:
                    await dao.execute_query("CREATE INDEX IF NOT EXISTS idx_scripts_created_at ON scripts(created_at)")
                except Exception:
                    pass  # Column doesn't exist

                try:
                    await dao.execute_query("CREATE INDEX IF NOT EXISTS idx_scripts_updated_at ON scripts(updated_at)")
                except Exception:
                    pass  # Column doesn't exist

                try:
                    await dao.execute_query("CREATE INDEX IF NOT EXISTS idx_workflows_created_at ON workflows(created_at)")
                except Exception:
                    pass  # Column doesn't exist

                try:
                    await dao.execute_query("CREATE INDEX IF NOT EXISTS idx_workflows_updated_at ON workflows(updated_at)")
                except Exception:
                    pass  # Column doesn't exist

                # Commit all changes
                await dao.commit()

                # Update schema version within the same transaction context
                await dao.execute_query(
                    "INSERT INTO schema_version (version, migration_name) VALUES (?, ?)",
                    (2, "fts5_and_performance")
                )
                await dao.commit()
            operations.extend([
                "+ Created FTS5 virtual tables",
                "+ Created FTS sync triggers",
                "+ Populated FTS with existing data",
                "+ Added performance indexes"
            ])
        else:
            operations.extend([
                "- Would create FTS5 virtual tables",
                "- Would create FTS sync triggers",
                "- Would populate FTS with existing data",
                "- Would add performance indexes"
            ])

        return operations

    async def normalize_data(self, dry_run: bool = True) -> List[str]:
        """Normalize and canonicalize existing data."""
        operations = ["Data normalization and canonicalization:"]

        async with DAO(self.db_path) as dao:
            # Get all scripts for normalization
            cursor = await dao.execute_query("SELECT id, tags, inputs, outputs FROM scripts")
            scripts = await cursor.fetchall()

            # Get all workflows for normalization
            cursor = await dao.execute_query("SELECT id, tags, steps FROM workflows")
            workflows = await cursor.fetchall()

            for script in scripts:
                script_id, tags, inputs, outputs = script
                normalized_ops = []

                # Normalize tags
                if tags:
                    try:
                        tag_list = json.loads(tags) if isinstance(tags, str) else tags
                        if isinstance(tag_list, list):
                            normalized_tags = sorted(set(tag.strip().lower() for tag in tag_list if tag.strip()))
                            if normalized_tags != tag_list:
                                normalized_ops.append(f"normalize tags for script {script_id}")
                                if not dry_run:
                                    await dao.execute_query(
                                        "UPDATE scripts SET tags = ? WHERE id = ?",
                                        (json.dumps(normalized_tags), script_id)
                                    )
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Normalize inputs/outputs
                for field_name, field_value in [("inputs", inputs), ("outputs", outputs)]:
                    if field_value:
                        try:
                            data = json.loads(field_value) if isinstance(field_value, str) else field_value
                            if isinstance(data, list):
                                normalized_data = []
                                for item in data:
                                    if isinstance(item, dict):
                                        # Normalize file extensions
                                        if 'type' in item:
                                            item['type'] = item['type'].lower().strip('.')
                                        # Sort keys for consistency
                                        normalized_item = {k: v for k, v in sorted(item.items())}
                                        normalized_data.append(normalized_item)
                                    else:
                                        normalized_data.append(item)

                                if normalized_data != data:
                                    normalized_ops.append(f"normalize {field_name} for script {script_id}")
                                    if not dry_run:
                                        await dao.execute_query(
                                            f"UPDATE scripts SET {field_name} = ? WHERE id = ?",
                                            (json.dumps(normalized_data), script_id)
                                        )
                        except (json.JSONDecodeError, TypeError):
                            pass

                if normalized_ops:
                    operations.extend([f"- {op}" if dry_run else f"+ {op}" for op in normalized_ops])

            # Normalize workflows
            for workflow in workflows:
                workflow_id, tags, steps = workflow
                normalized_ops = []

                # Normalize workflow tags
                if tags:
                    try:
                        tag_list = json.loads(tags) if isinstance(tags, str) else tags
                        if isinstance(tag_list, list):
                            normalized_tags = sorted(set(tag.strip().lower() for tag in tag_list if tag.strip()))
                            if normalized_tags != tag_list:
                                normalized_ops.append(f"normalize tags for workflow {workflow_id}")
                                if not dry_run:
                                    await dao.execute_query(
                                        "UPDATE workflows SET tags = ? WHERE id = ?",
                                        (json.dumps(normalized_tags), workflow_id)
                                    )
                    except (json.JSONDecodeError, TypeError):
                        pass

                if normalized_ops:
                    operations.extend([f"- {op}" if dry_run else f"+ {op}" for op in normalized_ops])

            # Commit all normalization changes
            if not dry_run and len(operations) > 1:
                await dao.commit()

        if len(operations) == 1:
            operations.append("- No data normalization needed" if dry_run else "+ All data already normalized")

        return operations

    async def check_integrity(self) -> List[str]:
        """Check database integrity."""
        issues = []

        async with DAO(self.db_path) as dao:
            # Check for orphaned data
            cursor = await dao.execute_query("""
                SELECT COUNT(*) FROM scripts
                WHERE inputs IS NOT NULL AND inputs != '' AND inputs != '[]'
                AND json_valid(inputs) = 0
            """)
            invalid_inputs = (await cursor.fetchone())[0]
            if invalid_inputs > 0:
                issues.append(f"Found {invalid_inputs} scripts with invalid JSON inputs")

            cursor = await dao.execute_query("""
                SELECT COUNT(*) FROM scripts
                WHERE outputs IS NOT NULL AND outputs != '' AND outputs != '[]'
                AND json_valid(outputs) = 0
            """)
            invalid_outputs = (await cursor.fetchone())[0]
            if invalid_outputs > 0:
                issues.append(f"Found {invalid_outputs} scripts with invalid JSON outputs")

            cursor = await dao.execute_query("""
                SELECT COUNT(*) FROM workflows
                WHERE steps IS NOT NULL AND steps != '' AND steps != '[]'
                AND json_valid(steps) = 0
            """)
            invalid_steps = (await cursor.fetchone())[0]
            if invalid_steps > 0:
                issues.append(f"Found {invalid_steps} workflows with invalid JSON steps")

            # Check for duplicates
            cursor = await dao.execute_query("""
                SELECT name, COUNT(*) as count FROM scripts
                GROUP BY name HAVING count > 1
            """)
            duplicate_scripts = await cursor.fetchall()
            if duplicate_scripts:
                issues.append(f"Found {len(duplicate_scripts)} duplicate script names")

            cursor = await dao.execute_query("""
                SELECT name, COUNT(*) as count FROM workflows
                GROUP BY name HAVING count > 1
            """)
            duplicate_workflows = await cursor.fetchall()
            if duplicate_workflows:
                issues.append(f"Found {len(duplicate_workflows)} duplicate workflow names")

            # SQLite integrity check
            cursor = await dao.execute_query("PRAGMA integrity_check")
            integrity_result = await cursor.fetchone()
            if integrity_result[0] != "ok":
                issues.append(f"SQLite integrity check failed: {integrity_result[0]}")

        if not issues:
            return ["+ Database integrity check passed"]

        return ["Database integrity issues found:"] + [f"- {issue}" for issue in issues]