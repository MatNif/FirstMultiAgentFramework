"""Tests for database migration system."""

import asyncio
import json
import sqlite3
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any

import pytest

from db.migrations import MigrationManager
from db.dao import DAO


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    yield db_path
    # Give time for connections to close on Windows
    import time
    time.sleep(0.1)
    try:
        Path(db_path).unlink(missing_ok=True)
    except PermissionError:
        # File still in use, try again after a short delay
        time.sleep(0.5)
        Path(db_path).unlink(missing_ok=True)


@pytest.fixture
async def pre_v2_db(temp_db):
    """Create a pre-v2 database with old schema and funky data."""
    # Create v0 database with original schema
    conn = sqlite3.connect(temp_db)
    conn.execute("""
        CREATE TABLE scripts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            cli TEXT,
            doc TEXT,
            inputs TEXT,
            outputs TEXT,
            tags TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE workflows (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            steps TEXT NOT NULL,
            tags TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert some funky data for normalization testing
    conn.execute("""
        INSERT INTO scripts (id, name, path, cli, doc, inputs, outputs, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "script-1",
        "Test Script",
        "/test/script.py",
        "python script.py",
        "A test script",
        json.dumps([
            {"name": "Weather File", "type": ".EPW", "required": True},
            {"name": "zone_file", "type": "GEOJSON", "required": True}
        ]),
        json.dumps([
            {"name": "results", "type": ".CSV", "description": "Output results"}
        ]),
        json.dumps(["Cooling", "cooling", "DEMAND", "demand", "cooling"])  # Duplicates and mixed case
    ))

    conn.execute("""
        INSERT INTO workflows (id, name, description, steps, tags)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "workflow-1",
        "Test Workflow",
        "A test workflow",
        json.dumps([
            {"script_id": "script-1", "description": "Run test script", "order": 1},
            {"script_id": "missing-script", "description": "Missing script", "order": 2}
        ]),
        json.dumps(["Test", "workflow", "TEST", "Workflow"])  # Mixed case duplicates
    ))

    # Insert invalid JSON data
    conn.execute("""
        INSERT INTO scripts (id, name, path, inputs, tags)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "script-invalid",
        "Invalid Script",
        "/invalid/script.py",
        "not valid json",
        "[invalid json"
    ))

    conn.commit()
    conn.close()

    # Ensure the connection is properly closed
    import time
    time.sleep(0.1)

    return temp_db


class TestMigrationManager:
    """Test the MigrationManager class."""

    @pytest.mark.asyncio
    async def test_schema_version_detection(self, temp_db):
        """Test schema version detection."""
        manager = MigrationManager(temp_db)

        # New database should be version 0
        version = await manager.get_schema_version()
        assert version == 0

        # After setting version, should return correct value
        await manager.set_schema_version(1, "test_migration")
        version = await manager.get_schema_version()
        assert version == 1

    @pytest.mark.asyncio
    async def test_needs_migration(self, pre_v2_db):
        """Test migration necessity detection."""
        manager = MigrationManager(pre_v2_db)

        # Pre-v2 database should need migration
        needs_migration = await manager.needs_migration()
        assert needs_migration is True

        current_version = await manager.get_schema_version()
        target_version = manager.get_target_version()
        assert current_version < target_version

    @pytest.mark.asyncio
    async def test_dry_run_migration(self, pre_v2_db):
        """Test dry run migration shows plan without making changes."""
        manager = MigrationManager(pre_v2_db)

        # Run dry migration
        operations = await manager.migrate(dry_run=True)

        # Should have operations but not execute them
        assert len(operations) > 0
        assert any("Migration to v1" in op for op in operations)
        assert any("Migration to v2" in op for op in operations)

        # Database should remain unchanged
        version = await manager.get_schema_version()
        assert version == 0

    @pytest.mark.asyncio
    async def test_apply_migration(self, pre_v2_db):
        """Test applying migration brings database to latest version."""
        manager = MigrationManager(pre_v2_db)

        # Store original data for preservation test
        conn = sqlite3.connect(pre_v2_db)
        original_scripts = conn.execute("SELECT id, name FROM scripts").fetchall()
        original_workflows = conn.execute("SELECT id, name FROM workflows").fetchall()
        conn.close()

        # Apply migration
        operations = await manager.migrate(dry_run=False)

        # Should have executed operations
        assert len(operations) > 0
        assert any("+" in op for op in operations)

        # Database should be at target version
        version = await manager.get_schema_version()
        target_version = manager.get_target_version()
        assert version == target_version

        # Verify new schema elements exist
        conn = sqlite3.connect(pre_v2_db)

        # Check schema_version table exists
        result = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='schema_version'
        """).fetchone()
        assert result is not None

        # Check FTS5 tables exist
        fts_tables = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name LIKE '%_fts'
        """).fetchall()
        assert len(fts_tables) >= 2  # scripts_fts and workflows_fts

        # Check triggers exist
        triggers = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='trigger'
        """).fetchall()
        assert len(triggers) >= 6  # AI, AD, AU triggers for both tables

        # Verify data preservation
        migrated_scripts = conn.execute("SELECT id, name FROM scripts").fetchall()
        migrated_workflows = conn.execute("SELECT id, name FROM workflows").fetchall()

        assert len(migrated_scripts) == len(original_scripts)
        assert len(migrated_workflows) == len(original_workflows)

        # Verify FTS is populated
        fts_count = conn.execute("SELECT COUNT(*) FROM scripts_fts").fetchone()[0]
        assert fts_count > 0

        conn.close()

    @pytest.mark.asyncio
    async def test_fts_functionality(self, pre_v2_db):
        """Test that FTS5 search works after migration."""
        manager = MigrationManager(pre_v2_db)

        # Apply migration
        await manager.migrate(dry_run=False)

        # Test FTS search
        conn = sqlite3.connect(pre_v2_db)

        # Search for "test" should find our test script
        results = conn.execute("""
            SELECT scripts.name FROM scripts_fts
            JOIN scripts ON scripts.rowid = scripts_fts.rowid
            WHERE scripts_fts MATCH 'test'
        """).fetchall()

        assert len(results) > 0
        assert any("Test Script" in str(result) for result in results)

        conn.close()

    @pytest.mark.asyncio
    async def test_data_normalization(self, pre_v2_db):
        """Test data normalization during migration."""
        manager = MigrationManager(pre_v2_db)

        # Apply migration and normalization
        await manager.migrate(dry_run=False)
        await manager.normalize_data(dry_run=False)

        # Check that tags were normalized
        conn = sqlite3.connect(pre_v2_db)

        # Get script tags
        script_tags = conn.execute(
            "SELECT tags FROM scripts WHERE id = 'script-1'"
        ).fetchone()[0]
        tags = json.loads(script_tags)

        # Tags should be deduplicated, lowercase, and sorted
        assert tags == ["cooling", "demand"]  # Sorted, lowercase, deduplicated

        # Get workflow tags
        workflow_tags = conn.execute(
            "SELECT tags FROM workflows WHERE id = 'workflow-1'"
        ).fetchone()[0]
        tags = json.loads(workflow_tags)

        # Should be normalized
        assert tags == ["test", "workflow"]  # Sorted, lowercase, deduplicated

        conn.close()

    @pytest.mark.asyncio
    async def test_integrity_checks(self, pre_v2_db):
        """Test integrity checking after migration."""
        manager = MigrationManager(pre_v2_db)

        # Apply migration
        await manager.migrate(dry_run=False)

        # Run integrity check
        issues = await manager.check_integrity()

        # Should find some issues (invalid JSON, missing script reference)
        error_issues = [issue for issue in issues if not issue.startswith("+")]
        assert len(error_issues) > 0

        # Should detect invalid JSON
        assert any("invalid JSON" in issue.lower() for issue in error_issues)

    @pytest.mark.asyncio
    async def test_idempotent_migrations(self, pre_v2_db):
        """Test that migrations are idempotent."""
        manager = MigrationManager(pre_v2_db)

        # Apply migration twice
        operations1 = await manager.migrate(dry_run=False)
        operations2 = await manager.migrate(dry_run=False)

        # First migration should do work
        assert len(operations1) > 1
        assert any("+" in op for op in operations1)

        # Second migration should do nothing
        assert len(operations2) == 1
        assert "already at the latest version" in operations2[0]

    @pytest.mark.asyncio
    async def test_migration_error_handling(self, temp_db):
        """Test migration error handling."""
        manager = MigrationManager(temp_db)

        # Create a corrupted database
        conn = sqlite3.connect(temp_db)
        conn.execute("CREATE TABLE scripts (invalid_schema TEXT)")
        conn.close()

        # Migration should handle errors gracefully
        with pytest.raises(Exception):
            await manager.migrate(dry_run=False)

    @pytest.mark.asyncio
    async def test_schema_version_tracking(self, temp_db):
        """Test that schema versions are tracked correctly."""
        manager = MigrationManager(temp_db)

        # Apply migrations
        await manager.migrate(dry_run=False)

        # Check schema_version table has entries
        conn = sqlite3.connect(temp_db)
        versions = conn.execute("""
            SELECT version, migration_name, applied_at
            FROM schema_version
            ORDER BY applied_at
        """).fetchall()

        assert len(versions) >= 2  # v1 and v2
        assert versions[0][0] == 1  # First should be version 1
        assert versions[-1][0] == 2  # Last should be version 2
        assert all(row[2] for row in versions)  # All should have timestamps

        conn.close()


class TestMigrationIntegration:
    """Integration tests for migration with DAO."""

    @pytest.mark.asyncio
    async def test_dao_works_after_migration(self, pre_v2_db):
        """Test that DAO continues to work after migration."""
        # Migrate the database
        manager = MigrationManager(pre_v2_db)
        await manager.migrate(dry_run=False)

        # DAO should work with migrated database
        async with DAO(pre_v2_db) as dao:
            scripts = await dao.get_all_scripts()
            workflows = await dao.get_all_workflows()

            assert len(scripts) >= 2  # Including the invalid one
            assert len(workflows) >= 1

    @pytest.mark.asyncio
    async def test_fts_search_through_dao(self, pre_v2_db):
        """Test FTS search integration with future DAO enhancements."""
        # Migrate the database
        manager = MigrationManager(pre_v2_db)
        await manager.migrate(dry_run=False)

        # Direct FTS test (simulating future DAO enhancement)
        async with DAO(pre_v2_db) as dao:
            cursor = await dao.execute_query("""
                SELECT scripts.* FROM scripts_fts
                JOIN scripts ON scripts.rowid = scripts_fts.rowid
                WHERE scripts_fts MATCH ?
            """, ("test",))

            results = await cursor.fetchall()
            assert len(results) > 0


if __name__ == "__main__":
    # Run tests manually for development
    import asyncio

    async def run_test():
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            test_db = tmp.name

        try:
            # Create pre-v2 fixture manually
            fixture = pre_v2_db.__pytest_wrapped__.obj
            await fixture(test_db)

            # Test migration
            manager = MigrationManager(test_db)
            print("Testing migration...")
            operations = await manager.migrate(dry_run=False)
            for op in operations:
                print(f"  {op}")

            print("\nTesting data normalization...")
            norm_ops = await manager.normalize_data(dry_run=False)
            for op in norm_ops:
                print(f"  {op}")

            print("\nTesting integrity check...")
            issues = await manager.check_integrity()
            for issue in issues:
                print(f"  {issue}")

        finally:
            Path(test_db).unlink(missing_ok=True)

    if __name__ == "__main__":
        asyncio.run(run_test())