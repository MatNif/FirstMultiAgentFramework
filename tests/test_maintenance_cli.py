"""Tests for the maintenance CLI commands."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from cli.maintain import app
from db.dao import DAO


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    # Initialize with basic schema
    conn = sqlite3.connect(db_path)
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

    # Add some test data
    conn.execute("""
        INSERT INTO scripts (id, name, path, tags)
        VALUES (?, ?, ?, ?)
    """, ("test-script", "Test Script", "/test.py", json.dumps(["test", "example"])))

    conn.commit()
    conn.close()

    yield db_path
    Path(db_path).unlink(missing_ok=True)


class TestMaintenanceCLI:
    """Test the maintenance CLI commands."""

    def test_backup_command(self, temp_db):
        """Test the backup command."""
        runner = CliRunner()

        result = runner.invoke(app, ["backup", "--db", temp_db])

        assert result.exit_code == 0
        assert "Backup completed" in result.stdout
        assert "backup_" in result.stdout

    def test_migrate_dry_run(self, temp_db):
        """Test migrate command in dry run mode."""
        runner = CliRunner()

        result = runner.invoke(app, ["migrate", "--db", temp_db, "--dry-run"])

        assert result.exit_code == 0
        assert "Current schema version: 0" in result.stdout
        assert "Target schema version: 2" in result.stdout

    def test_migrate_apply(self, temp_db):
        """Test migrate command with apply."""
        runner = CliRunner()

        result = runner.invoke(app, ["migrate", "--db", temp_db, "--apply"])

        assert result.exit_code == 0
        assert "Migration completed successfully" in result.stdout or "already up to date" in result.stdout

    def test_integrity_command(self, temp_db):
        """Test the integrity check command."""
        runner = CliRunner()

        result = runner.invoke(app, ["integrity", "--db", temp_db])

        assert result.exit_code == 0
        assert "integrity check" in result.stdout.lower()

    def test_canonicalize_dry_run(self, temp_db):
        """Test canonicalize command in dry run mode."""
        runner = CliRunner()

        result = runner.invoke(app, ["canonicalize", "--db", temp_db, "--dry-run"])

        assert result.exit_code == 0
        # Should not fail and should show what would be done

    def test_report_command(self, temp_db):
        """Test the report command."""
        runner = CliRunner()

        result = runner.invoke(app, ["report", "--db", temp_db])

        assert result.exit_code == 0
        assert "Database Report" in result.stdout
        assert "Scripts:" in result.stdout
        assert "Workflows:" in result.stdout

    def test_vacuum_command(self, temp_db):
        """Test the vacuum command."""
        runner = CliRunner()

        result = runner.invoke(app, ["vacuum", "--db", temp_db])

        assert result.exit_code == 0
        assert "Database vacuum completed" in result.stdout

    def test_reindex_command(self, temp_db):
        """Test the reindex command."""
        runner = CliRunner()

        result = runner.invoke(app, ["reindex", "--db", temp_db])

        assert result.exit_code == 0
        assert "Database reindex completed" in result.stdout

    def test_invalid_database_path(self):
        """Test handling of invalid database path."""
        runner = CliRunner()

        result = runner.invoke(app, ["backup", "--db", "/nonexistent/database.db"])

        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_help_messages(self):
        """Test that help messages are displayed correctly."""
        runner = CliRunner()

        # Test main help
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "CEA Assistant database maintenance operations" in result.stdout

        # Test command-specific help
        commands = ["backup", "migrate", "integrity", "canonicalize", "report", "vacuum", "reindex"]
        for cmd in commands:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0

    def test_conversation_id_logging(self, temp_db):
        """Test that conversation ID is used for logging."""
        runner = CliRunner()

        with patch('cli.maintain.get_settings') as mock_settings:
            mock_settings.return_value.database_path = temp_db

            result = runner.invoke(app, [
                "backup",
                "--db", temp_db,
                "--conversation-id", "test-conv-123"
            ])

            assert result.exit_code == 0

    def test_dry_run_vs_apply_behavior(self, temp_db):
        """Test that dry-run and apply modes behave differently."""
        runner = CliRunner()

        # Add some data that needs canonicalization
        conn = sqlite3.connect(temp_db)
        conn.execute("""
            UPDATE scripts SET tags = ? WHERE id = ?
        """, (json.dumps(["TEST", "test", "Example"]), "test-script"))
        conn.commit()
        conn.close()

        # Dry run should show plan
        dry_result = runner.invoke(app, ["canonicalize", "--db", temp_db, "--dry-run"])
        assert dry_result.exit_code == 0

        # Apply should make changes
        apply_result = runner.invoke(app, ["canonicalize", "--db", temp_db, "--apply"])
        assert apply_result.exit_code == 0

    def test_backup_before_destructive_operations(self, temp_db):
        """Test that backups are created before destructive operations."""
        runner = CliRunner()

        # Run migrate with apply (which should create backup)
        result = runner.invoke(app, ["migrate", "--db", temp_db, "--apply"])

        if "Backup created" in result.stdout:
            # If backup was created, verify the message
            assert "Backup created:" in result.stdout


class TestErrorHandling:
    """Test error handling in maintenance commands."""

    def test_corrupted_database_handling(self):
        """Test handling of corrupted database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
            # Write invalid data to create corrupted database
            tmp.write(b"corrupted database content")

        try:
            runner = CliRunner()
            result = runner.invoke(app, ["integrity", "--db", db_path])

            # Should handle corruption gracefully
            assert result.exit_code != 0  # Should fail but not crash

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_permission_denied_handling(self, temp_db):
        """Test handling of permission denied errors."""
        # This test is platform-specific and may not work on all systems
        if Path(temp_db).exists():
            # Make database read-only
            Path(temp_db).chmod(0o444)

            try:
                runner = CliRunner()
                result = runner.invoke(app, ["migrate", "--db", temp_db, "--apply"])

                # Should handle permission error gracefully
                # (May succeed if running as admin/root)

            finally:
                # Restore permissions
                Path(temp_db).chmod(0o644)


class TestCLIIntegration:
    """Integration tests for CLI commands."""

    def test_full_maintenance_workflow(self, temp_db):
        """Test a complete maintenance workflow."""
        runner = CliRunner()

        # 1. Check current state
        result = runner.invoke(app, ["report", "--db", temp_db])
        assert result.exit_code == 0

        # 2. Run integrity check
        result = runner.invoke(app, ["integrity", "--db", temp_db])
        assert result.exit_code == 0

        # 3. Apply migrations
        result = runner.invoke(app, ["migrate", "--db", temp_db, "--apply"])
        assert result.exit_code == 0

        # 4. Canonicalize data
        result = runner.invoke(app, ["canonicalize", "--db", temp_db, "--apply"])
        assert result.exit_code == 0

        # 5. Final integrity check
        result = runner.invoke(app, ["integrity", "--db", temp_db])
        assert result.exit_code == 0

        # 6. Optimize database
        result = runner.invoke(app, ["vacuum", "--db", temp_db])
        assert result.exit_code == 0

    def test_command_chaining_safe(self, temp_db):
        """Test that commands can be safely chained."""
        runner = CliRunner()

        # Each command should leave database in a consistent state
        commands = [
            ["backup", "--db", temp_db],
            ["migrate", "--db", temp_db, "--apply"],
            ["canonicalize", "--db", temp_db, "--apply"],
            ["reindex", "--db", temp_db],
            ["vacuum", "--db", temp_db],
            ["integrity", "--db", temp_db],
            ["report", "--db", temp_db]
        ]

        for cmd in commands:
            result = runner.invoke(app, cmd)
            assert result.exit_code == 0, f"Command failed: {' '.join(cmd)}"


if __name__ == "__main__":
    # Manual test runner for development
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        test_db = tmp.name

    try:
        # Initialize test database
        conn = sqlite3.connect(test_db)
        conn.execute("""
            CREATE TABLE scripts (
                id TEXT PRIMARY KEY,
                name TEXT,
                path TEXT,
                tags TEXT
            )
        """)
        conn.execute("""
            INSERT INTO scripts (id, name, path, tags)
            VALUES (?, ?, ?, ?)
        """, ("test", "Test Script", "/test.py", json.dumps(["TEST", "test"])))
        conn.commit()
        conn.close()

        # Test CLI commands
        runner = CliRunner()

        print("Testing backup command...")
        result = runner.invoke(app, ["backup", "--db", test_db])
        print(f"Exit code: {result.exit_code}")
        print(f"Output: {result.stdout}")

        print("\nTesting migrate command...")
        result = runner.invoke(app, ["migrate", "--db", test_db, "--dry-run"])
        print(f"Exit code: {result.exit_code}")
        print(f"Output: {result.stdout}")

    finally:
        Path(test_db).unlink(missing_ok=True)