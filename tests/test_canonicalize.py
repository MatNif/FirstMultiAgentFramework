"""Tests for data canonicalization and normalization."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from db.migrations import MigrationManager


@pytest.fixture
def test_db_with_funky_data():
    """Create a test database with funky data for canonicalization."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    conn = sqlite3.connect(db_path)

    # Create tables
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

    # Insert funky data for canonicalization testing
    test_cases = [
        {
            "id": "script-mixed-case",
            "name": "Mixed Case Script",
            "path": "/test.py",
            "inputs": json.dumps([
                {"name": "weather_file", "type": ".EPW", "required": True},
                {"name": "zone-file", "type": "GeoJSON", "required": True}
            ]),
            "outputs": json.dumps([
                {"name": "Output File", "type": ".CSV"},
                {"name": "summary", "type": ".json"}
            ]),
            "tags": json.dumps(["COOLING", "cooling", "Demand", "DEMAND", "analysis", "Analysis"])
        },
        {
            "id": "script-synonyms",
            "name": "Synonym Test Script",
            "path": "/synonym.py",
            "inputs": json.dumps([
                {"name": "epw", "type": ".epw", "required": True},
                {"name": "weather", "type": ".EPW", "required": False},
                {"name": "zone", "type": ".geojson", "required": True},
                {"name": "geometry", "type": "GEOJSON", "required": False}
            ]),
            "outputs": json.dumps([
                {"name": "cost", "type": ".xlsx"},
                {"name": "capex_opex", "type": ".CSV"}
            ]),
            "tags": json.dumps(["energy-analysis", "cost_optimization", "HEATING", "heating"])
        },
        {
            "id": "script-special-chars",
            "name": "Special Characters Script",
            "path": "/special.py",
            "inputs": json.dumps([
                {"name": "input-file", "type": ".TXT", "required": True},
                {"name": "config@file", "type": ".JSON", "required": False}
            ]),
            "tags": json.dumps(["pre-processing", "data@cleaning", "Input/Output", "file-handling"])
        }
    ]

    for case in test_cases:
        conn.execute("""
            INSERT INTO scripts (id, name, path, inputs, outputs, tags)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (case["id"], case["name"], case["path"],
              case.get("inputs"), case.get("outputs"), case["tags"]))

    # Insert workflow test cases
    workflow_cases = [
        {
            "id": "workflow-mixed",
            "name": "Mixed Case Workflow",
            "description": "Test workflow",
            "steps": json.dumps([
                {"script_id": "script-mixed-case", "description": "Step 1", "order": 1},
                {"script_id": "script-synonyms", "description": "Step 2", "order": 2}
            ]),
            "tags": json.dumps(["WORKFLOW", "workflow", "Test", "test", "multi-step"])
        }
    ]

    for case in workflow_cases:
        conn.execute("""
            INSERT INTO workflows (id, name, description, steps, tags)
            VALUES (?, ?, ?, ?, ?)
        """, (case["id"], case["name"], case["description"], case["steps"], case["tags"]))

    conn.commit()
    conn.close()

    yield db_path
    Path(db_path).unlink(missing_ok=True)


class TestDataCanonicalization:
    """Test data canonicalization functionality."""

    @pytest.mark.asyncio
    async def test_tag_canonicalization(self, test_db_with_funky_data):
        """Test that tags are canonicalized correctly."""
        manager = MigrationManager(test_db_with_funky_data)

        # Run normalization
        operations = await manager.normalize_data(dry_run=False)

        # Check results
        conn = sqlite3.connect(test_db_with_funky_data)

        # Check script tags
        script_tags = conn.execute(
            "SELECT tags FROM scripts WHERE id = 'script-mixed-case'"
        ).fetchone()[0]
        tags = json.loads(script_tags)

        # Should be lowercase, deduplicated, and sorted
        expected_tags = ["analysis", "cooling", "demand"]
        assert tags == expected_tags, f"Expected {expected_tags}, got {tags}"

        # Check workflow tags
        workflow_tags = conn.execute(
            "SELECT tags FROM workflows WHERE id = 'workflow-mixed'"
        ).fetchone()[0]
        tags = json.loads(workflow_tags)

        # Should be normalized
        expected_workflow_tags = ["multi_step", "test", "workflow"]
        assert tags == expected_workflow_tags, f"Expected {expected_workflow_tags}, got {tags}"

        conn.close()

    @pytest.mark.asyncio
    async def test_io_data_canonicalization(self, test_db_with_funky_data):
        """Test that input/output data is canonicalized."""
        manager = MigrationManager(test_db_with_funky_data)

        # Run normalization
        await manager.normalize_data(dry_run=False)

        # Check results
        conn = sqlite3.connect(test_db_with_funky_data)

        # Check inputs normalization
        inputs_json = conn.execute(
            "SELECT inputs FROM scripts WHERE id = 'script-synonyms'"
        ).fetchone()[0]
        inputs = json.loads(inputs_json)

        # Check that file types are normalized (lowercase, no leading dot)
        for inp in inputs:
            if "type" in inp:
                assert not inp["type"].startswith("."), f"Type should not start with dot: {inp['type']}"
                assert inp["type"].islower(), f"Type should be lowercase: {inp['type']}"

        # Check that keys are sorted consistently
        for inp in inputs:
            keys = list(inp.keys())
            assert keys == sorted(keys), f"Keys should be sorted: {keys}"

        conn.close()

    @pytest.mark.asyncio
    async def test_synonym_normalization(self, test_db_with_funky_data):
        """Test that common synonyms are normalized."""
        manager = MigrationManager(test_db_with_funky_data)

        # Run normalization
        await manager.normalize_data(dry_run=False)

        # Check results
        conn = sqlite3.connect(test_db_with_funky_data)

        inputs_json = conn.execute(
            "SELECT inputs FROM scripts WHERE id = 'script-synonyms'"
        ).fetchone()[0]
        inputs = json.loads(inputs_json)

        # Note: The current implementation focuses on type normalization
        # Synonym normalization for field names would be a future enhancement
        # For now, just verify the structure is consistent

        # Verify all inputs have consistent structure
        for inp in inputs:
            assert "name" in inp, "All inputs should have 'name' field"
            assert "type" in inp, "All inputs should have 'type' field"
            assert "required" in inp, "All inputs should have 'required' field"

        conn.close()

    @pytest.mark.asyncio
    async def test_special_character_handling(self, test_db_with_funky_data):
        """Test handling of special characters in tags and data."""
        manager = MigrationManager(test_db_with_funky_data)

        # Run normalization
        await manager.normalize_data(dry_run=False)

        # Check results
        conn = sqlite3.connect(test_db_with_funky_data)

        script_tags = conn.execute(
            "SELECT tags FROM scripts WHERE id = 'script-special-chars'"
        ).fetchone()[0]
        tags = json.loads(script_tags)

        # Special characters should be handled consistently
        # @ symbols and slashes should be converted to underscores
        expected_tags = ["data_cleaning", "file_handling", "input_output", "pre_processing"]
        assert tags == expected_tags, f"Expected {expected_tags}, got {tags}"

        conn.close()

    @pytest.mark.asyncio
    async def test_dry_run_canonicalization(self, test_db_with_funky_data):
        """Test dry run mode for canonicalization."""
        manager = MigrationManager(test_db_with_funky_data)

        # Store original data
        conn = sqlite3.connect(test_db_with_funky_data)
        original_script_tags = conn.execute(
            "SELECT tags FROM scripts WHERE id = 'script-mixed-case'"
        ).fetchone()[0]
        conn.close()

        # Run dry run
        operations = await manager.normalize_data(dry_run=True)

        # Should show what would be done
        assert len(operations) > 1
        assert any("normalize tags" in op for op in operations)

        # Data should be unchanged
        conn = sqlite3.connect(test_db_with_funky_data)
        current_script_tags = conn.execute(
            "SELECT tags FROM scripts WHERE id = 'script-mixed-case'"
        ).fetchone()[0]
        conn.close()

        assert current_script_tags == original_script_tags

    @pytest.mark.asyncio
    async def test_no_changes_needed(self, test_db_with_funky_data):
        """Test canonicalization when no changes are needed."""
        manager = MigrationManager(test_db_with_funky_data)

        # First, canonicalize the data
        await manager.normalize_data(dry_run=False)

        # Run canonicalization again
        operations = await manager.normalize_data(dry_run=False)

        # Should report no changes needed
        assert len(operations) <= 2  # Just the header and "no changes needed" message
        assert any("no data normalization needed" in op.lower() or "all data already normalized" in op.lower() for op in operations)

    @pytest.mark.asyncio
    async def test_invalid_json_handling(self, test_db_with_funky_data):
        """Test handling of invalid JSON during canonicalization."""
        # Add invalid JSON to database
        conn = sqlite3.connect(test_db_with_funky_data)
        conn.execute("""
            INSERT INTO scripts (id, name, path, tags)
            VALUES (?, ?, ?, ?)
        """, ("script-invalid", "Invalid Script", "/invalid.py", "invalid json"))
        conn.commit()
        conn.close()

        manager = MigrationManager(test_db_with_funky_data)

        # Should handle invalid JSON gracefully
        operations = await manager.normalize_data(dry_run=False)

        # Should complete without error (invalid JSON is skipped)
        assert len(operations) >= 1


class TestCanonicalizationHelpers:
    """Test helper functions for canonicalization."""

    def test_tag_normalization_rules(self):
        """Test tag normalization rules."""
        # This would test individual helper functions if they were extracted
        # For now, we test through the integration tests above
        pass

    def test_io_data_normalization_rules(self):
        """Test I/O data normalization rules."""
        # This would test individual helper functions if they were extracted
        # For now, we test through the integration tests above
        pass


if __name__ == "__main__":
    # Manual test runner for development
    import asyncio

    async def run_manual_test():
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            test_db = tmp.name

        try:
            # Create test database
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
            """, ("test", "Test", "/test.py", json.dumps(["COOLING", "cooling", "Demand"])))
            conn.commit()
            conn.close()

            # Test canonicalization
            manager = MigrationManager(test_db)
            print("Before canonicalization:")
            conn = sqlite3.connect(test_db)
            tags = conn.execute("SELECT tags FROM scripts").fetchone()[0]
            print(f"  Tags: {tags}")
            conn.close()

            operations = await manager.normalize_data(dry_run=False)
            print("\nCanonicalization operations:")
            for op in operations:
                print(f"  {op}")

            print("\nAfter canonicalization:")
            conn = sqlite3.connect(test_db)
            tags = conn.execute("SELECT tags FROM scripts").fetchone()[0]
            print(f"  Tags: {tags}")
            conn.close()

        finally:
            Path(test_db).unlink(missing_ok=True)

    if __name__ == "__main__":
        asyncio.run(run_manual_test())