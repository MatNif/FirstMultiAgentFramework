#!/usr/bin/env python3
"""
Test script for refresh catalog functionality
"""

import asyncio
from pathlib import Path

from agents.config import config
from agents.script_discovery import ScriptDiscovery
from db import DAO


async def test_script_discovery():
    """Test script discovery functionality"""
    print("Testing script discovery...")

    # Test script discovery
    discovery = ScriptDiscovery(str(config.get_cea_root()), config.get_script_discovery_timeout())

    print(f"CEA_ROOT: {discovery.cea_root}")
    print(f"Timeout: {discovery.timeout}")

    # Test individual script analysis
    test_script = Path("test_cea_scripts/demand_analysis.py")
    if test_script.exists():
        print(f"\nTesting individual script: {test_script}")
        help_output = await discovery._get_help_output(test_script)
        print(f"Help output length: {len(help_output) if help_output else 0}")
        if help_output:
            print(f"Help output preview: {help_output[:200]}...")

            script = await discovery._analyze_script(test_script)
            if script:
                print(f"Analyzed script: {script.name}")
                print(f"  Doc: {script.doc}")
                print(f"  Tags: {script.tags}")
                print(f"  Inputs: {len(script.inputs)}")
            else:
                print("Failed to analyze script")

    scripts = await discovery.discover_scripts()

    print(f"Discovered {len(scripts)} scripts:")
    for script in scripts:
        print(f"  - {script.name}: {script.doc[:50]}...")
        print(f"    Tags: {script.tags}")
        print(f"    Inputs: {len(script.inputs)}, Outputs: {len(script.outputs)}")

    return scripts


async def test_dao_upsert():
    """Test DAO upsert functionality"""
    print("\nTesting DAO upsert...")

    dao = DAO("test_refresh.db")
    await dao.initialize()

    scripts = await test_script_discovery()

    for script in scripts:
        script_id = await dao.upsert_script(script)
        print(f"Upserted script {script.name} with ID: {script_id}")

    # Verify scripts in database
    all_scripts = await dao.get_all_scripts()
    print(f"\nDatabase now contains {len(all_scripts)} scripts")

    return all_scripts


if __name__ == "__main__":
    asyncio.run(test_dao_upsert())