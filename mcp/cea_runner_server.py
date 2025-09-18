"""
MCP CEA Runner Server

This module provides MCP-compatible interfaces for CEA script discovery and execution.
Currently implemented as stubs that delegate to the existing DAO-based system.

In the future, this will be a proper MCP server that can be called remotely.
"""

import asyncio
from typing import Any, Dict, List, Optional
from pathlib import Path

from loguru import logger

from db import DAO
from db.models import Script


class CEARunnerServer:
    """
    MCP server for CEA script operations.

    Provides MCP-compatible callable functions with the same signatures
    they would have if exposed via MCP protocol.

    Future MCP integration will replace the direct DAO usage with
    actual MCP client calls to a remote CEA runner server.
    """

    def __init__(self, dao: Optional[DAO] = None):
        """
        Initialize CEA Runner Server.

        Args:
            dao: Database access object. If None, will create a new instance.
        """
        self.dao = dao or DAO()
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the server and underlying DAO."""
        if not self._initialized:
            await self.dao.initialize()
            self._initialized = True
            logger.info("CEA Runner Server initialized")

    async def list_scripts(self,
                          category: Optional[str] = None,
                          tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        List available CEA scripts.

        MCP-compatible function signature for script discovery.

        Args:
            category: Optional category filter (e.g., "demand", "networks", "optimization")
            tags: Optional list of tags to filter by

        Returns:
            List of script dictionaries with metadata

        Example:
            scripts = await server.list_scripts(category="demand")
            scripts = await server.list_scripts(tags=["cooling", "thermal"])
        """
        if not self._initialized:
            await self.initialize()

        try:
            # Get all scripts from DAO
            scripts = await self.dao.search_scripts(query="")

            # Filter by category if provided
            if category:
                scripts = [s for s in scripts if category.lower() in (s.category or "").lower()]

            # Filter by tags if provided
            if tags:
                tag_set = set(tag.lower() for tag in tags)
                scripts = [s for s in scripts if tag_set.intersection(set(s.tags or []))]

            # Convert to MCP-compatible format
            result = []
            for script in scripts:
                script_dict = {
                    "id": script.id,
                    "name": script.name,
                    "description": script.doc or "No description available",
                    "category": script.category,
                    "tags": script.tags or [],
                    "file_path": script.file_path,
                    "inputs": [
                        {
                            "name": inp.name,
                            "type": inp.type,
                            "required": inp.required,
                            "description": inp.description or ""
                        }
                        for inp in script.inputs
                    ] if script.inputs else [],
                    "outputs": script.outputs or []
                }
                result.append(script_dict)

            logger.debug(f"Listed {len(result)} scripts (category={category}, tags={tags})")
            return result

        except Exception as e:
            logger.error(f"Error listing scripts: {e}")
            raise

    async def script_help(self, script_id: str) -> Dict[str, Any]:
        """
        Get detailed help information for a specific script.

        MCP-compatible function signature for script introspection.

        Args:
            script_id: Unique identifier of the script

        Returns:
            Dictionary with detailed script information

        Example:
            help_info = await server.script_help("cea-demand-calculation")
        """
        if not self._initialized:
            await self.initialize()

        try:
            script = await self.dao.get_script_by_id(script_id)

            if not script:
                raise ValueError(f"Script not found: {script_id}")

            # Build comprehensive help information
            help_info = {
                "id": script.id,
                "name": script.name,
                "description": script.doc or "No description available",
                "category": script.category,
                "tags": script.tags or [],
                "file_path": script.file_path,
                "command": script.command,
                "inputs": [],
                "outputs": script.outputs or [],
                "examples": [],
                "usage": f"python {script.file_path}" if script.file_path else "Usage information not available"
            }

            # Detailed input information
            if script.inputs:
                for inp in script.inputs:
                    input_info = {
                        "name": inp.name,
                        "type": inp.type,
                        "required": inp.required,
                        "description": inp.description or "No description",
                        "default": inp.default,
                        "example": f"--{inp.name} example_value" if inp.name else ""
                    }
                    help_info["inputs"].append(input_info)

            # Generate usage examples
            if script.inputs:
                required_args = [inp for inp in script.inputs if inp.required]
                if required_args:
                    example_args = []
                    for inp in required_args[:3]:  # Show first 3 required args
                        example_args.append(f"--{inp.name} example_{inp.name}")

                    help_info["examples"].append({
                        "description": "Basic usage with required arguments",
                        "command": f"python {script.file_path} {' '.join(example_args)}"
                    })

            logger.debug(f"Retrieved help for script: {script_id}")
            return help_info

        except Exception as e:
            logger.error(f"Error getting script help for {script_id}: {e}")
            raise

    async def run_script(self,
                        script_id: str,
                        args: Dict[str, Any],
                        timeout: Optional[float] = None,
                        working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a CEA script with provided arguments.

        MCP-compatible function signature for script execution.

        Args:
            script_id: Unique identifier of the script to run
            args: Dictionary of argument name -> value mappings
            timeout: Optional timeout in seconds (default: 300)
            working_dir: Optional working directory for execution

        Returns:
            Dictionary with execution results

        Example:
            result = await server.run_script(
                "cea-demand-calculation",
                {
                    "scenario": "baseline",
                    "weather_file": "weather.epw",
                    "buildings": "zone.shp"
                },
                timeout=600
            )
        """
        if not self._initialized:
            await self.initialize()

        try:
            script = await self.dao.get_script_by_id(script_id)

            if not script:
                raise ValueError(f"Script not found: {script_id}")

            # Validate required arguments
            if script.inputs:
                required_inputs = [inp.name for inp in script.inputs if inp.required]
                missing_args = [arg for arg in required_inputs if arg not in args]

                if missing_args:
                    raise ValueError(f"Missing required arguments: {missing_args}")

            # Build command for execution
            cmd_parts = []
            if script.file_path:
                cmd_parts.extend(["python", str(script.file_path)])
            elif script.command:
                cmd_parts.extend(script.command.split())
            else:
                raise ValueError(f"Script {script_id} has no executable command or file path")

            # Add arguments
            for arg_name, arg_value in args.items():
                cmd_parts.extend([f"--{arg_name}", str(arg_value)])

            # Set default timeout
            if timeout is None:
                timeout = 300.0  # 5 minutes default

            # Set working directory
            if working_dir is None and script.file_path:
                working_dir = str(Path(script.file_path).parent)

            # Execute the script (stub implementation)
            # In real MCP implementation, this would make an actual subprocess call
            logger.info(f"Would execute: {' '.join(cmd_parts)} (timeout={timeout}s, cwd={working_dir})")

            # Simulate execution result
            result = {
                "script_id": script_id,
                "command": " ".join(cmd_parts),
                "exit_code": 0,  # Simulated success
                "stdout": f"Simulated output from {script.name}",
                "stderr": "",
                "execution_time": 1.5,  # Simulated execution time
                "working_directory": working_dir,
                "arguments": args,
                "status": "completed"
            }

            logger.info(f"Script {script_id} executed successfully (simulated)")
            return result

        except Exception as e:
            logger.error(f"Error executing script {script_id}: {e}")
            # Return error result in MCP-compatible format
            return {
                "script_id": script_id,
                "command": "",
                "exit_code": 1,
                "stdout": "",
                "stderr": str(e),
                "execution_time": 0.0,
                "working_directory": working_dir,
                "arguments": args,
                "status": "failed",
                "error": str(e)
            }

    async def shutdown(self) -> None:
        """Clean shutdown of the server."""
        if self._initialized:
            # In a real MCP server, this would close connections, cleanup resources
            logger.info("CEA Runner Server shutting down")
            self._initialized = False


# Convenience function for creating a server instance
async def create_cea_runner_server(dao: Optional[DAO] = None) -> CEARunnerServer:
    """
    Create and initialize a CEA Runner Server instance.

    Args:
        dao: Optional DAO instance. If None, creates a new one.

    Returns:
        Initialized CEARunnerServer instance
    """
    server = CEARunnerServer(dao)
    await server.initialize()
    return server