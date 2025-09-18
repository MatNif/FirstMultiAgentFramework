"""
Capabilities Provider Interface

Defines the interface for accessing CEA script capabilities and workflows.
This allows for easy swapping between DAO-based and MCP-based implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from db.models import Script, Workflow


class CapabilitiesProvider(ABC):
    """
    Abstract interface for accessing CEA capabilities (scripts and workflows).

    This interface allows the QueryTranslatorAgent to work with different
    data sources without being tightly coupled to a specific implementation.

    Current implementation: DAOCapabilitiesProvider (uses SQLite DAO)
    Future implementation: MCPCapabilitiesProvider (uses MCP server calls)
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the capabilities provider."""
        pass

    @abstractmethod
    async def get_all_workflows(self) -> List[Workflow]:
        """
        Get all available workflows.

        Returns:
            List of Workflow objects
        """
        pass

    @abstractmethod
    async def get_workflow_by_id(self, workflow_id: str) -> Optional[Workflow]:
        """
        Get a specific workflow by ID.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            Workflow object or None if not found
        """
        pass

    @abstractmethod
    async def get_script_by_id(self, script_id: str) -> Optional[Script]:
        """
        Get a specific script by ID.

        Args:
            script_id: Unique script identifier

        Returns:
            Script object or None if not found
        """
        pass

    @abstractmethod
    async def search_scripts(self,
                           query: str = "",
                           category: Optional[str] = None,
                           tags: Optional[List[str]] = None) -> List[Script]:
        """
        Search for scripts matching criteria.

        Args:
            query: Text query for script search
            category: Optional category filter
            tags: Optional list of tags to filter by

        Returns:
            List of matching Script objects
        """
        pass

    @abstractmethod
    async def get_script_help(self, script_id: str) -> Dict[str, Any]:
        """
        Get detailed help information for a script.

        Args:
            script_id: Unique script identifier

        Returns:
            Dictionary with detailed script information
        """
        pass


class DAOCapabilitiesProvider(CapabilitiesProvider):
    """
    DAO-based implementation of CapabilitiesProvider.

    Uses the existing SQLite DAO for accessing scripts and workflows.
    This is the current implementation.
    """

    def __init__(self, dao):
        """
        Initialize with a DAO instance.

        Args:
            dao: Database access object
        """
        self.dao = dao

    async def initialize(self) -> None:
        """Initialize the DAO."""
        await self.dao.initialize()

    async def get_all_workflows(self) -> List[Workflow]:
        """Get all workflows from DAO."""
        return await self.dao.get_all_workflows()

    async def get_workflow_by_id(self, workflow_id: str) -> Optional[Workflow]:
        """Get workflow by ID from DAO."""
        return await self.dao.get_workflow_by_id(workflow_id)

    async def get_script_by_id(self, script_id: str) -> Optional[Script]:
        """Get script by ID from DAO."""
        return await self.dao.get_script_by_id(script_id)

    async def search_scripts(self,
                           query: str = "",
                           category: Optional[str] = None,
                           tags: Optional[List[str]] = None) -> List[Script]:
        """Search scripts using DAO."""
        # The current DAO search_scripts method only takes a query parameter
        # For now, we'll use the query and filter results if needed
        scripts = await self.dao.search_scripts(query)

        # Apply additional filters
        if category:
            scripts = [s for s in scripts if category.lower() in (s.category or "").lower()]

        if tags:
            tag_set = set(tag.lower() for tag in tags)
            scripts = [s for s in scripts if tag_set.intersection(set(s.tags or []))]

        return scripts

    async def get_script_help(self, script_id: str) -> Dict[str, Any]:
        """Get script help information from DAO."""
        script = await self.dao.get_script_by_id(script_id)

        if not script:
            raise ValueError(f"Script not found: {script_id}")

        # Convert Script object to help dictionary
        help_info = {
            "id": script.id,
            "name": script.name,
            "description": script.doc or "No description available",
            "category": script.category,
            "tags": script.tags or [],
            "file_path": script.file_path,
            "command": script.command,
            "inputs": [
                {
                    "name": inp.name,
                    "type": inp.type,
                    "required": inp.required,
                    "description": inp.description or "",
                    "default": inp.default
                }
                for inp in script.inputs
            ] if script.inputs else [],
            "outputs": script.outputs or []
        }

        return help_info


class MCPCapabilitiesProvider(CapabilitiesProvider):
    """
    MCP-based implementation of CapabilitiesProvider.

    Uses MCP server calls to access CEA capabilities remotely.
    This is a future implementation stub.
    """

    def __init__(self, mcp_client):
        """
        Initialize with an MCP client.

        Args:
            mcp_client: MCP client for making server calls
        """
        self.mcp_client = mcp_client
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the MCP client connection."""
        # In a real implementation, this would establish MCP connection
        # await self.mcp_client.connect()
        self._initialized = True

    async def get_all_workflows(self) -> List[Workflow]:
        """Get all workflows via MCP."""
        # Stub implementation
        # In real implementation:
        # workflows_data = await self.mcp_client.call("list_workflows")
        # return [Workflow.from_dict(w) for w in workflows_data]
        raise NotImplementedError("MCP integration not yet implemented")

    async def get_workflow_by_id(self, workflow_id: str) -> Optional[Workflow]:
        """Get workflow by ID via MCP."""
        # Stub implementation
        # In real implementation:
        # workflow_data = await self.mcp_client.call("get_workflow", {"id": workflow_id})
        # return Workflow.from_dict(workflow_data) if workflow_data else None
        raise NotImplementedError("MCP integration not yet implemented")

    async def get_script_by_id(self, script_id: str) -> Optional[Script]:
        """Get script by ID via MCP."""
        # Stub implementation
        # In real implementation:
        # script_data = await self.mcp_client.call("get_script", {"id": script_id})
        # return Script.from_dict(script_data) if script_data else None
        raise NotImplementedError("MCP integration not yet implemented")

    async def search_scripts(self,
                           query: str = "",
                           category: Optional[str] = None,
                           tags: Optional[List[str]] = None) -> List[Script]:
        """Search scripts via MCP."""
        # Stub implementation
        # In real implementation:
        # scripts_data = await self.mcp_client.call("list_scripts", {
        #     "query": query,
        #     "category": category,
        #     "tags": tags
        # })
        # return [Script.from_dict(s) for s in scripts_data]
        raise NotImplementedError("MCP integration not yet implemented")

    async def get_script_help(self, script_id: str) -> Dict[str, Any]:
        """Get script help via MCP."""
        # Stub implementation
        # In real implementation:
        # return await self.mcp_client.call("script_help", {"script_id": script_id})
        raise NotImplementedError("MCP integration not yet implemented")