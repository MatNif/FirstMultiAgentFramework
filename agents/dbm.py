from typing import Any, Dict, List

from loguru import logger

from bus import Message, Performative, Router
from db import DAO
from .base import BaseAgent
from .config import config
from .script_discovery import ScriptDiscovery


class DatabaseManagerAgent(BaseAgent):
    def __init__(self, router: Router, dao: DAO) -> None:
        super().__init__("dbm", router)
        self.dao = dao
        self.script_discovery = ScriptDiscovery(
            str(config.get_cea_root()),
            config.get_script_discovery_timeout()
        )
        self.setup_handlers()

    def setup_handlers(self) -> None:
        @self.on("refresh_catalog")
        async def handle_refresh_catalog(message: Message) -> None:
            logger.info(f"DatabaseManagerAgent received refresh catalog request: {message.content}")

            try:
                # Get optional path override from message
                cea_root_override = message.content.get("cea_root")
                if cea_root_override:
                    discovery = ScriptDiscovery(cea_root_override, config.get_script_discovery_timeout())
                else:
                    discovery = self.script_discovery

                # Discover scripts
                logger.info(f"Starting script discovery in: {discovery.cea_root}")
                scripts = await discovery.discover_scripts()

                # Upsert discovered scripts into database
                upserted_count = 0
                for script in scripts:
                    try:
                        await self.dao.upsert_script(script)
                        upserted_count += 1
                    except Exception as e:
                        logger.error(f"Failed to upsert script {script.name}: {e}")

                logger.info(f"Catalog refresh completed: {len(scripts)} discovered, {upserted_count} upserted")

                await self.reply(
                    message,
                    Performative.INFORM,
                    "catalog_refreshed",
                    {
                        "scripts_discovered": len(scripts),
                        "scripts_upserted": upserted_count,
                        "cea_root": str(discovery.cea_root)
                    }
                )

            except Exception as e:
                logger.error(f"Error refreshing catalog: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to refresh catalog: {str(e)}"}
                )

        @self.on("script_search")
        async def handle_script_search(message: Message) -> None:
            logger.info(f"DatabaseManagerAgent received script search: {message.content}")

            tags = message.content.get("tags", [])
            query = message.content.get("query", "")

            try:
                # Use the new DAO search methods
                if tags:
                    scripts = await self.dao.find_scripts_by_tags(tags)
                else:
                    from db.models import ScriptSearchCriteria
                    criteria = ScriptSearchCriteria(name=query, description=query)
                    scripts = await self.dao.search_scripts(criteria)

                # Convert to dict format for backward compatibility
                scripts_dict = [
                    {
                        "id": script.id,
                        "name": script.name,
                        "path": script.path,
                        "doc": script.doc,
                        "tags": script.tags,
                        "inputs": [inp.model_dump() for inp in script.inputs],
                        "outputs": [out.model_dump() for out in script.outputs]
                    }
                    for script in scripts
                ]

                await self.reply(
                    message,
                    Performative.INFORM,
                    "script_results",
                    {"scripts": scripts_dict, "count": len(scripts_dict)}
                )
            except Exception as e:
                logger.error(f"Error searching scripts: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to search scripts: {str(e)}"}
                )

        @self.on("workflow_search")
        async def handle_workflow_search(message: Message) -> None:
            logger.info(f"DatabaseManagerAgent received workflow search: {message.content}")

            query = message.content.get("query", "")

            try:
                workflows = await self.dao.search_workflows(query=query)
                await self.reply(
                    message,
                    Performative.INFORM,
                    "workflow_results",
                    {"workflows": workflows, "count": len(workflows)}
                )
            except Exception as e:
                logger.error(f"Error searching workflows: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to search workflows: {str(e)}"}
                )

        @self.on("add_script")
        async def handle_add_script(message: Message) -> None:
            logger.info(f"DatabaseManagerAgent received add script request: {message.content}")

            script_data = message.content.get("script", {})

            try:
                script_id = await self.dao.add_script(script_data)
                await self.reply(
                    message,
                    Performative.INFORM,
                    "script_added",
                    {"script_id": script_id, "success": True}
                )
            except Exception as e:
                logger.error(f"Error adding script: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to add script: {str(e)}"}
                )

        @self.on("add_workflow")
        async def handle_add_workflow(message: Message) -> None:
            logger.info(f"DatabaseManagerAgent received add workflow request: {message.content}")

            workflow_data = message.content.get("workflow", {})

            try:
                workflow_id = await self.dao.add_workflow(workflow_data)
                await self.reply(
                    message,
                    Performative.INFORM,
                    "workflow_added",
                    {"workflow_id": workflow_id, "success": True}
                )
            except Exception as e:
                logger.error(f"Error adding workflow: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to add workflow: {str(e)}"}
                )