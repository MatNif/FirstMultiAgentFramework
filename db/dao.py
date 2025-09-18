import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import aiosqlite
from loguru import logger

from .models import Script, ScriptSearchCriteria, Workflow, WorkflowSearchCriteria


class DAO:
    def __init__(self, db_path: str = "cea_assistant.db") -> None:
        self.db_path = db_path

    async def initialize(self) -> None:
        """Initialize the database with schema"""
        async with aiosqlite.connect(self.db_path) as db:
            # Create scripts table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS scripts (
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

            # Create workflows table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS workflows (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    steps TEXT NOT NULL,
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            await db.execute("CREATE INDEX IF NOT EXISTS idx_scripts_name ON scripts(name)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_scripts_tags ON scripts(tags)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_workflows_name ON workflows(name)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_workflows_tags ON workflows(tags)")

            await db.commit()
            logger.info("Database initialized successfully")

    async def recreate_tables(self) -> None:
        """Drop and recreate all tables"""
        async with aiosqlite.connect(self.db_path) as db:
            # Drop existing tables
            await db.execute("DROP TABLE IF EXISTS scripts")
            await db.execute("DROP TABLE IF EXISTS workflows")
            await db.commit()
            logger.info("Dropped existing tables")

        # Recreate tables
        await self.initialize()

    async def upsert_script(self, script: Script) -> str:
        """Insert or update a script"""
        if script.id is None:
            script.id = str(uuid.uuid4())
            script.created_at = datetime.now()

        script.updated_at = datetime.now()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO scripts
                (id, name, path, cli, doc, inputs, outputs, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                script.id,
                script.name,
                script.path,
                script.cli,
                script.doc,
                json.dumps([input.model_dump() for input in script.inputs]),
                json.dumps([output.model_dump() for output in script.outputs]),
                json.dumps(script.tags),
                script.created_at.isoformat() if script.created_at else None,
                script.updated_at.isoformat() if script.updated_at else None,
            ))
            await db.commit()
            logger.info(f"Upserted script: {script.name} (ID: {script.id})")
            return script.id

    async def find_scripts_by_tags(self, tags: List[str]) -> List[Script]:
        """Find scripts that match any of the given tags"""
        if not tags:
            return []

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # Build query to find scripts with any matching tags
            placeholders = ",".join("?" for _ in tags)
            sql = f"""
                SELECT * FROM scripts
                WHERE id IN (
                    SELECT DISTINCT s.id FROM scripts s
                    WHERE {" OR ".join("s.tags LIKE ?" for _ in tags)}
                )
                ORDER BY name
            """

            # Create LIKE patterns for tag matching
            params = [f'%"{tag}"%' for tag in tags]

            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()

            scripts = []
            for row in rows:
                script_data = dict(row)
                script_data["inputs"] = json.loads(script_data["inputs"] or "[]")
                script_data["outputs"] = json.loads(script_data["outputs"] or "[]")
                script_data["tags"] = json.loads(script_data["tags"] or "[]")

                # Convert to datetime objects
                if script_data["created_at"]:
                    script_data["created_at"] = datetime.fromisoformat(script_data["created_at"])
                if script_data["updated_at"]:
                    script_data["updated_at"] = datetime.fromisoformat(script_data["updated_at"])

                scripts.append(Script(**script_data))

            logger.info(f"Found {len(scripts)} scripts matching tags: {tags}")
            return scripts

    async def search_scripts(self, criteria: Optional[ScriptSearchCriteria] = None) -> List[Script]:
        """Search scripts with flexible criteria"""
        if criteria is None:
            criteria = ScriptSearchCriteria()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            sql = "SELECT * FROM scripts WHERE 1=1"
            params = []

            if criteria.name:
                sql += " AND name LIKE ?"
                params.append(f"%{criteria.name}%")

            if criteria.description:
                sql += " AND doc LIKE ?"
                params.append(f"%{criteria.description}%")

            if criteria.tags:
                tag_conditions = []
                for tag in criteria.tags:
                    tag_conditions.append("tags LIKE ?")
                    params.append(f'%"{tag}"%')
                sql += f" AND ({' OR '.join(tag_conditions)})"

            sql += " ORDER BY name"

            if criteria.limit:
                sql += " LIMIT ?"
                params.append(criteria.limit)

            if criteria.offset:
                sql += " OFFSET ?"
                params.append(criteria.offset)

            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()

            scripts = []
            for row in rows:
                script_data = dict(row)
                script_data["inputs"] = json.loads(script_data["inputs"] or "[]")
                script_data["outputs"] = json.loads(script_data["outputs"] or "[]")
                script_data["tags"] = json.loads(script_data["tags"] or "[]")

                # Convert to datetime objects
                if script_data["created_at"]:
                    script_data["created_at"] = datetime.fromisoformat(script_data["created_at"])
                if script_data["updated_at"]:
                    script_data["updated_at"] = datetime.fromisoformat(script_data["updated_at"])

                scripts.append(Script(**script_data))

            return scripts

    async def get_script_by_id(self, script_id: str) -> Optional[Script]:
        """Get a script by its ID"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM scripts WHERE id = ?", (script_id,))
            row = await cursor.fetchone()

            if row:
                script_data = dict(row)
                script_data["inputs"] = json.loads(script_data["inputs"] or "[]")
                script_data["outputs"] = json.loads(script_data["outputs"] or "[]")
                script_data["tags"] = json.loads(script_data["tags"] or "[]")

                # Convert to datetime objects
                if script_data["created_at"]:
                    script_data["created_at"] = datetime.fromisoformat(script_data["created_at"])
                if script_data["updated_at"]:
                    script_data["updated_at"] = datetime.fromisoformat(script_data["updated_at"])

                return Script(**script_data)
            return None

    async def upsert_workflow(self, workflow: Workflow) -> str:
        """Insert or update a workflow"""
        if workflow.id is None:
            workflow.id = str(uuid.uuid4())
            workflow.created_at = datetime.now()

        workflow.updated_at = datetime.now()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO workflows
                (id, name, description, steps, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                workflow.id,
                workflow.name,
                workflow.description,
                json.dumps([step.model_dump() for step in workflow.steps]),
                json.dumps(workflow.tags),
                workflow.created_at.isoformat() if workflow.created_at else None,
                workflow.updated_at.isoformat() if workflow.updated_at else None,
            ))
            await db.commit()
            logger.info(f"Upserted workflow: {workflow.name} (ID: {workflow.id})")
            return workflow.id

    async def get_workflow_by_name(self, name: str) -> Optional[Workflow]:
        """Get a workflow by its name"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM workflows WHERE name = ?", (name,))
            row = await cursor.fetchone()

            if row:
                workflow_data = dict(row)
                workflow_data["steps"] = json.loads(workflow_data["steps"])
                workflow_data["tags"] = json.loads(workflow_data["tags"] or "[]")

                # Convert to datetime objects
                if workflow_data["created_at"]:
                    workflow_data["created_at"] = datetime.fromisoformat(workflow_data["created_at"])
                if workflow_data["updated_at"]:
                    workflow_data["updated_at"] = datetime.fromisoformat(workflow_data["updated_at"])

                return Workflow(**workflow_data)
            return None

    async def search_workflows(self, criteria: Optional[WorkflowSearchCriteria] = None) -> List[Workflow]:
        """Search workflows with flexible criteria"""
        if criteria is None:
            criteria = WorkflowSearchCriteria()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            sql = "SELECT * FROM workflows WHERE 1=1"
            params = []

            if criteria.name:
                sql += " AND name LIKE ?"
                params.append(f"%{criteria.name}%")

            if criteria.description:
                sql += " AND description LIKE ?"
                params.append(f"%{criteria.description}%")

            if criteria.tags:
                tag_conditions = []
                for tag in criteria.tags:
                    tag_conditions.append("tags LIKE ?")
                    params.append(f'%"{tag}"%')
                sql += f" AND ({' OR '.join(tag_conditions)})"

            sql += " ORDER BY name"

            if criteria.limit:
                sql += " LIMIT ?"
                params.append(criteria.limit)

            if criteria.offset:
                sql += " OFFSET ?"
                params.append(criteria.offset)

            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()

            workflows = []
            for row in rows:
                workflow_data = dict(row)
                workflow_data["steps"] = json.loads(workflow_data["steps"])
                workflow_data["tags"] = json.loads(workflow_data["tags"] or "[]")

                # Convert to datetime objects
                if workflow_data["created_at"]:
                    workflow_data["created_at"] = datetime.fromisoformat(workflow_data["created_at"])
                if workflow_data["updated_at"]:
                    workflow_data["updated_at"] = datetime.fromisoformat(workflow_data["updated_at"])

                workflows.append(Workflow(**workflow_data))

            return workflows

    async def get_all_scripts(self) -> List[Script]:
        """Get all scripts in the database"""
        return await self.search_scripts()

    async def get_all_workflows(self) -> List[Workflow]:
        """Get all workflows in the database"""
        return await self.search_workflows()

    # Legacy methods for backward compatibility
    async def add_script(self, script_data: dict) -> str:
        """Legacy method - converts dict to Script model"""
        script = Script(**script_data)
        return await self.upsert_script(script)

    async def add_workflow(self, workflow_data: dict) -> str:
        """Legacy method - converts dict to Workflow model"""
        workflow = Workflow(**workflow_data)
        return await self.upsert_workflow(workflow)