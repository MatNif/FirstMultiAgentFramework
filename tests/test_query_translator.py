"""
Unit tests for QueryTranslatorAgent workflow mapping functionality
"""

import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from agents.translator import QueryTranslatorAgent
from agents.models import Task, Plan, PlanStep
from bus import Router
from db import DAO
from db.models import Workflow, WorkflowStep, Script, ScriptInput


class TestQueryTranslatorAgent(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.router = Router()
        self.dao = AsyncMock(spec=DAO)
        self.translator = QueryTranslatorAgent(self.router, self.dao)

    def test_extract_task_tags(self):
        """Test tag extraction from task"""
        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "zone.geojson", "weather": "weather.epw"},
            constraints={"algorithm": "genetic"},
            raw_text="estimate district cooling demand"
        )

        tags = self.translator._extract_task_tags(task)

        expected_tags = {"cooling", "demand", "district", "geometry", "weather", "algorithm"}
        self.assertTrue(expected_tags.issubset(tags))

    def test_calculate_workflow_score(self):
        """Test workflow scoring by tag overlap"""
        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "zone.geojson"},
            constraints={},
            raw_text="estimate cooling demand"
        )

        # Mock workflow with overlapping tags
        workflow = MagicMock()
        workflow.name = "estimate_cooling_demand"
        workflow.tags = ["cooling", "demand", "thermal", "district"]

        score = self.translator._calculate_workflow_score(task, workflow)

        # Should get points for tag overlap + intent match bonus
        self.assertGreater(score, 5)  # Should have intent bonus

    def test_map_task_inputs_to_script_args(self):
        """Test mapping task inputs to script arguments"""
        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "buildings.geojson", "weather": "weather.epw"},
            constraints={"timestep": "hourly"},
            raw_text="estimate cooling demand"
        )

        # Mock script with inputs
        script = MagicMock()
        script.inputs = [
            MagicMock(name="scenario_config"),
            MagicMock(name="buildings"),
            MagicMock(name="weather_file")
        ]

        args = self.translator._map_task_inputs_to_script_args(task, script)

        self.assertEqual(args["buildings"], "buildings.geojson")
        self.assertEqual(args["weather_file"], "weather.epw")
        self.assertEqual(args["timestep"], "hourly")
        # scenario_config is only added if the script requires it
        if any(inp.name == "scenario_config" for inp in script.inputs):
            self.assertEqual(args["scenario_config"], "scenario.yml")

    async def test_find_best_workflow_cooling_demand(self):
        """Test workflow selection for cooling demand"""
        # Mock workflows from database
        cooling_workflow = MagicMock()
        cooling_workflow.id = "workflow-cooling-demand-001"
        cooling_workflow.name = "estimate_cooling_demand"
        cooling_workflow.tags = ["cooling", "demand", "estimation", "thermal"]

        cost_workflow = MagicMock()
        cost_workflow.id = "workflow-cooling-system-001"
        cost_workflow.name = "design_cost_optimal_cooling_system"
        cost_workflow.tags = ["cost", "optimal", "cooling", "system"]

        self.dao.get_all_workflows.return_value = [cooling_workflow, cost_workflow]

        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "zone.geojson", "weather": "weather.epw"},
            constraints={},
            raw_text="estimate district cooling demand"
        )

        best_workflow = await self.translator._find_best_workflow(task)

        # Should select the cooling demand workflow
        self.assertEqual(best_workflow.name, "estimate_cooling_demand")

    async def test_find_best_workflow_cost_optimization(self):
        """Test workflow selection for cost optimization"""
        # Mock workflows
        cooling_workflow = MagicMock()
        cooling_workflow.name = "estimate_cooling_demand"
        cooling_workflow.tags = ["cooling", "demand", "estimation"]

        cost_workflow = MagicMock()
        cost_workflow.name = "design_cost_optimal_cooling_system"
        cost_workflow.tags = ["cost", "optimal", "cooling", "system", "design"]

        self.dao.get_all_workflows.return_value = [cooling_workflow, cost_workflow]

        task = Task(
            intent="cost optimal design",
            scope="building",
            inputs={"geometry": "building.geojson"},
            constraints={},
            raw_text="design cost optimal cooling system"
        )

        best_workflow = await self.translator._find_best_workflow(task)

        # Should select the cost optimization workflow
        self.assertEqual(best_workflow.name, "design_cost_optimal_cooling_system")

    async def test_compute_plan_with_complete_inputs(self):
        """Test plan computation with all required inputs present"""
        # Mock workflow
        workflow = MagicMock()
        workflow.id = "workflow-001"
        workflow.name = "estimate_cooling_demand"
        workflow.steps = [
            MagicMock(script_id="script-001"),
            MagicMock(script_id="script-002")
        ]

        # Mock scripts
        script1 = MagicMock()
        script1.inputs = [
            MagicMock(name="weather_file", required=True, type="epw"),
            MagicMock(name="buildings", required=True, type="shapefile")
        ]

        script2 = MagicMock()
        script2.inputs = [
            MagicMock(name="results_directory", required=True, type="directory")
        ]

        self.dao.get_script_by_id.side_effect = lambda script_id: script1 if script_id == "script-001" else script2

        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "zone.geojson", "weather": "weather.epw"},
            constraints={},
            raw_text="estimate cooling demand"
        )

        plan = await self.translator._compute_plan(task, workflow)

        # Should have no missing inputs for basic cooling demand
        self.assertEqual(len(plan.missing), 0)
        self.assertEqual(len(plan.plan), 2)  # Two steps
        self.assertIn("cooling demand", plan.explain)

    async def test_compute_plan_with_missing_inputs(self):
        """Test plan computation with missing required inputs"""
        # Mock workflow
        workflow = MagicMock()
        workflow.name = "estimate_cooling_demand"
        workflow.steps = [MagicMock(script_id="script-001")]

        # Mock script with weather requirement
        script = MagicMock()
        script.inputs = [
            MagicMock(name="weather_file", required=True, type="epw"),
            MagicMock(name="buildings", required=True, type="shapefile")
        ]

        self.dao.get_script_by_id.return_value = script

        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={"geometry": "zone.geojson"},  # Missing weather file
            constraints={},
            raw_text="estimate cooling demand"
        )

        plan = await self.translator._compute_plan(task, workflow)

        # Should detect missing weather file
        self.assertIn("weather_epw", plan.missing)

    def test_generate_explanation(self):
        """Test explanation generation"""
        task = Task(
            intent="cooling demand",
            scope="district",
            inputs={},
            constraints={},
            raw_text="estimate cooling demand"
        )

        workflow = MagicMock()
        workflow.name = "estimate_cooling_demand"
        workflow.steps = [
            MagicMock(description="Calculate cooling loads"),
            MagicMock(description="Generate report")
        ]

        explanation = self.translator._generate_explanation(
            task, workflow, {"weather_epw"}, ["geometry"]
        )

        self.assertIn("cooling demand", explanation)
        self.assertIn("district-level", explanation)
        self.assertIn("Calculate cooling loads", explanation)
        self.assertIn("weather_epw", explanation)
        self.assertIn("geometry", explanation)

    def test_generate_assumptions(self):
        """Test assumption generation"""
        task = Task(
            intent="cooling demand",
            scope="building",
            inputs={},
            constraints={},
            raw_text="estimate cooling demand"
        )

        workflow = MagicMock()
        workflow.steps = [
            MagicMock(action="optimize_cooling_systems")
        ]

        assumptions = self.translator._generate_assumptions(task, workflow)

        # Should include standard assumptions
        self.assertTrue(any("building schedules" in assumption for assumption in assumptions))
        self.assertTrue(any("building analysis" in assumption or "Single building analysis" in assumption for assumption in assumptions))
        self.assertTrue(any("cooling" in assumption for assumption in assumptions))
        # Optimization assumption only appears if optimization steps are present
        if any("optimization" in step.action for step in workflow.steps):
            self.assertTrue(any("optimization" in assumption for assumption in assumptions))


class AsyncTestCase(unittest.TestCase):
    """Base class for async tests"""
    def run_async(self, coroutine):
        """Helper to run async tests"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coroutine)
        finally:
            loop.close()


class TestQueryTranslatorAgentAsync(AsyncTestCase):
    """Async tests for QueryTranslatorAgent"""

    def test_cooling_demand_workflow_mapping(self):
        """Test that cooling demand tasks map to correct workflow"""
        async def run_test():
            router = Router()
            dao = AsyncMock(spec=DAO)
            translator = QueryTranslatorAgent(router, dao)

            # Mock cooling demand workflow
            workflow = MagicMock()
            workflow.name = "estimate_cooling_demand"
            workflow.tags = ["cooling", "demand", "estimation", "thermal"]
            dao.get_all_workflows.return_value = [workflow]

            task = Task(
                intent="cooling demand",
                scope="district",
                inputs={"geometry": "zone.geojson", "weather": "weather.epw"},
                constraints={},
                raw_text="estimate district cooling demand"
            )

            result = await translator._find_best_workflow(task)
            self.assertEqual(result.name, "estimate_cooling_demand")

        self.run_async(run_test())

    def test_cost_optimization_workflow_mapping(self):
        """Test that cost optimization tasks map to correct workflow"""
        async def run_test():
            router = Router()
            dao = AsyncMock(spec=DAO)
            translator = QueryTranslatorAgent(router, dao)

            # Mock cost optimization workflow
            workflow = MagicMock()
            workflow.name = "design_cost_optimal_cooling_system"
            workflow.tags = ["cost", "optimal", "cooling", "system", "design"]
            dao.get_all_workflows.return_value = [workflow]

            task = Task(
                intent="cost optimal design",
                scope="building",
                inputs={"geometry": "building.geojson"},
                constraints={},
                raw_text="design cost optimal cooling system"
            )

            result = await translator._find_best_workflow(task)
            self.assertEqual(result.name, "design_cost_optimal_cooling_system")

        self.run_async(run_test())

    def test_ghg_evaluation_workflow_mapping(self):
        """Test that GHG evaluation tasks map to correct workflow"""
        async def run_test():
            router = Router()
            dao = AsyncMock(spec=DAO)
            translator = QueryTranslatorAgent(router, dao)

            # Mock GHG evaluation workflow
            workflow = MagicMock()
            workflow.name = "evaluate_ghg_existing_system"
            workflow.tags = ["ghg", "emissions", "evaluation", "existing", "assessment"]
            dao.get_all_workflows.return_value = [workflow]

            task = Task(
                intent="ghg evaluation",
                scope="district",
                inputs={"data": "energy_consumption.csv"},
                constraints={},
                raw_text="evaluate GHG emissions of existing system"
            )

            result = await translator._find_best_workflow(task)
            self.assertEqual(result.name, "evaluate_ghg_existing_system")

        self.run_async(run_test())


if __name__ == "__main__":
    unittest.main()