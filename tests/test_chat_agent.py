"""
Unit tests for ChatAgent task parsing functionality
"""

import unittest
from agents.chat import ChatAgent
from agents.models import Task
from bus import Router


class TestChatAgentTaskParsing(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.router = Router()
        self.chat_agent = ChatAgent(self.router)

    def test_parse_task_cooling_demand_district(self):
        """Test parsing of district cooling demand request"""
        user_text = "estimate district cooling demand from zone.geojson and weather.epw"

        task = self.chat_agent._parse_task(user_text)

        self.assertEqual(task.intent, "cooling demand")
        self.assertEqual(task.scope, "district")
        self.assertIn("geometry", task.inputs)
        self.assertEqual(task.inputs["geometry"], "zone.geojson")
        self.assertIn("weather", task.inputs)
        self.assertEqual(task.inputs["weather"], "weather.epw")
        self.assertEqual(task.raw_text, user_text)

    def test_parse_task_network_optimization(self):
        """Test parsing of network optimization request"""
        user_text = "optimize thermal network for data.csv using genetic algorithm"

        task = self.chat_agent._parse_task(user_text)

        self.assertEqual(task.intent, "network")
        self.assertIsNone(task.scope)  # No explicit scope mentioned
        self.assertIn("data", task.inputs)
        self.assertEqual(task.inputs["data"], "data.csv")
        self.assertIn("algorithm", task.constraints)
        self.assertEqual(task.constraints["algorithm"], "genetic")
        self.assertEqual(task.raw_text, user_text)

    def test_parse_task_building_cost_analysis(self):
        """Test parsing of building-level cost analysis"""
        user_text = "calculate building cost with hourly analysis using schedule.xlsx"

        task = self.chat_agent._parse_task(user_text)

        self.assertEqual(task.intent, "cost")
        self.assertEqual(task.scope, "building")
        self.assertIn("schedule", task.inputs)
        self.assertEqual(task.inputs["schedule"], "schedule.xlsx")
        self.assertIn("timestep", task.constraints)
        self.assertEqual(task.constraints["timestep"], "hourly")
        self.assertEqual(task.raw_text, user_text)

    def test_parse_task_ghg_emissions(self):
        """Test parsing of GHG emissions analysis"""
        user_text = "analyze carbon emissions for district energy system"

        task = self.chat_agent._parse_task(user_text)

        self.assertEqual(task.intent, "ghg")
        self.assertEqual(task.scope, "district")
        self.assertEqual(len(task.inputs), 0)  # No specific files mentioned
        self.assertEqual(len(task.constraints), 0)  # No constraints mentioned
        self.assertEqual(task.raw_text, user_text)

    def test_parse_task_tech_selection(self):
        """Test parsing of technology selection request"""
        user_text = "choose building technology system with config.json at 22°C"

        task = self.chat_agent._parse_task(user_text)

        self.assertEqual(task.intent, "tech selection")
        self.assertEqual(task.scope, "building")
        self.assertIn("config", task.inputs)
        self.assertEqual(task.inputs["config"], "config.json")
        self.assertIn("temperature", task.constraints)
        self.assertEqual(task.constraints["temperature"], "22°C")
        self.assertEqual(task.raw_text, user_text)

    def test_detect_intent_keywords(self):
        """Test intent detection with various keywords"""
        # Test cooling demand
        self.assertEqual(
            self.chat_agent._detect_intent("estimate cooling demand", {}),
            "general_analysis"  # Empty intent_keywords should return default
        )

        intent_keywords = {
            "cooling demand": ["cooling", "demand", "cool", "estimate"],
            "network": ["network", "pipe", "distribution", "optimize"],
        }

        self.assertEqual(
            self.chat_agent._detect_intent("estimate cooling demand", intent_keywords),
            "cooling demand"
        )

        self.assertEqual(
            self.chat_agent._detect_intent("optimize pipe network", intent_keywords),
            "network"
        )

    def test_detect_scope(self):
        """Test scope detection"""
        self.assertEqual(
            self.chat_agent._detect_scope("analyze district energy system"),
            "district"
        )

        self.assertEqual(
            self.chat_agent._detect_scope("calculate building cooling demand"),
            "building"
        )

        self.assertIsNone(
            self.chat_agent._detect_scope("general energy analysis")
        )

    def test_extract_file_inputs(self):
        """Test file input extraction"""
        inputs = self.chat_agent._extract_file_inputs("use zone.geojson and weather.epw with data.csv")

        self.assertIn("geometry", inputs)
        self.assertEqual(inputs["geometry"], "zone.geojson")
        self.assertIn("weather", inputs)
        self.assertEqual(inputs["weather"], "weather.epw")
        self.assertIn("data", inputs)
        self.assertEqual(inputs["data"], "data.csv")

    def test_extract_constraints(self):
        """Test constraint extraction"""
        constraints = self.chat_agent._extract_constraints("hourly analysis at 22°C using genetic algorithm")

        self.assertIn("timestep", constraints)
        self.assertEqual(constraints["timestep"], "hourly")
        self.assertIn("temperature", constraints)
        self.assertEqual(constraints["temperature"], "22°C")
        self.assertIn("algorithm", constraints)
        self.assertEqual(constraints["algorithm"], "genetic")

    def test_faq_lookup(self):
        """Test FAQ lookup functionality"""
        # Test exact match
        answer = self.chat_agent._lookup_faq("What is CEA?")
        self.assertIsNotNone(answer)
        self.assertIn("City Energy Analyst", answer)

        # Test partial match with question pattern
        answer = self.chat_agent._lookup_faq("how do I calculate cooling demand")
        self.assertIsNotNone(answer)
        self.assertIn("demand analysis script", answer)

        # Test no match (non-question pattern)
        answer = self.chat_agent._lookup_faq("estimate cooling demand")
        self.assertIsNone(answer)

        # Test no match (question pattern but no matching content)
        answer = self.chat_agent._lookup_faq("how do I cook pasta?")
        self.assertIsNone(answer)


if __name__ == "__main__":
    unittest.main()