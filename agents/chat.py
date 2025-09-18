import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from bus import Message, Performative, Router
from .base import BaseAgent
from .models import Task


class ChatAgent(BaseAgent):
    def __init__(self, router: Router) -> None:
        super().__init__("chat", router)
        self.pending_queries = {}  # Track original queries by conversation_id
        self.glossary = self._load_glossary()
        self.setup_handlers()

    def _load_glossary(self) -> dict:
        """Load FAQ glossary from data/glossary.json"""
        try:
            glossary_path = Path("data/glossary.json")
            if glossary_path.exists():
                with open(glossary_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"Glossary file not found: {glossary_path}")
                return {"faq": []}
        except Exception as e:
            logger.error(f"Failed to load glossary: {e}")
            return {"faq": []}

    def setup_handlers(self) -> None:
        @self.on("query")
        async def handle_query(message: Message) -> None:
            logger.info(f"ChatAgent received query: {message.content}")

            query = message.content.get("question", "")
            if not query:
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": "No question provided in query"}
                )
                return

            # Store the original query message for later response
            self.pending_queries[message.conversation_id] = message

            # Route to appropriate handler
            await self._handle_user_text(query, message.conversation_id)

        @self.on("user_text")
        async def handle_user_text(message: Message) -> None:
            logger.info(f"ChatAgent received user_text: {message.content}")

            user_text = message.content.get("text", "")
            if not user_text:
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": "No text provided"}
                )
                return

            # Store the original message for later response
            self.pending_queries[message.conversation_id] = message

            # Process user text
            await self._handle_user_text(user_text, message.conversation_id)

        @self.on("script_results")
        async def handle_script_results(message: Message) -> None:
            scripts = message.content.get("scripts", [])
            if scripts:
                response = "Found these scripts for your query:\n"
                for script in scripts:
                    response += f"- {script['name']}: {script['doc']}\n"
            else:
                response = "No scripts found matching your query."

            # Reply to the original query sender
            original_message = self.pending_queries.get(message.conversation_id)
            if original_message:
                await self.reply(
                    original_message,
                    Performative.INFORM,
                    "response",
                    {"answer": response}
                )
                # Clean up the pending query
                del self.pending_queries[message.conversation_id]
            else:
                logger.warning(f"No pending query found for conversation {message.conversation_id}")

        @self.on("workflow_results")
        async def handle_workflow_results(message: Message) -> None:
            workflows = message.content.get("workflows", [])
            if workflows:
                response = "Found these workflows:\n"
                for workflow in workflows:
                    response += f"- {workflow['name']}: {workflow['description']}\n"
            else:
                response = "No workflows found matching your query."

            # Reply to the original query sender
            original_message = self.pending_queries.get(message.conversation_id)
            if original_message:
                await self.reply(
                    original_message,
                    Performative.INFORM,
                    "response",
                    {"answer": response}
                )
                # Clean up the pending query
                del self.pending_queries[message.conversation_id]
            else:
                logger.warning(f"No pending query found for conversation {message.conversation_id}")

        @self.on("response")
        async def handle_response(message: Message) -> None:
            """Handle responses from other agents (like translator)"""
            logger.info(f"ChatAgent received response: {message.content}")

            response = message.content.get("answer", "No response")

            # Reply to the original query sender
            original_message = self.pending_queries.get(message.conversation_id)
            if original_message:
                await self.reply(
                    original_message,
                    Performative.INFORM,
                    "response",
                    {"answer": response}
                )
                # Clean up the pending query
                del self.pending_queries[message.conversation_id]
            else:
                logger.warning(f"No pending query found for conversation {message.conversation_id}")

        @self.on("plan")
        async def handle_plan(message: Message) -> None:
            """Handle plan responses from QueryTranslatorAgent"""
            logger.info(f"ChatAgent received plan: {message.content}")

            # Forward the plan response directly with raw data
            original_message = self.pending_queries.get(message.conversation_id)
            if original_message:
                # Forward the plan message with the same performative and content
                await self.reply(
                    original_message,
                    message.performative,  # Keep FAILURE or INFORM
                    message.content_type,  # Keep "plan"
                    message.content  # Forward raw plan data
                )
                # Clean up the pending query
                del self.pending_queries[message.conversation_id]
            else:
                logger.warning(f"No pending query found for conversation {message.conversation_id}")

    async def _handle_user_text(self, user_text: str, conversation_id: str) -> None:
        """Process user text - either FAQ lookup or task extraction"""

        # First try FAQ lookup
        faq_answer = self._lookup_faq(user_text)
        if faq_answer:
            original_message = self.pending_queries.get(conversation_id)
            if original_message:
                await self.reply(
                    original_message,
                    Performative.INFORM,
                    "response",
                    {"answer": faq_answer}
                )
                del self.pending_queries[conversation_id]
            return

        # If no FAQ match, parse as task
        task = self._parse_task(user_text)
        logger.info(f"Parsed task: {task}")

        # Send task to translator
        await self.send(
            "translator",
            Performative.REQUEST,
            "task",
            task.to_dict(),
            conversation_id
        )

    def _lookup_faq(self, user_text: str) -> Optional[str]:
        """Look up FAQ answer using simple keyword matching"""
        user_lower = user_text.lower()

        # Check for exact FAQ question patterns first
        faq_indicators = ["what is", "how do", "what are", "how to", "what file", "what format"]
        is_faq_question = any(indicator in user_lower for indicator in faq_indicators)

        if not is_faq_question:
            return None  # Skip FAQ lookup for non-question patterns

        for faq_item in self.glossary.get("faq", []):
            question = faq_item["question"].lower()

            # Simple keyword-based matching
            question_words = set(re.findall(r'\w+', question))
            user_words = set(re.findall(r'\w+', user_lower))

            # Calculate similarity (basic word overlap)
            # Filter out common stop words for better matching
            stop_words = {"the", "a", "an", "are", "to", "of", "for", "with", "in", "on", "at"}
            question_content_words = question_words - stop_words
            user_content_words = user_words - stop_words

            common_words = question_content_words.intersection(user_content_words)
            # Need at least 1 meaningful word match for questions with specific terms
            if len(common_words) >= 1 and any(word in ["cea", "cooling", "demand", "file", "formats", "support", "network", "optimization", "calculate", "analyze"] for word in common_words):
                return faq_item["answer"]

        return None

    def _parse_task(self, user_text: str) -> Task:
        """Parse user text into a structured Task using rule-based extraction"""

        # Keyword mapping for intent detection
        intent_keywords = {
            "cooling demand": ["cooling", "demand", "cool", "estimate"],
            "network": ["network", "pipe", "distribution", "optimize"],
            "tech selection": ["technology", "system", "selection", "choose"],
            "kpis": ["kpi", "performance", "indicator", "metric"],
            "cost": ["cost", "economic", "financial", "price"],
            "ghg": ["emission", "carbon", "ghg", "co2", "greenhouse"]
        }

        # Detect intent
        intent = self._detect_intent(user_text, intent_keywords)

        # Detect scope
        scope = self._detect_scope(user_text)

        # Extract file inputs
        inputs = self._extract_file_inputs(user_text)

        # Extract constraints (simple implementation)
        constraints = self._extract_constraints(user_text)

        return Task(
            intent=intent,
            scope=scope,
            inputs=inputs,
            constraints=constraints,
            raw_text=user_text
        )

    def _detect_intent(self, text: str, intent_keywords: Dict[str, List[str]]) -> str:
        """Detect primary intent using keyword matching"""
        text_lower = text.lower()
        text_words = re.findall(r'\w+', text_lower)

        intent_scores = {}

        for intent, keywords in intent_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in text_lower:
                    score += 1
                # Bonus for exact word match
                if keyword in text_words:
                    score += 1
            intent_scores[intent] = score

        # Return intent with highest score, or default
        if intent_scores:
            best_intent = max(intent_scores, key=intent_scores.get)
            if intent_scores[best_intent] > 0:
                return best_intent

        return "general_analysis"  # Default intent

    def _detect_scope(self, text: str) -> Optional[str]:
        """Detect analysis scope from text"""
        text_lower = text.lower()

        # District-level indicators
        district_words = ["district", "neighbourhood", "neighborhood", "area", "zone", "region"]
        building_words = ["building", "house", "structure", "facility"]

        district_score = sum(1 for word in district_words if word in text_lower)
        building_score = sum(1 for word in building_words if word in text_lower)

        if district_score > building_score:
            return "district"
        elif building_score > 0:
            return "building"

        return None

    def _extract_file_inputs(self, text: str) -> Dict[str, str]:
        """Extract file references from text"""
        inputs = {}

        # File extension patterns
        file_patterns = {
            r'(\w+\.geojson)': 'geometry',
            r'(\w+\.epw)': 'weather',
            r'(\w+\.csv)': 'data',
            r'(\w+\.xlsx?)': 'schedule',
            r'(\w+\.json)': 'config'
        }

        for pattern, input_type in file_patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Use filename as key, or generic type if multiple files of same type
                key = input_type if input_type not in inputs else f"{input_type}_{len(inputs)}"
                inputs[key] = match

        # Extract other input hints
        if "schedule" in text.lower():
            inputs["schedules"] = "occupancy_schedules"
        if "weather" in text.lower() and "weather" not in inputs:
            inputs["weather"] = "weather_data"
        if "geometry" in text.lower() and "geometry" not in inputs:
            inputs["geometry"] = "building_geometry"

        return inputs

    def _extract_constraints(self, text: str) -> Dict[str, str]:
        """Extract constraints and requirements from text"""
        constraints = {}

        # Time-related constraints
        time_pattern = r'(hourly|daily|monthly|annual|yearly)'
        time_match = re.search(time_pattern, text, re.IGNORECASE)
        if time_match:
            constraints["timestep"] = time_match.group(1).lower()

        # Temperature constraints
        temp_pattern = r'(\d+(?:\.\d+)?)\s*(?:°C|celsius|degrees)'
        temp_match = re.search(temp_pattern, text, re.IGNORECASE)
        if temp_match:
            constraints["temperature"] = f"{temp_match.group(1)}°C"

        # Algorithm preferences
        if "genetic" in text.lower():
            constraints["algorithm"] = "genetic"
        elif "steiner" in text.lower():
            constraints["algorithm"] = "steiner"
        elif "mst" in text.lower():
            constraints["algorithm"] = "mst"

        return constraints