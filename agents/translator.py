from typing import Dict, List, Set
import re

from loguru import logger

from bus import Message, Performative, Router
from .base import BaseAgent
from .models import Plan, PlanStep, Task
from .capabilities import CapabilitiesProvider, DAOCapabilitiesProvider
from db import DAO


class QueryTranslatorAgent(BaseAgent):
    def __init__(self, router: Router, dao: DAO = None, capabilities_provider: CapabilitiesProvider = None) -> None:
        super().__init__("translator", router)

        # Support both legacy DAO and new CapabilitiesProvider interfaces
        if capabilities_provider is not None:
            self.capabilities = capabilities_provider
        elif dao is not None:
            self.capabilities = DAOCapabilitiesProvider(dao)
        else:
            raise ValueError("Either dao or capabilities_provider must be provided")

        # Keep dao for backwards compatibility
        self.dao = dao
        self.setup_handlers()

    def setup_handlers(self) -> None:
        @self.on("translate")
        async def handle_translate(message: Message) -> None:
            logger.info(f"TranslatorAgent received translation request: {message.content}")

            text = message.content.get("text", "")
            target_language = message.content.get("target_language", "en")

            if not text:
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": "No text provided for translation"}
                )
                return

            translated_text = self._translate(text, target_language)

            await self.reply(
                message,
                Performative.INFORM,
                "translation_result",
                {
                    "original_text": text,
                    "translated_text": translated_text,
                    "target_language": target_language
                }
            )

        @self.on("language_detect")
        async def handle_language_detect(message: Message) -> None:
            logger.info(f"TranslatorAgent received language detection request: {message.content}")

            text = message.content.get("text", "")

            if not text:
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": "No text provided for language detection"}
                )
                return

            detected_language = self._detect_language(text)

            await self.reply(
                message,
                Performative.INFORM,
                "language_detection_result",
                {
                    "text": text,
                    "detected_language": detected_language,
                    "confidence": 0.95  # Mock confidence score
                }
            )

        @self.on("task")
        async def handle_task(message: Message) -> None:
            logger.info(f"QueryTranslatorAgent received task: {message.content}")

            try:
                # Parse task from message content
                task = Task.from_dict(message.content)

                # Find best matching workflow
                best_workflow = await self._find_best_workflow(task)

                if not best_workflow:
                    await self.reply(
                        message,
                        Performative.FAILURE,
                        "error",
                        {"error": f"No workflow found for intent: {task.intent}"}
                    )
                    return

                # Compute plan and validate inputs
                plan = await self._compute_plan(task, best_workflow)

                if plan.missing:
                    # Reply with FAILURE if inputs are missing
                    await self.reply(
                        message,
                        Performative.FAILURE,
                        "plan",
                        {
                            "reason": f"Missing required inputs: {', '.join(plan.missing)}",
                            "missing": plan.missing,
                            "plan": plan.to_dict()
                        }
                    )
                else:
                    # Reply with INFORM if plan is complete
                    await self.reply(
                        message,
                        Performative.INFORM,
                        "plan",
                        {
                            "plan": plan.to_dict(),
                            "workflow_id": best_workflow.id,
                            "workflow_name": best_workflow.name
                        }
                    )

            except Exception as e:
                logger.error(f"Error processing task: {e}")
                await self.reply(
                    message,
                    Performative.FAILURE,
                    "error",
                    {"error": f"Failed to process task: {str(e)}"}
                )

    async def _find_best_workflow(self, task: Task) -> object:
        """Find the best matching workflow by tag overlap"""

        # Get candidate workflows using tag-based search
        candidate_workflows = await self._get_candidate_workflows(task)

        if not candidate_workflows:
            return None

        # Score workflows by tag overlap
        best_workflow = None
        best_score = 0

        for workflow in candidate_workflows:
            score = self._calculate_workflow_score(task, workflow)
            logger.debug(f"Workflow {workflow.name} scored {score}")

            if score > best_score:
                best_score = score
                best_workflow = workflow

        logger.info(f"Selected workflow: {best_workflow.name if best_workflow else 'None'} with score {best_score}")
        return best_workflow

    async def _get_candidate_workflows(self, task: Task) -> List[object]:
        """Get candidate workflows by tag overlap with task intent and inferred tags"""

        # Extract tags from task intent and inputs
        task_tags = self._extract_task_tags(task)
        logger.debug(f"Task tags: {task_tags}")

        # Get all workflows from capabilities provider
        all_workflows = await self.capabilities.get_all_workflows()

        # Filter workflows that have tag overlap
        candidate_workflows = []
        for workflow in all_workflows:
            workflow_tags = set(workflow.tags)
            overlap = task_tags.intersection(workflow_tags)

            if overlap:
                candidate_workflows.append(workflow)
                logger.debug(f"Workflow {workflow.name} has overlap: {overlap}")

        return candidate_workflows

    def _extract_task_tags(self, task: Task) -> Set[str]:
        """Extract tags from task intent and inferred from inputs/scope"""
        tags = set()

        # Tags from intent (split on spaces and underscores)
        intent_words = re.findall(r'\w+', task.intent.lower())
        tags.update(intent_words)

        # Tags from scope
        if task.scope:
            tags.add(task.scope)

        # Tags from input types
        for input_type in task.inputs.keys():
            if input_type in ["geometry", "weather", "schedule", "data", "config"]:
                tags.add(input_type)

        # Inferred tags from file extensions
        for input_file in task.inputs.values():
            if input_file.endswith('.geojson'):
                tags.add('geometry')
            elif input_file.endswith('.epw'):
                tags.add('weather')
            elif input_file.endswith('.csv'):
                tags.add('data')
            elif input_file.endswith('.xlsx'):
                tags.add('schedule')

        # Inferred tags from constraints
        for constraint_type in task.constraints.keys():
            if constraint_type in ["algorithm", "timestep", "temperature"]:
                tags.add(constraint_type)

        return tags

    def _calculate_workflow_score(self, task: Task, workflow: object) -> float:
        """Calculate matching score between task and workflow"""
        task_tags = self._extract_task_tags(task)
        workflow_tags = set(workflow.tags)

        # Calculate tag overlap
        overlap = task_tags.intersection(workflow_tags)
        overlap_score = len(overlap)

        # Bonus for exact intent match
        intent_bonus = 0
        if task.intent in workflow.name or any(word in workflow.name for word in task.intent.split()):
            intent_bonus = 5

        # Bonus for scope match
        scope_bonus = 0
        if task.scope and task.scope in workflow.tags:
            scope_bonus = 2

        total_score = overlap_score + intent_bonus + scope_bonus
        return total_score

    async def _compute_plan(self, task: Task, workflow: object) -> Plan:
        """Compute execution plan and validate inputs"""

        # Get all scripts referenced by the workflow
        script_ids = [step.script_id for step in workflow.steps]
        scripts = {}
        for script_id in script_ids:
            script = await self.capabilities.get_script_by_id(script_id)
            if script:
                scripts[script_id] = script

        # Compute required inputs by union of all script inputs
        required_inputs = {}
        for script in scripts.values():
            for inp in script.inputs:
                if inp.required:
                    required_inputs[inp.name] = inp.type

        # Map task inputs to required inputs
        available_inputs = set()
        missing_inputs = []

        # Check for required weather file
        if "weather_file" in required_inputs or any("weather" in req.lower() for req in required_inputs.keys()):
            if any(inp.endswith('.epw') for inp in task.inputs.values()) or "weather" in task.inputs:
                available_inputs.add("weather_epw")
            else:
                missing_inputs.append("weather_epw")

        # Check for required geometry
        if "buildings" in required_inputs or any("geometry" in req.lower() for req in required_inputs.keys()):
            if any(inp.endswith('.geojson') for inp in task.inputs.values()) or "geometry" in task.inputs:
                available_inputs.add("geometry")
            else:
                missing_inputs.append("geometry")

        # Check for scenario config (often required)
        if "scenario_config" in required_inputs:
            # Assume we can generate this if we have basic inputs
            if available_inputs:
                available_inputs.add("scenario_config")
            else:
                missing_inputs.append("scenario_config")

        # Create plan steps
        plan_steps = []
        for step in workflow.steps:
            script = scripts.get(step.script_id)
            if script:
                args = self._map_task_inputs_to_script_args(task, script)
                plan_step = PlanStep(
                    script_id=step.script_id,
                    args=args
                )
                plan_steps.append(plan_step)

        # Generate explanation
        explanation = self._generate_explanation(task, workflow, available_inputs, missing_inputs)

        # Generate assumptions
        assumptions = self._generate_assumptions(task, workflow)

        plan = Plan(
            plan=plan_steps,
            explain=explanation,
            assumptions=assumptions,
            missing=missing_inputs
        )

        return plan

    def _map_task_inputs_to_script_args(self, task: Task, script: object) -> Dict[str, str]:
        """Map task inputs to script arguments"""
        args = {}

        # Map file inputs
        for task_input_key, task_input_value in task.inputs.items():
            if task_input_key == "geometry" or task_input_value.endswith('.geojson'):
                args["buildings"] = task_input_value
            elif task_input_key == "weather" or task_input_value.endswith('.epw'):
                args["weather_file"] = task_input_value
            elif task_input_key == "schedule" or task_input_value.endswith('.xlsx'):
                args["occupancy_schedules"] = task_input_value
            elif task_input_key == "data" or task_input_value.endswith('.csv'):
                args["energy_demands"] = task_input_value

        # Map constraints to parameters
        for constraint_key, constraint_value in task.constraints.items():
            if constraint_key == "algorithm":
                args["algorithm"] = constraint_value
            elif constraint_key == "timestep":
                args["timestep"] = constraint_value

        # Add default scenario config if needed
        if any(inp.name == "scenario_config" for inp in script.inputs):
            args["scenario_config"] = "scenario.yml"

        return args

    def _generate_explanation(self, task: Task, workflow: object, available_inputs: Set[str], missing_inputs: List[str]) -> str:
        """Generate explanation of why this workflow fits the task"""

        explanation = f"Selected workflow '{workflow.name}' because it matches your intent to {task.intent}."

        if task.scope:
            explanation += f" This workflow is suitable for {task.scope}-level analysis."

        explanation += f" The workflow consists of {len(workflow.steps)} steps: "
        step_descriptions = [step.description for step in workflow.steps]
        explanation += ", ".join(step_descriptions) + "."

        if available_inputs:
            explanation += f" Available inputs: {', '.join(available_inputs)}."

        if missing_inputs:
            explanation += f" Missing required inputs: {', '.join(missing_inputs)}."

        return explanation

    def _generate_assumptions(self, task: Task, workflow: object) -> List[str]:
        """Generate assumptions made during planning"""
        assumptions = []

        # Standard assumptions
        assumptions.append("Using default building schedules if not provided")
        assumptions.append("Standard thermal model parameters unless specified")

        if task.scope == "district":
            assumptions.append("All buildings in district have similar characteristics")
        elif task.scope == "building":
            assumptions.append("Single building analysis with typical occupancy patterns")

        if "cooling" in task.intent:
            assumptions.append("Focusing on cooling loads and systems")

        if any("optimization" in step.action for step in workflow.steps):
            assumptions.append("Using default optimization objectives unless specified")

        return assumptions

    def _translate(self, text: str, target_language: str) -> str:
        """Mock translation function - in real implementation, this would use a translation service"""
        translation_map = {
            ("hello", "es"): "hola",
            ("cooling demand", "es"): "demanda de refrigeración",
            ("energy analysis", "es"): "análisis energético",
            ("building", "es"): "edificio",
            ("simulation", "es"): "simulación",
        }

        text_lower = text.lower()
        for (original, lang), translation in translation_map.items():
            if original in text_lower and lang == target_language:
                return text.replace(original, translation)

        return f"[{target_language.upper()}] {text}"

    def _detect_language(self, text: str) -> str:
        """Mock language detection - in real implementation, this would use a language detection service"""
        spanish_keywords = ["hola", "energía", "edificio", "análisis", "simulación"]
        french_keywords = ["bonjour", "énergie", "bâtiment", "analyse", "simulation"]
        german_keywords = ["hallo", "energie", "gebäude", "analyse", "simulation"]

        text_lower = text.lower()

        if any(keyword in text_lower for keyword in spanish_keywords):
            return "es"
        elif any(keyword in text_lower for keyword in french_keywords):
            return "fr"
        elif any(keyword in text_lower for keyword in german_keywords):
            return "de"
        else:
            return "en"