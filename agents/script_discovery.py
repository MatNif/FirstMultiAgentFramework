import asyncio
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from db.models import Script, ScriptInput, ScriptOutput


class ScriptDiscovery:
    """Utility class for discovering and analyzing Python scripts"""

    # Common stopwords to filter from tags
    STOPWORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
        'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
        'to', 'was', 'will', 'with', 'can', 'this', 'or', 'but', 'not',
        'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other',
        'some', 'such', 'only', 'own', 'same', 'so', 'than', 'too', 'very'
    }

    def __init__(self, cea_root: str, timeout: float = 10.0):
        self.cea_root = Path(cea_root)
        self.timeout = timeout

    async def discover_scripts(self) -> List[Script]:
        """Discover all Python scripts in CEA_ROOT and extract metadata"""
        if not self.cea_root.exists():
            logger.warning(f"CEA_ROOT path does not exist: {self.cea_root}")
            return []

        python_files = list(self.cea_root.rglob("*.py"))
        logger.info(f"Found {len(python_files)} Python files in {self.cea_root}")

        scripts = []
        discovered = 0
        updated = 0
        skipped = 0

        for py_file in python_files:
            try:
                script = await self._analyze_script(py_file)
                if script:
                    scripts.append(script)
                    discovered += 1
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"Failed to analyze {py_file}: {e}")
                skipped += 1

        logger.info(f"Script discovery completed: {discovered} discovered, {updated} updated, {skipped} skipped")
        return scripts

    async def _analyze_script(self, script_path: Path) -> Optional[Script]:
        """Analyze a single Python script to extract metadata"""
        try:
            # Get help output
            help_output = await self._get_help_output(script_path)
            if not help_output:
                return None

            # Extract metadata
            name = self._extract_script_name(script_path)
            cli_command = self._extract_cli_command(help_output, script_path)
            doc = self._extract_documentation(help_output)
            tags = self._extract_tags(script_path, help_output, doc)
            inputs, outputs = self._extract_inputs_outputs(help_output)

            # Create relative path from CEA_ROOT
            relative_path = script_path.relative_to(self.cea_root)

            script = Script(
                name=name,
                path=str(relative_path),
                cli=cli_command,
                doc=doc,
                inputs=inputs,
                outputs=outputs,
                tags=tags
            )

            logger.debug(f"Analyzed script: {name} with {len(inputs)} inputs, {len(outputs)} outputs, {len(tags)} tags")
            return script

        except Exception as e:
            logger.error(f"Error analyzing script {script_path}: {e}")
            return None

    async def _get_help_output(self, script_path: Path) -> Optional[str]:
        """Run script with --help and capture output"""
        try:
            # Try different help options
            help_flags = ["--help", "-h"]

            for flag in help_flags:
                try:
                    # Use absolute path to ensure proper execution
                    script_abs_path = script_path if script_path.is_absolute() else script_path.resolve()

                    process = await asyncio.create_subprocess_exec(
                        "python", str(script_abs_path), flag,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(),
                        timeout=self.timeout
                    )

                    if process.returncode == 0 and stdout:
                        return stdout.decode('utf-8', errors='ignore')

                    # Also try stderr in case help goes there
                    if stderr:
                        stderr_text = stderr.decode('utf-8', errors='ignore')
                        if 'usage:' in stderr_text.lower():
                            return stderr_text

                except asyncio.TimeoutError:
                    logger.warning(f"Timeout running {script_path} {flag}")
                    try:
                        process.terminate()
                        await process.wait()
                    except:
                        pass
                except Exception as e:
                    logger.debug(f"Failed to run {script_path} {flag}: {e}")
                    continue

            return None

        except Exception as e:
            logger.warning(f"Could not get help for {script_path}: {e}")
            return None

    def _extract_script_name(self, script_path: Path) -> str:
        """Extract script name from path"""
        # Use stem (filename without extension) and clean it up
        name = script_path.stem

        # Convert underscores to spaces and title case
        name = name.replace('_', ' ').replace('-', ' ')

        # Remove common prefixes/suffixes
        prefixes = ['cea', 'script', 'tool', 'util']
        suffixes = ['main', 'cli', 'tool']

        words = name.lower().split()
        words = [w for w in words if w not in prefixes and w not in suffixes]

        return '_'.join(words) if words else script_path.stem

    def _extract_cli_command(self, help_output: str, script_path: Path) -> str:
        """Extract CLI command from help output"""
        lines = help_output.split('\n')

        # Look for usage line
        for line in lines:
            line = line.strip()
            if line.lower().startswith('usage:'):
                # Extract the command part after 'usage:'
                usage = line[6:].strip()
                if usage:
                    return usage

        # Fallback: generate basic command
        script_name = script_path.stem
        return f"python {script_path.name} [options]"

    def _extract_documentation(self, help_output: str) -> str:
        """Extract documentation from help output"""
        lines = help_output.split('\n')

        # Look for description after usage
        doc_lines = []
        in_description = False

        for line in lines:
            line = line.strip()

            # Skip empty lines at start
            if not line and not doc_lines:
                continue

            # Stop at options/arguments section
            if line.lower().startswith(('options:', 'arguments:', 'positional arguments:', 'optional arguments:')):
                break

            # Skip usage line
            if line.lower().startswith('usage:'):
                in_description = True
                continue

            if in_description and line:
                doc_lines.append(line)
            elif doc_lines:  # Stop at first empty line after content
                break

        doc = ' '.join(doc_lines).strip()

        # Limit length
        if len(doc) > 500:
            doc = doc[:500] + "..."

        return doc if doc else "No description available"

    def _extract_tags(self, script_path: Path, help_output: str, doc: str) -> List[str]:
        """Extract tags from filename, path, and documentation"""
        tags = set()

        # Tags from filename
        filename_words = re.findall(r'\w+', script_path.stem.lower())
        tags.update(w for w in filename_words if len(w) > 2 and w not in self.STOPWORDS)

        # Tags from path components
        path_parts = [part.lower() for part in script_path.parts[:-1]]  # Exclude filename
        for part in path_parts:
            words = re.findall(r'\w+', part)
            tags.update(w for w in words if len(w) > 2 and w not in self.STOPWORDS)

        # Tags from first sentence of documentation
        if doc:
            first_sentence = doc.split('.')[0].lower()
            words = re.findall(r'\w+', first_sentence)
            # Filter and add meaningful words
            meaningful_words = [w for w in words if len(w) > 3 and w not in self.STOPWORDS]
            tags.update(meaningful_words[:5])  # Limit to first 5 meaningful words

        # Add some domain-specific keywords if found
        cea_keywords = {
            'energy', 'thermal', 'cooling', 'heating', 'demand', 'supply',
            'network', 'optimization', 'simulation', 'analysis', 'building',
            'solar', 'renewable', 'cost', 'emissions', 'report', 'validation'
        }

        text_to_search = f"{script_path.name} {help_output} {doc}".lower()
        for keyword in cea_keywords:
            if keyword in text_to_search:
                tags.add(keyword)

        # Convert to sorted list
        return sorted(list(tags))

    def _extract_inputs_outputs(self, help_output: str) -> Tuple[List[ScriptInput], List[ScriptOutput]]:
        """Extract input and output parameters from help output"""
        inputs = []
        outputs = []

        lines = help_output.split('\n')
        current_section = None

        for line in lines:
            line = line.strip()

            # Detect sections
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in ['options:', 'arguments:', 'positional arguments:', 'optional arguments:']):
                current_section = 'options'
                continue
            elif line_lower.startswith('usage:'):
                current_section = 'usage'
                continue

            # Parse options/arguments
            if current_section == 'options' and line:
                input_param = self._parse_option_line(line)
                if input_param:
                    inputs.append(input_param)

        # Infer outputs from common patterns
        help_text = help_output.lower()
        output_patterns = [
            ('output', 'file', 'Generated output file'),
            ('result', 'csv', 'Analysis results in CSV format'),
            ('report', 'pdf', 'Generated report file'),
            ('data', 'json', 'Output data in JSON format'),
            ('log', 'txt', 'Execution log file')
        ]

        for name, file_type, description in output_patterns:
            if name in help_text:
                outputs.append(ScriptOutput(
                    name=name,
                    type=file_type,
                    description=description
                ))

        # If no outputs inferred, add a generic one
        if not outputs:
            outputs.append(ScriptOutput(
                name="output",
                type="file",
                description="Script output file"
            ))

        return inputs, outputs

    def _parse_option_line(self, line: str) -> Optional[ScriptInput]:
        """Parse a single option line from help output"""
        # Match patterns like:
        # -f, --file FILE          Input file path
        # --input INPUT            Input data file
        # -o, --output OUTPUT      Output directory

        # Look for option patterns
        option_match = re.match(r'\s*(-\w,?\s*)?--(\w+)(\s+\w+)?\s+(.*)', line)
        if not option_match:
            # Try simpler pattern
            option_match = re.match(r'\s*--(\w+)\s+(.*)', line)
            if not option_match:
                return None

        if len(option_match.groups()) >= 2:
            if len(option_match.groups()) == 4:
                _, param_name, _, description = option_match.groups()
            else:
                param_name, description = option_match.groups()

            # Skip output-related parameters
            if any(keyword in param_name.lower() for keyword in ['output', 'out', 'result']):
                return None

            # Determine if required (heuristic)
            required = 'required' in description.lower() or 'must' in description.lower()

            # Infer type from parameter name and description
            param_type = self._infer_parameter_type(param_name, description)

            return ScriptInput(
                name=param_name,
                type=param_type,
                description=description.strip(),
                required=required
            )

        return None

    def _infer_parameter_type(self, param_name: str, description: str) -> str:
        """Infer parameter type from name and description"""
        name_lower = param_name.lower()
        desc_lower = description.lower()

        # Type mapping based on common patterns
        if any(keyword in name_lower for keyword in ['file', 'path', 'input']):
            if 'csv' in desc_lower:
                return 'csv'
            elif 'json' in desc_lower:
                return 'json'
            elif 'yaml' in desc_lower or 'yml' in desc_lower:
                return 'yaml'
            elif 'excel' in desc_lower or 'xlsx' in desc_lower:
                return 'excel'
            else:
                return 'file'
        elif any(keyword in name_lower for keyword in ['dir', 'directory']):
            return 'directory'
        elif any(keyword in name_lower for keyword in ['config', 'settings']):
            return 'config'
        elif any(keyword in desc_lower for keyword in ['number', 'count', 'integer']):
            return 'integer'
        elif any(keyword in desc_lower for keyword in ['float', 'decimal']):
            return 'float'
        elif any(keyword in desc_lower for keyword in ['bool', 'flag', 'enable', 'disable']):
            return 'boolean'
        else:
            return 'string'