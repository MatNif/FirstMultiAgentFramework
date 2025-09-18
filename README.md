# CEA Assistant

A minimal multi-agent system for a CEA (City Energy Analyst) helper bot.

## Architecture

- **Agents**: Base agent class and specialized agents (chat, translator, database manager)
- **Message Bus**: FIPA-like messaging system with asyncio-based routing
- **Database**: SQLite persistence layer with scripts and workflows
- **CLI**: Command-line interface for user interaction

## Try it in 60 Seconds

Get started immediately with example data:

```bash
# 1. Install dependencies (if not already done)
pip install -r requirements.txt

# 2. Try the cost-optimal cooling system example
python -m cli.run --refresh "I want a cost-optimal cooling system for this district using zone.geojson and zurich.epw"

# 3. Ask about CEA basics
python -m cli.run "what is CEA?"

# 4. Get help with file formats
python -m cli.run "what file formats does CEA support?"
```

### More Examples

```bash
# District cooling demand analysis
python -m cli.run "estimate district cooling demand from zone.geojson and zurich.epw"

# Network optimization with algorithm preference
python -m cli.run "optimize distribution network using genetic algorithm"

# Building-specific analysis
python -m cli.run "analyze energy performance for office building"

# Get JSON output for programmatic use
python -m cli.run --json "calculate emissions for district cooling"
```

The assistant will return:
- **Execution Plans**: Step-by-step workflows with CEA scripts
- **FAQ Responses**: Direct answers from the knowledge base
- **Missing Input Alerts**: What files/data you need to provide

## Quick Start

1. Install dependencies:
   ```bash
   poetry install
   ```

2. Run the assistant:
   ```bash
   poetry run cea-assistant "what scripts estimate cooling demand?"
   ```

## Development

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=. --cov-report=html

# Run specific test
poetry run pytest tests/test_contracts.py -v
```

### Code Quality

```bash
# Format code
poetry run black .

# Lint code
poetry run ruff check .

# Type checking
poetry run mypy .

# Install pre-commit hooks
poetry run pre-commit install
```

## Example Commands

```bash
# Query available scripts
poetry run cea-assistant "what scripts estimate cooling demand?"

# Ask about workflows
poetry run cea-assistant "show me workflows for building energy analysis"

# General questions
poetry run cea-assistant "how do I analyze district energy systems?"
```

## MCP Integration

The CEA Assistant is designed with a pluggable architecture that supports both local SQLite storage and remote MCP (Model Context Protocol) servers for accessing CEA capabilities.

### Current Architecture

The system currently uses:
- **Local SQLite Database**: Scripts and workflows stored in `cea_assistant.db`
- **DAO Pattern**: Direct database access via `db.DAO` class
- **Capabilities Provider Interface**: Abstraction layer for easy MCP migration

### Future MCP Integration

#### Phase 1: MCP Server Setup (Future)

1. **CEA Runner MCP Server**
   ```bash
   # Future: Run CEA capabilities as MCP server
   cea-mcp-server --port 8000 --cea-root ./cea_scripts
   ```

2. **Agent Configuration**
   ```python
   # Future: Configure agents to use MCP instead of DAO
   from mcp import MCPClient
   from agents import QueryTranslatorAgent, MCPCapabilitiesProvider

   mcp_client = MCPClient("http://localhost:8000")
   capabilities = MCPCapabilitiesProvider(mcp_client)
   translator = QueryTranslatorAgent(router, capabilities_provider=capabilities)
   ```

#### Phase 2: MCP Functions Available

The MCP layer exposes these callable functions:

**Script Discovery & Help**
```python
# List available CEA scripts
scripts = await mcp_server.list_scripts(category="demand", tags=["cooling"])

# Get detailed script help
help_info = await mcp_server.script_help("cea-demand-calculation")
```

**Script Execution**
```python
# Execute CEA scripts remotely
result = await mcp_server.run_script(
    "cea-demand-calculation",
    {
        "scenario": "baseline",
        "weather_file": "weather.epw",
        "buildings": "zone.shp"
    },
    timeout=600
)
```

#### Phase 3: Migration Path

To switch from DAO to MCP:

1. **Update QueryTranslatorAgent**:
   ```python
   # Current (DAO-based)
   translator = QueryTranslatorAgent(router, dao)

   # Future (MCP-based)
   mcp_capabilities = MCPCapabilitiesProvider(mcp_client)
   translator = QueryTranslatorAgent(router, capabilities_provider=mcp_capabilities)
   ```

2. **Benefits**:
   - **Remote Execution**: CEA scripts run on dedicated compute servers
   - **Scalability**: Multiple agents can share the same CEA server
   - **Isolation**: CEA environment separated from agent runtime
   - **Versioning**: Centralized CEA version management

#### Current MCP Stubs

The codebase includes MCP integration stubs:

- **`/mcp/cea_runner_server.py`**: MCP server interface for CEA operations
- **`/agents/capabilities.py`**: Provider interface with DAO and MCP implementations
- **`/agents/translator.py`**: Updated to use CapabilitiesProvider interface

### MCP Integration Example

```python
# Future usage pattern
from mcp import CEARunnerServer, MCPClient
from agents import QueryTranslatorAgent, MCPCapabilitiesProvider

# Setup MCP server (future)
cea_server = CEARunnerServer()
await cea_server.initialize()

# Setup MCP client (future)
mcp_client = MCPClient("http://cea-server:8000")
await mcp_client.connect()

# Create capabilities provider
mcp_capabilities = MCPCapabilitiesProvider(mcp_client)

# Use with agents
translator = QueryTranslatorAgent(router, capabilities_provider=mcp_capabilities)
```

## Project Structure

```
├── agents/              # Agent implementations
│   ├── base.py          # Base agent class
│   ├── chat.py          # Chat agent with task parsing
│   ├── translator.py    # Query translator with workflow mapping
│   ├── capabilities.py  # CapabilitiesProvider interface (DAO/MCP)
│   └── dbm.py           # Database manager agent
├── bus/                 # Message bus system
│   ├── messages.py      # Message definitions
│   └── router.py        # Message routing
├── db/                  # Database layer
│   ├── schema.sql       # Database schema
│   ├── dao.py           # Data access objects
│   ├── models.py        # Pydantic models
│   └── seed.py          # Database seeding
├── mcp/                 # MCP integration layer
│   └── cea_runner_server.py # MCP server for CEA operations
├── cli/                 # Command-line interface
│   └── run.py           # Main entry point with Rich formatting
├── config.py            # Configuration management with pydantic-settings
├── scripts/             # Development scripts
│   └── ci.py            # Local CI script
└── tests/               # Test suite
    ├── test_message_schema.py     # Message bus tests
    ├── test_chat_intents.py       # Chat agent intent tests
    ├── test_translator_mapping.py # Translator workflow tests
    └── test_*.py                  # Other test files
```