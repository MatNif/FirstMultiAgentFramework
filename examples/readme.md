# CEA Assistant Examples

This directory contains example files and questions to help you get started with the CEA Assistant.

## Example Files

- **`zone.geojson`** - Sample building geometry with 3 buildings (office, residential, retail)
- **`zurich.epw`** - Placeholder weather file for Zurich, Switzerland
- **`readme.md`** - This file with example questions

## Quick Start

Try these example questions with the CEA Assistant. Copy and paste them into your terminal:

### 1. Cost-Optimal Cooling System Design
```bash
python -m cli.run "I want a cost-optimal cooling system for this district using zone.geojson and zurich.epw"
```

### 2. District Cooling Demand Analysis
```bash
python -m cli.run "estimate district cooling demand from zone.geojson and zurich.epw"
```

### 3. Network Optimization
```bash
python -m cli.run "optimize distribution network for buildings in zone.geojson using genetic algorithm"
```

### 4. Building Performance Analysis
```bash
python -m cli.run "analyze energy performance for the office building in zone.geojson"
```

### 5. General CEA Information
```bash
python -m cli.run "what is CEA and how do I get started?"
```

## Expected Results

Each command will return either:
- **Execution Plan**: Step-by-step workflow with CEA scripts to run
- **FAQ Response**: Direct answer from the knowledge base
- **Workflow Mapping**: Detailed plan with required inputs and assumptions

## File Details

### zone.geojson
Contains 3 sample buildings in Zurich:
- **B001**: 4-story office building (1,200 m²)
- **B002**: 3-story residential building (800 m²)
- **B003**: 5-story retail center (1,500 m²)

Each building includes:
- Geographic coordinates (Zurich city center)
- Building properties (height, floors, year built)
- Occupancy type and floor area
- Descriptive metadata

### zurich.epw
Placeholder weather file with:
- Location coordinates for Zurich
- Instructions for downloading real EPW data
- Sample weather data format
- Links to EnergyPlus weather database

## Advanced Usage

For more complex scenarios:

```bash
# Add refresh flag to update script catalog
python -m cli.run --refresh "design heating system for zone.geojson"

# Get JSON output for programmatic use
python -m cli.run --json "calculate emissions for district cooling"

# Combine multiple requirements
python -m cli.run "optimize cost and emissions for cooling system using zone.geojson and zurich.epw with genetic algorithm"
```

## Troubleshooting

If you get errors:
1. Make sure you're in the project root directory
2. Check that example files exist in `./examples/`
3. Use `--refresh` flag to update the script database
4. Try simpler questions first (like "what is CEA?")

## Next Steps

1. Download real weather data from [EnergyPlus Weather](https://energyplus.net/weather)
2. Create your own building geometry files
3. Explore the full CEA toolkit and documentation
4. Run the generated workflows with actual CEA installation