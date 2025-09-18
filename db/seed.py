import asyncio

from loguru import logger

from .dao import DAO
from .models import Script, ScriptInput, ScriptOutput, Workflow, WorkflowStep


async def seed_database(dao: DAO) -> None:
    """Seed the database with example CEA scripts and workflows"""
    logger.info("Starting database seeding...")

    # Define realistic CEA scripts
    scripts = [
        # 1. Demand calculation script
        Script(
            id="demand-calc-001",
            name="demand_calculation",
            path="/cea/scripts/demand/thermal_loads.py",
            cli="cea demand --scenario scenario.yml --weather weather.epw",
            doc="Calculate heating and cooling demand for buildings based on occupancy schedules, building properties, and weather data",
            inputs=[
                ScriptInput(
                    name="scenario_config",
                    type="yaml",
                    description="Scenario configuration file with paths to building data",
                    required=True
                ),
                ScriptInput(
                    name="weather_file",
                    type="epw",
                    description="EnergyPlus weather file for the location",
                    required=True
                ),
                ScriptInput(
                    name="buildings",
                    type="shapefile",
                    description="Building geometries and properties shapefile",
                    required=True
                ),
                ScriptInput(
                    name="occupancy_schedules",
                    type="excel",
                    description="Hourly occupancy schedules for different building types",
                    required=False,
                    default="standard_schedules.xlsx"
                )
            ],
            outputs=[
                ScriptOutput(
                    name="thermal_loads",
                    type="csv",
                    description="Hourly heating and cooling loads per building",
                    format="building_id,timestamp,heating_load_W,cooling_load_W"
                ),
                ScriptOutput(
                    name="energy_summary",
                    type="json",
                    description="Annual energy summary statistics",
                    format="json"
                )
            ],
            tags=["demand", "thermal", "heating", "cooling", "energy", "simulation"]
        ),

        # 2. Network analysis script
        Script(
            id="network-analysis-001",
            name="network_layout",
            path="/cea/scripts/network/layout_optimization.py",
            cli="cea network --buildings buildings.shp --streets streets.shp --algorithm steiner",
            doc="Optimize thermal network layout connecting buildings using minimum spanning tree algorithms",
            inputs=[
                ScriptInput(
                    name="buildings",
                    type="shapefile",
                    description="Building locations and connection points",
                    required=True
                ),
                ScriptInput(
                    name="streets",
                    type="shapefile",
                    description="Street network for routing constraints",
                    required=True
                ),
                ScriptInput(
                    name="algorithm",
                    type="string",
                    description="Optimization algorithm: steiner, mst, or genetic",
                    required=False,
                    default="steiner"
                ),
                ScriptInput(
                    name="pipe_costs",
                    type="csv",
                    description="Cost per meter for different pipe diameters",
                    required=False
                )
            ],
            outputs=[
                ScriptOutput(
                    name="network_layout",
                    type="shapefile",
                    description="Optimized network layout with pipe routes",
                    format="LineString geometries with diameter attributes"
                ),
                ScriptOutput(
                    name="connection_costs",
                    type="csv",
                    description="Capital costs for network connections",
                    format="building_id,connection_length_m,capital_cost_USD"
                )
            ],
            tags=["network", "thermal", "optimization", "layout", "pipes", "infrastructure"]
        ),

        # 3. Supply system optimization script
        Script(
            id="supply-optimization-001",
            name="supply_system_optimization",
            path="/cea/scripts/optimization/supply_system.py",
            cli="cea optimize-supply --buildings buildings.csv --technologies technologies.yml --objectives cost,emissions",
            doc="Multi-objective optimization of energy supply systems including renewables, storage, and conventional technologies",
            inputs=[
                ScriptInput(
                    name="energy_demands",
                    type="csv",
                    description="Hourly energy demands from demand calculation",
                    required=True
                ),
                ScriptInput(
                    name="technology_database",
                    type="yaml",
                    description="Available technologies with costs and performance data",
                    required=True
                ),
                ScriptInput(
                    name="objectives",
                    type="list",
                    description="Optimization objectives: cost, emissions, renewable_share",
                    required=False,
                    default=["cost", "emissions"]
                ),
                ScriptInput(
                    name="solar_potential",
                    type="csv",
                    description="Solar PV and thermal potential per building",
                    required=False
                )
            ],
            outputs=[
                ScriptOutput(
                    name="pareto_solutions",
                    type="csv",
                    description="Pareto-optimal supply system configurations",
                    format="solution_id,cost_USD,emissions_tCO2,renewable_share"
                ),
                ScriptOutput(
                    name="technology_sizing",
                    type="json",
                    description="Optimal sizing of technologies for each solution",
                    format="nested json with technology capacities"
                ),
                ScriptOutput(
                    name="operation_schedule",
                    type="csv",
                    description="Hourly operation schedule for optimal solution",
                    format="timestamp,technology,power_output_kW"
                )
            ],
            tags=["supply", "optimization", "renewable", "storage", "cost", "emissions", "pareto"]
        ),

        # 4. Reporting script
        Script(
            id="report-generation-001",
            name="energy_report_generator",
            path="/cea/scripts/reporting/energy_analysis_report.py",
            cli="cea report --results results/ --template template.html --format pdf",
            doc="Generate comprehensive energy analysis reports with visualizations and key performance indicators",
            inputs=[
                ScriptInput(
                    name="results_directory",
                    type="directory",
                    description="Directory containing analysis results (demand, supply, network)",
                    required=True
                ),
                ScriptInput(
                    name="report_template",
                    type="html",
                    description="HTML template for report generation",
                    required=False,
                    default="default_template.html"
                ),
                ScriptInput(
                    name="output_format",
                    type="string",
                    description="Output format: pdf, html, or docx",
                    required=False,
                    default="pdf"
                ),
                ScriptInput(
                    name="charts_config",
                    type="yaml",
                    description="Configuration for charts and visualizations",
                    required=False
                )
            ],
            outputs=[
                ScriptOutput(
                    name="energy_report",
                    type="file",
                    description="Complete energy analysis report",
                    format="PDF, HTML, or DOCX document"
                ),
                ScriptOutput(
                    name="key_indicators",
                    type="json",
                    description="Key performance indicators summary",
                    format="json with metrics like EUI, renewable share, costs"
                ),
                ScriptOutput(
                    name="charts",
                    type="directory",
                    description="Generated charts and visualizations",
                    format="directory with PNG/SVG files"
                )
            ],
            tags=["report", "visualization", "kpi", "analysis", "pdf", "charts"]
        ),

        # 5. Validation script
        Script(
            id="validation-001",
            name="model_validation",
            path="/cea/scripts/validation/energy_model_validator.py",
            cli="cea validate --simulation simulation_results.csv --measured measured_data.csv --metrics rmse,mape",
            doc="Validate simulation results against measured data using statistical metrics and uncertainty analysis",
            inputs=[
                ScriptInput(
                    name="simulation_results",
                    type="csv",
                    description="Simulated energy consumption data",
                    required=True
                ),
                ScriptInput(
                    name="measured_data",
                    type="csv",
                    description="Measured energy consumption from building monitoring",
                    required=True
                ),
                ScriptInput(
                    name="validation_metrics",
                    type="list",
                    description="Metrics to calculate: rmse, mape, r2, cvrmse",
                    required=False,
                    default=["rmse", "mape", "r2"]
                ),
                ScriptInput(
                    name="confidence_level",
                    type="float",
                    description="Confidence level for uncertainty bounds (0.0-1.0)",
                    required=False,
                    default=0.95
                )
            ],
            outputs=[
                ScriptOutput(
                    name="validation_metrics",
                    type="json",
                    description="Statistical validation metrics",
                    format="json with RMSE, MAPE, R², CV(RMSE) values"
                ),
                ScriptOutput(
                    name="calibration_report",
                    type="pdf",
                    description="Model calibration and validation report",
                    format="PDF with plots and statistical analysis"
                ),
                ScriptOutput(
                    name="uncertainty_bounds",
                    type="csv",
                    description="Confidence intervals for predictions",
                    format="timestamp,simulation,measured,lower_bound,upper_bound"
                )
            ],
            tags=["validation", "calibration", "statistics", "uncertainty", "monitoring", "measured"]
        )
    ]

    # Upsert all scripts
    script_ids = {}
    for script in scripts:
        script_id = await dao.upsert_script(script)
        script_ids[script.name] = script_id
        logger.info(f"Seeded script: {script.name}")

    # Define workflows that reference the scripts
    workflows = [
        # Complete energy analysis workflow
        Workflow(
            id="workflow-complete-001",
            name="complete_energy_analysis",
            description="Complete building energy analysis from demand calculation to optimization and reporting",
            steps=[
                WorkflowStep(
                    step=1,
                    script_id=script_ids["demand_calculation"],
                    script_name="demand_calculation",
                    action="calculate_building_demands",
                    description="Calculate heating and cooling demands for all buildings",
                    parameters={
                        "include_solar_gains": True,
                        "thermal_comfort_model": "ISO_13790"
                    }
                ),
                WorkflowStep(
                    step=2,
                    script_id=script_ids["network_layout"],
                    script_name="network_layout",
                    action="optimize_network_layout",
                    description="Design optimal thermal network connecting buildings",
                    depends_on=[1],
                    parameters={
                        "algorithm": "steiner",
                        "max_pipe_diameter": 500
                    }
                ),
                WorkflowStep(
                    step=3,
                    script_id=script_ids["supply_system_optimization"],
                    script_name="supply_system_optimization",
                    action="optimize_supply_systems",
                    description="Optimize energy supply system configuration",
                    depends_on=[1, 2],
                    parameters={
                        "objectives": ["cost", "emissions", "renewable_share"],
                        "max_iterations": 1000
                    }
                ),
                WorkflowStep(
                    step=4,
                    script_id=script_ids["energy_report_generator"],
                    script_name="energy_report_generator",
                    action="generate_analysis_report",
                    description="Generate comprehensive energy analysis report",
                    depends_on=[1, 2, 3],
                    parameters={
                        "output_format": "pdf",
                        "include_sensitivity_analysis": True
                    }
                )
            ],
            tags=["complete", "analysis", "workflow", "energy", "optimization"]
        ),

        # Model validation workflow
        Workflow(
            id="workflow-validation-001",
            name="model_validation_workflow",
            description="Validate energy models against measured data with calibration and uncertainty analysis",
            steps=[
                WorkflowStep(
                    step=1,
                    script_id=script_ids["demand_calculation"],
                    script_name="demand_calculation",
                    action="simulate_building_performance",
                    description="Run building energy simulation with initial parameters",
                    parameters={
                        "timestep": "hourly",
                        "include_uncertainties": True
                    }
                ),
                WorkflowStep(
                    step=2,
                    script_id=script_ids["model_validation"],
                    script_name="model_validation",
                    action="validate_against_measured_data",
                    description="Compare simulation results with measured data",
                    depends_on=[1],
                    parameters={
                        "validation_metrics": ["rmse", "mape", "r2", "cvrmse"],
                        "confidence_level": 0.95
                    }
                ),
                WorkflowStep(
                    step=3,
                    script_id=script_ids["energy_report_generator"],
                    script_name="energy_report_generator",
                    action="generate_validation_report",
                    description="Generate model validation and calibration report",
                    depends_on=[1, 2],
                    parameters={
                        "report_type": "validation",
                        "include_calibration_plots": True
                    }
                )
            ],
            tags=["validation", "calibration", "measured", "uncertainty", "workflow"]
        ),

        # Cooling demand estimation workflow
        Workflow(
            id="workflow-cooling-demand-001",
            name="estimate_cooling_demand",
            description="Estimate building cooling demand using weather data and building geometry",
            steps=[
                WorkflowStep(
                    step=1,
                    script_id=script_ids["demand_calculation"],
                    script_name="demand_calculation",
                    action="calculate_thermal_loads",
                    description="Calculate hourly cooling loads for buildings",
                    parameters={
                        "thermal_model": "iso13790",
                        "timestep": "hourly",
                        "include_solar_gains": True
                    }
                ),
                WorkflowStep(
                    step=2,
                    script_id=script_ids["energy_report_generator"],
                    script_name="energy_report_generator",
                    action="generate_demand_summary",
                    description="Generate cooling demand analysis report",
                    depends_on=[1],
                    parameters={
                        "report_type": "demand_analysis",
                        "include_peak_loads": True,
                        "include_monthly_totals": True
                    }
                )
            ],
            tags=["cooling", "demand", "estimation", "thermal", "loads"]
        ),

        # Cost-optimal cooling system design workflow
        Workflow(
            id="workflow-cooling-system-001",
            name="design_cost_optimal_cooling_system",
            description="Design a cost-optimal cooling system for buildings with renewable integration",
            steps=[
                WorkflowStep(
                    step=1,
                    script_id=script_ids["demand_calculation"],
                    script_name="demand_calculation",
                    action="calculate_cooling_demands",
                    description="Calculate detailed cooling loads",
                    parameters={
                        "thermal_model": "detailed",
                        "include_solar_gains": True,
                        "timestep": "hourly"
                    }
                ),
                WorkflowStep(
                    step=2,
                    script_id=script_ids["supply_system_optimization"],
                    script_name="supply_system_optimization",
                    action="optimize_cooling_systems",
                    description="Optimize cooling system configuration for minimum cost",
                    depends_on=[1],
                    parameters={
                        "objectives": ["cost"],
                        "technology_types": ["heat_pump", "chiller", "free_cooling", "solar_cooling"],
                        "include_storage": True
                    }
                ),
                WorkflowStep(
                    step=3,
                    script_id=script_ids["energy_report_generator"],
                    script_name="energy_report_generator",
                    action="generate_optimization_report",
                    description="Generate cost-optimal system design report",
                    depends_on=[1, 2],
                    parameters={
                        "report_type": "system_design",
                        "include_cost_breakdown": True,
                        "include_performance_curves": True
                    }
                )
            ],
            tags=["cost", "optimal", "cooling", "system", "design", "optimization"]
        ),

        # GHG evaluation of existing system workflow
        Workflow(
            id="workflow-ghg-evaluation-001",
            name="evaluate_ghg_existing_system",
            description="Evaluate greenhouse gas emissions of existing energy systems",
            steps=[
                WorkflowStep(
                    step=1,
                    script_id=script_ids["demand_calculation"],
                    script_name="demand_calculation",
                    action="calculate_energy_demands",
                    description="Calculate current energy consumption patterns",
                    parameters={
                        "existing_systems": True,
                        "include_measured_data": True,
                        "timestep": "monthly"
                    }
                ),
                WorkflowStep(
                    step=2,
                    script_id=script_ids["supply_system_optimization"],
                    script_name="supply_system_optimization",
                    action="assess_emissions",
                    description="Assess GHG emissions from existing systems",
                    depends_on=[1],
                    parameters={
                        "assessment_type": "existing_systems",
                        "emission_factors": "regional",
                        "include_lifecycle": True
                    }
                ),
                WorkflowStep(
                    step=3,
                    script_id=script_ids["energy_report_generator"],
                    script_name="energy_report_generator",
                    action="generate_ghg_report",
                    description="Generate GHG assessment report",
                    depends_on=[1, 2],
                    parameters={
                        "report_type": "ghg_assessment",
                        "include_benchmarks": True,
                        "include_reduction_scenarios": True
                    }
                )
            ],
            tags=["ghg", "emissions", "evaluation", "existing", "assessment", "carbon"]
        )
    ]

    # Upsert all workflows
    for workflow in workflows:
        workflow_id = await dao.upsert_workflow(workflow)
        logger.info(f"Seeded workflow: {workflow.name}")

    logger.info(f"Database seeding completed successfully!")
    logger.info(f"Seeded {len(scripts)} scripts and {len(workflows)} workflows")


async def print_database_contents(dao: DAO) -> None:
    """Print all scripts and workflows in the database"""
    print("\n" + "="*80)
    print("DATABASE CONTENTS")
    print("="*80)

    # Print all scripts
    scripts = await dao.get_all_scripts()
    print(f"\nSCRIPTS ({len(scripts)} total):")
    print("-" * 40)

    for script in scripts:
        print(f"\n[SCRIPT] {script.name}")
        print(f"   ID: {script.id}")
        print(f"   Path: {script.path}")
        print(f"   CLI: {script.cli}")
        print(f"   Description: {script.doc}")
        print(f"   Tags: {', '.join(script.tags)}")
        print(f"   Inputs: {len(script.inputs)} parameters")
        print(f"   Outputs: {len(script.outputs)} files")

    # Print all workflows
    workflows = await dao.get_all_workflows()
    print(f"\nWORKFLOWS ({len(workflows)} total):")
    print("-" * 40)

    for workflow in workflows:
        print(f"\n[WORKFLOW] {workflow.name}")
        print(f"   ID: {workflow.id}")
        print(f"   Description: {workflow.description}")
        print(f"   Tags: {', '.join(workflow.tags)}")
        print(f"   Steps: {len(workflow.steps)} steps")

        for step in workflow.steps:
            depends = f" (depends on: {step.depends_on})" if step.depends_on else ""
            print(f"     {step.step}. {step.action}{depends}")

    print("\n" + "="*80)


if __name__ == "__main__":
    async def main() -> None:
        dao = DAO()
        await dao.recreate_tables()
        await seed_database(dao)
        await print_database_contents(dao)

    asyncio.run(main())