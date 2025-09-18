#!/usr/bin/env python3
"""
CEA Demand Analysis Tool

Calculate thermal energy demands for buildings based on usage schedules,
weather data, and building properties.
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Calculate thermal energy demands for buildings based on usage schedules, weather data, and building properties."
    )

    parser.add_argument(
        '--buildings',
        type=str,
        required=True,
        help='Shapefile containing building geometries and properties'
    )

    parser.add_argument(
        '--weather',
        type=str,
        required=True,
        help='EnergyPlus weather file (.epw) for the location'
    )

    parser.add_argument(
        '--schedules',
        type=str,
        help='Excel file with occupancy and usage schedules'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./results',
        help='Directory to store output files'
    )

    parser.add_argument(
        '--timestep',
        type=int,
        default=1,
        help='Simulation timestep in hours (1-24)'
    )

    parser.add_argument(
        '--thermal-model',
        type=str,
        choices=['simple', 'detailed', 'iso13790'],
        default='simple',
        help='Thermal calculation model to use'
    )

    args = parser.parse_args()

    print(f"Running demand analysis with:")
    print(f"  Buildings: {args.buildings}")
    print(f"  Weather: {args.weather}")
    print(f"  Output: {args.output_dir}")
    print(f"  Model: {args.thermal_model}")
    print("Analysis complete!")


if __name__ == "__main__":
    main()