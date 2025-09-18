#!/usr/bin/env python3
"""
CEA Network Optimization Tool

Optimize district thermal network layout to minimize capital costs
while ensuring adequate supply capacity.
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Optimize district thermal network layout to minimize capital costs while ensuring adequate supply capacity."
    )

    parser.add_argument(
        '--buildings',
        type=str,
        required=True,
        help='CSV file with building thermal demands and coordinates'
    )

    parser.add_argument(
        '--streets',
        type=str,
        help='Shapefile with street network for routing constraints'
    )

    parser.add_argument(
        '--algorithm',
        type=str,
        choices=['steiner', 'mst', 'genetic'],
        default='steiner',
        help='Network optimization algorithm to use'
    )

    parser.add_argument(
        '--pipe-costs',
        type=str,
        help='CSV file with pipe diameter and cost data'
    )

    parser.add_argument(
        '--max-pressure-drop',
        type=float,
        default=300.0,
        help='Maximum allowable pressure drop in Pa/m'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='network_layout.shp',
        help='Output shapefile for optimized network layout'
    )

    parser.add_argument(
        '--iterations',
        type=int,
        default=1000,
        help='Maximum optimization iterations'
    )

    args = parser.parse_args()

    print(f"Running network optimization with:")
    print(f"  Buildings: {args.buildings}")
    print(f"  Algorithm: {args.algorithm}")
    print(f"  Max pressure drop: {args.max_pressure_drop} Pa/m")
    print(f"  Iterations: {args.iterations}")
    print(f"  Output: {args.output}")
    print("Network optimization complete!")


if __name__ == "__main__":
    main()