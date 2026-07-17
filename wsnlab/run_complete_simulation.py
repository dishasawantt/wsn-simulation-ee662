"""
Complete Assignment 2 Runner with Metrics Analysis
This script runs simulations and automatically generates detailed reports
"""

import sys
sys.path.insert(1, '.')
from source import config

# Configure simulation (modify as needed)
config.SIM_NODE_COUNT = 30
config.SIM_DURATION = 400
config.MAX_CHILD_NODES_PER_CLUSTER = 8
config.PACKET_LOSS_RATIO = 0.05
config.ENABLE_ENERGY_MODEL = True
config.INITIAL_ENERGY = 8000.0
config.USE_MESH_ROUTING = True
config.ENABLE_CLUSTER_OPTIMIZATION = True
config.SIM_VISUALIZATION = True

print("\n" + "="*80)
print("ASSIGNMENT 2 - COMPLETE SIMULATION WITH ANALYSIS")
print("="*80)
print("\nCONFIGURATION:")
print(f"  Nodes: {config.SIM_NODE_COUNT}")
print(f"  Duration: {config.SIM_DURATION}s")
print(f"  Max children per cluster: {config.MAX_CHILD_NODES_PER_CLUSTER}")
print(f"  Packet loss ratio: {config.PACKET_LOSS_RATIO*100:.1f}%")
print(f"  Energy model: {config.ENABLE_ENERGY_MODEL}")
print(f"  Initial energy: {config.INITIAL_ENERGY} mJ")
print(f"  Mesh routing: {config.USE_MESH_ROUTING}")
print(f"  Cluster optimization: {config.ENABLE_CLUSTER_OPTIMIZATION}")
print("="*80)

# Run the simulation
exec(open('assignment2_implementation.py').read())

# Perform detailed analysis
print("\nPerforming detailed metrics analysis...")
from metrics_analysis import analyze_metrics, export_to_json, export_to_csv

analyze_metrics(logger)

# Export data
export_choice = input("\nExport simulation data? (json/csv/both/no): ").strip().lower()
if export_choice in ['json', 'both']:
    export_to_json(logger, 'assignment2_results.json')
if export_choice in ['csv', 'both']:
    export_to_csv(logger, 'assignment2_results')

print("\nSimulation complete!")
print("\nTo run different scenarios, use: python test_scenarios.py")
print("To test failure recovery, use: python test_failure_recovery.py")

