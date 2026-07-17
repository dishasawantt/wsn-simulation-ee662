#!/usr/bin/env python3
"""
Optimized Assignment 2 Simulation Runner
Fast execution with all deliverables
"""
import sys
sys.path.insert(1, '.')
from source import config

config.SIM_NODE_COUNT = 35
config.SIM_DURATION = 500
config.NODE_ARRIVAL_MAX = 10
config.SIM_VISUALIZATION = True
config.MAX_CHILD_NODES_PER_CLUSTER = 8
config.PACKET_LOSS_RATIO = 0.05
config.ENABLE_ENERGY_MODEL = True
config.USE_MESH_ROUTING = True
config.ENABLE_CLUSTER_OPTIMIZATION = True

exec(open('assignment2_implementation.py').read())

if not config.SIM_VISUALIZATION:
    from metrics_analysis import analyze_metrics
    analyze_metrics(logger)

