"""
Test Scenarios for Assignment 2
This script provides different test configurations to validate all deliverables
"""

import sys
sys.path.insert(1, '.')
from source import config

def scenario_1_basic_network():
    """Test basic network formation with default settings"""
    print("\n=== SCENARIO 1: Basic Network Formation ===")
    config.SIM_NODE_COUNT = 20
    config.SIM_DURATION = 300
    config.MAX_CHILD_NODES_PER_CLUSTER = 10
    config.PACKET_LOSS_RATIO = 0.0
    config.ENABLE_ENERGY_MODEL = False
    config.USE_MESH_ROUTING = True
    config.SIM_VISUALIZATION = True
    print("Testing basic network formation with 20 nodes")
    print("Expected: All nodes join network, low join times")

def scenario_2_packet_loss():
    """Test network behavior under packet loss"""
    print("\n=== SCENARIO 2: Packet Loss Scenario ===")
    config.SIM_NODE_COUNT = 25
    config.SIM_DURATION = 400
    config.PACKET_LOSS_RATIO = 0.15  # 15% packet loss
    config.ENABLE_ENERGY_MODEL = False
    config.USE_MESH_ROUTING = True
    print("Testing with 15% packet loss")
    print("Expected: Some packets dropped, recovery via retransmissions")

def scenario_3_cluster_capacity():
    """Test cluster capacity limits"""
    print("\n=== SCENARIO 3: Cluster Capacity Limits ===")
    config.SIM_NODE_COUNT = 40
    config.SIM_DURATION = 400
    config.MAX_CHILD_NODES_PER_CLUSTER = 5  # Small clusters
    config.PACKET_LOSS_RATIO = 0.0
    config.ENABLE_ENERGY_MODEL = False
    print("Testing with max 5 children per cluster")
    print("Expected: Deeper hierarchy, more cluster heads")

def scenario_4_energy_model():
    """Test energy depletion"""
    print("\n=== SCENARIO 4: Energy Model ===")
    config.SIM_NODE_COUNT = 25
    config.SIM_DURATION = 800
    config.ENABLE_ENERGY_MODEL = True
    config.INITIAL_ENERGY = 5000.0  # Lower energy
    config.USE_MESH_ROUTING = True
    print("Testing energy model with limited energy")
    print("Expected: Nodes die when energy depleted, network recovers")

def scenario_5_mesh_vs_tree():
    """Test mesh routing vs tree routing"""
    print("\n=== SCENARIO 5: Mesh vs Tree Routing ===")
    config.SIM_NODE_COUNT = 30
    config.SIM_DURATION = 400
    config.USE_MESH_ROUTING = True  # Change to False for comparison
    config.ENABLE_ENERGY_MODEL = False
    print("Testing mesh routing (set USE_MESH_ROUTING=False to test tree)")
    print("Expected: Shorter paths with mesh routing")

def scenario_6_cluster_optimization():
    """Test cluster optimization"""
    print("\n=== SCENARIO 6: Cluster Optimization ===")
    config.SIM_NODE_COUNT = 35
    config.SIM_DURATION = 500
    config.ENABLE_CLUSTER_OPTIMIZATION = True
    config.OPTIMIZATION_INTERVAL = 80
    config.OPTIMIZATION_TARGET = 'ENERGY'  # or 'CLUSTERS'
    config.ENABLE_ENERGY_MODEL = True
    print("Testing cluster optimization for energy")
    print("Expected: Clusters reorganize for better efficiency")

def scenario_7_high_density():
    """Test high node density"""
    print("\n=== SCENARIO 7: High Density Network ===")
    config.SIM_NODE_COUNT = 60
    config.SIM_DURATION = 500
    config.SIM_NODE_PLACING_CELL_SIZE = 40  # Closer spacing
    config.MAX_CHILD_NODES_PER_CLUSTER = 8
    config.USE_MESH_ROUTING = True
    print("Testing high density network")
    print("Expected: Good mesh routing performance, multiple paths")

def scenario_8_sparse_network():
    """Test sparse network"""
    print("\n=== SCENARIO 8: Sparse Network ===")
    config.SIM_NODE_COUNT = 25
    config.SIM_DURATION = 400
    config.SIM_NODE_PLACING_CELL_SIZE = 70  # Wider spacing
    config.NODE_TX_RANGE = 120  # Increased range
    print("Testing sparse network with increased tx range")
    print("Expected: Tree routing dominant, fewer alternate paths")

def scenario_9_combined_stress():
    """Test combined stress conditions"""
    print("\n=== SCENARIO 9: Combined Stress Test ===")
    config.SIM_NODE_COUNT = 45
    config.SIM_DURATION = 600
    config.PACKET_LOSS_RATIO = 0.1
    config.ENABLE_ENERGY_MODEL = True
    config.INITIAL_ENERGY = 6000.0
    config.MAX_CHILD_NODES_PER_CLUSTER = 6
    config.USE_MESH_ROUTING = True
    config.ENABLE_CLUSTER_OPTIMIZATION = True
    print("Testing combined conditions: packet loss + energy + optimization")
    print("Expected: Network adapts and survives")

# Menu system
scenarios = {
    '1': ('Basic Network Formation', scenario_1_basic_network),
    '2': ('Packet Loss Test', scenario_2_packet_loss),
    '3': ('Cluster Capacity Limits', scenario_3_cluster_capacity),
    '4': ('Energy Model', scenario_4_energy_model),
    '5': ('Mesh vs Tree Routing', scenario_5_mesh_vs_tree),
    '6': ('Cluster Optimization', scenario_6_cluster_optimization),
    '7': ('High Density Network', scenario_7_high_density),
    '8': ('Sparse Network', scenario_8_sparse_network),
    '9': ('Combined Stress Test', scenario_9_combined_stress),
}

if __name__ == '__main__':
    print("\n" + "="*80)
    print("ASSIGNMENT 2 - TEST SCENARIOS")
    print("="*80)
    print("\nAvailable test scenarios:")
    for key, (name, _) in scenarios.items():
        print(f"  {key}. {name}")
    print("\nEnter scenario number (or 'all' to run all scenarios sequentially):")
    
    choice = input("> ").strip()
    
    if choice.lower() == 'all':
        for key in sorted(scenarios.keys()):
            _, setup_func = scenarios[key]
            setup_func()
            print("\nPress Enter to continue to next scenario...")
            input()
            exec(open('assignment2_implementation.py').read())
    elif choice in scenarios:
        _, setup_func = scenarios[choice]
        setup_func()
        print("\nStarting simulation...")
        exec(open('assignment2_implementation.py').read())
    else:
        print(f"Invalid choice: {choice}")
        print("Running default scenario (Basic Network Formation)...")
        scenario_1_basic_network()
        exec(open('assignment2_implementation.py').read())

