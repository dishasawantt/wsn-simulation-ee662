## Configuration for Part 4: Configurable Network Parameters
## Extends Part 3 with:
## - MAX_CLUSTER_MEMBERS: Limit on nodes per cluster
## - TX_POWER: Per-cluster or uniform transmission power
## - PACKET_LOSS_RATE: Simulated channel impairments

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 100
SIM_NODE_PLACING_CELL_SIZE = 50   # Spacing between nodes
SIM_DURATION = 2000               # More time for network formation
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (500, 500)     # Match cell_size * 10 for 10x10 grid
SIM_TITLE = 'Part 4: Configurable Parameters (Cluster Size, TxPower, Packet Loss)'
SIM_VISUALIZATION = True
SCALE = 1

# ============== HEARTBEAT & TIMERS ==============
HEARTH_BEAT_TIME_INTERVAL = 20

# ============== TABLE TIMEOUT FACTORS ==============
NEIGHBOR_TIMEOUT_FACTOR = 3
MEMBER_TIMEOUT_FACTOR = 5
CHILD_NET_TIMEOUT_FACTOR = 5

# ============== ROUTING CONFIGURATION ==============
ENABLE_MESH_ROUTING = True
USE_TWO_HOP_MESH = True

# ============== DATA PACKET CONFIGURATION ==============
ENABLE_DATA_PACKETS = True
DATA_PACKET_START_TIME = 300
DATA_PACKET_INTERVAL = 50
DATA_PACKET_COUNT = 5

# ============== METRICS & TRACING ==============
ENABLE_PACKET_TRACING = True
ENABLE_JOIN_TIME_TRACKING = True
ENABLE_DELAY_TRACKING = True

# ============== LOGGING & EXPORT ==============
ENABLE_LOGGING = True
EXPORT_TABLES = True
EXPORT_METRICS = True

# ======================================================================
# PART 4 NEW FEATURES: CONFIGURABLE PARAMETERS
# ======================================================================

# ============== 4a: CLUSTER SIZE LIMIT ==============
# Maximum number of member nodes allowed in a cluster (excluding the CH itself)
# When a cluster reaches this limit, new JOIN_REQUESTs will be forwarded
# to allow formation of a new cluster
MAX_CLUSTER_MEMBERS = 0  # Set to 0 or None for unlimited (larger for dense networks)

# ============== 4b: TX POWER CONFIGURATION ==============
# Transmission power affects the node's TX range
# TX_RANGE = NODE_TX_RANGE * (TX_POWER_DBM / TX_POWER_MAX) for simplicity

USE_UNIFORM_TX_POWER = True   # If True, all nodes use TX_POWER_DEFAULT
                               # If False, each cluster can have different power

TX_POWER_MIN = 120    # Minimum TX range (meters) - realistic for WSN
TX_POWER_MAX = 200   # Maximum TX range (meters)
TX_POWER_DEFAULT = 60  # Default TX range for uniform mode

# Per-cluster TX power configuration (used when USE_UNIFORM_TX_POWER = False)
# Format: {cluster_net_addr: tx_power}
# Clusters not listed will use TX_POWER_DEFAULT
CLUSTER_TX_POWER = {
    # Example: 12: 120,  # Cluster 12 uses TX range 120
    #          6: 80,    # Cluster 6 uses TX range 80
}

# ============== 4c: PACKET LOSS CONFIGURATION ==============
# Packet loss rate (0.0 = perfect channel, 1.0 = all packets lost)
PACKET_LOSS_RATE = 0  # 2% packet loss (lower for better connectivity during testing)

# Apply packet loss to specific packet types (True/False)
PACKET_LOSS_APPLY_TO_CONTROL = True   # PROBE, HEART_BEAT, JOIN_*, NETWORK_*
PACKET_LOSS_APPLY_TO_DATA = True      # DATA packets

# Distance-dependent loss: packets to farther nodes have higher loss
# Formula: effective_loss = PACKET_LOSS_RATE + (distance/tx_range) * DISTANCE_LOSS_FACTOR
ENABLE_DISTANCE_DEPENDENT_LOSS = False
DISTANCE_LOSS_FACTOR = 0.1  # Additional loss factor based on distance

# ======================================================================
# TCP-LIKE RELIABILITY FEATURES
# ======================================================================

# ============== RETRANSMISSION CONFIGURATION ==============
ENABLE_RETRANSMISSION = True      # Enable packet retransmission on timeout/no ACK
MAX_RETRIES = 3                   # Maximum retransmission attempts
RETRANSMIT_TIMEOUT = 3.0          # Seconds to wait before retransmitting
ENABLE_ACK = True                 # Enable acknowledgment packets

# Packet types that require acknowledgment (reliable delivery)
# Note: JOIN_REQUEST excluded - uses TIMER_JOIN_REQUEST for retries since unregistered nodes have no address
RELIABLE_PACKET_TYPES = ['DATA', 'NETWORK_REQUEST']

# ============== MULTIPATH ROUTING ==============
ENABLE_MULTIPATH_ROUTING = True   # Send critical packets via multiple paths
MULTIPATH_PACKET_TYPES = ['DATA']  # Packet types to use multipath for
MULTIPATH_REDUNDANCY = 2          # Number of paths to try (max)


