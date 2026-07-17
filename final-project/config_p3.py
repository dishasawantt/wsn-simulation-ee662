## Configuration for Part 3: Hybrid Mesh-Tree Routing with Metrics
## Extends Part 2 with routing, packet tracing, and timing metrics

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 70
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 100
SIM_NODE_PLACING_CELL_SIZE = 60
SIM_DURATION = 800
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (600, 600)
SIM_TITLE = 'Part 3: Hybrid Mesh-Tree Routing with Metrics'
SIM_VISUALIZATION = True
SCALE = 1

# ============== HEARTBEAT & TIMERS ==============
HEARTH_BEAT_TIME_INTERVAL = 20

# ============== TABLE TIMEOUT FACTORS ==============
NEIGHBOR_TIMEOUT_FACTOR = 3
MEMBER_TIMEOUT_FACTOR = 5
CHILD_NET_TIMEOUT_FACTOR = 5

# ============== ROUTING CONFIGURATION ==============
ENABLE_MESH_ROUTING = True  # Use mesh routing when destination in neighbor table
USE_TWO_HOP_MESH = True     # Also try 2-hop neighbors for mesh routing

# ============== DATA PACKET CONFIGURATION ==============
ENABLE_DATA_PACKETS = True
DATA_PACKET_START_TIME = 300  # Start sending data packets after network forms
DATA_PACKET_INTERVAL = 50     # Interval between data packets per node
DATA_PACKET_COUNT = 5         # Number of data packets each node sends

# ============== METRICS & TRACING ==============
ENABLE_PACKET_TRACING = True
ENABLE_JOIN_TIME_TRACKING = True
ENABLE_DELAY_TRACKING = True

# ============== LOGGING & EXPORT ==============
ENABLE_LOGGING = True
EXPORT_TABLES = True
EXPORT_METRICS = True
METRICS_EXPORT_PATH = './'

