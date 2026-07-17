## Configuration for Part 1: Neighbor Discovery Protocol
## Implements single-hop and multi-hop neighbor tables

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100  # transmission range of nodes
NODE_ARRIVAL_MAX = 200  # max time to wake up

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 100
SIM_NODE_PLACING_CELL_SIZE = 60
SIM_DURATION = 500
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (600, 600)
SIM_TITLE = 'Part 1: Neighbor Discovery Protocol'
SIM_VISUALIZATION = True
SCALE = 1

# ============== NEIGHBOR TABLE CONFIGURATION ==============
# Toggle between single-hop and multi-hop modes
MULTI_HOP_NEIGHBOR_TABLE = True  # False = 1-hop only, True = multi-hop via sharing
MAX_NEIGHBOR_HOPS = 2  # Maximum hops to track (2 = two-hop neighbors)

# ============== HEARTBEAT / HELLO PROTOCOL ==============
HELLO_INTERVAL = 20  # How often nodes send HELLO/heartbeat messages
NEIGHBOR_TIMEOUT_FACTOR = 3  # Neighbor considered dead after HELLO_INTERVAL * this factor

# ============== LOGGING ==============
ENABLE_LOGGING = True
EXPORT_NEIGHBOR_TABLE = True
METRICS_EXPORT_PATH = './output_p1/'

