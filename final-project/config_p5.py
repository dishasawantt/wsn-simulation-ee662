## Configuration for Part 5: Minimal Cluster Overlap & CH Migration
## Extends Part 4 with:
## - MINIMAL_CLUSTER_OVERLAP: Reduce cluster overlap
## - ROUTER role: Bridge communication between CHs
## - CH_HANDOFF: Allow CH role to migrate between nodes

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 50
SIM_NODE_PLACING_CELL_SIZE = 60
SIM_DURATION = 2000
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (400, 400)
SIM_TITLE = 'Part 5: Minimal Cluster Overlap & CH Migration'
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
DATA_PACKET_START_TIME = 400
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

# ============== CLUSTER SIZE & TX POWER (from Part 4) ==============
MAX_CLUSTER_MEMBERS = 15
USE_UNIFORM_TX_POWER = True
TX_POWER_MIN = 100
TX_POWER_MAX = 150
TX_POWER_DEFAULT = 100

CLUSTER_TX_POWER = {}

# ============== PACKET LOSS (from Part 4) ==============
PACKET_LOSS_RATE = 0
PACKET_LOSS_APPLY_TO_CONTROL = True
PACKET_LOSS_APPLY_TO_DATA = True
ENABLE_DISTANCE_DEPENDENT_LOSS = False
DISTANCE_LOSS_FACTOR = 0

# ============== TCP-LIKE RELIABILITY (from Part 4) ==============
ENABLE_RETRANSMISSION = True
MAX_RETRIES = 3
RETRANSMIT_TIMEOUT = 3.0
ENABLE_ACK = True
RELIABLE_PACKET_TYPES = ['DATA', 'NETWORK_REQUEST']
ENABLE_MULTIPATH_ROUTING = True
MULTIPATH_PACKET_TYPES = ['DATA']
MULTIPATH_REDUNDANCY = 2

# ======================================================================
# PART 5 NEW FEATURES: MINIMAL OVERLAP & CH MIGRATION
# ======================================================================

# ============== 5a: MINIMAL CLUSTER OVERLAP ==============
ENABLE_MINIMAL_OVERLAP = True      # Enable minimal overlap mode
MIN_CH_DISTANCE = 60               # Minimum distance between cluster heads (meters)
                                    # New CH won't form if existing CH is within this distance

# When minimal overlap is enabled:
# - NETWORK_REQUEST is rejected if requester is too close to existing CH
# - Forces nodes to join existing clusters rather than forming new ones
# - Results in less overlap but potentially larger clusters

# ============== 5b: ROUTER NODES ==============
ENABLE_ROUTER_NODES = True         # Enable router role for CH-to-CH bridging
ROUTER_COLOR = (1.0, 0.5, 0.0)     # Orange color for routers

# Router criteria:
# - Node is member of one cluster but can hear another CH
# - Becomes ROUTER when it bridges two or more clusters
# - Forwards inter-cluster traffic

# ============== 5c: CH ROLE MIGRATION (HANDOFF) ==============
ENABLE_CH_HANDOFF = True           # Allow CH role to move between nodes

# Handoff triggers:
CH_HANDOFF_LOW_ENERGY_THRESHOLD = 0.2   # Trigger handoff when energy < 20%
CH_HANDOFF_LOAD_IMBALANCE = 1.5         # Trigger if member count > avg * 1.5
CH_HANDOFF_TIMEOUT = 10.0               # Seconds to wait for handoff completion

# Handoff process:
# 1. Current CH sends CH_HANDOFF_REQUEST to candidate (best member)
# 2. Candidate responds with CH_HANDOFF_ACCEPT
# 3. Current CH transfers members table, child_networks, etc.
# 4. Current CH sends CH_HANDOFF_COMPLETE and becomes REGISTERED
# 5. New CH broadcasts NETWORK_UPDATE to inform network

# ============== 5d: CH SELECTION CRITERIA ==============
# When selecting a new CH candidate for handoff or new cluster:
CH_SELECTION_PREFER_HIGH_ENERGY = True   # Prefer nodes with more remaining energy
CH_SELECTION_PREFER_CENTRAL = True       # Prefer nodes central to cluster members
CH_SELECTION_MIN_NEIGHBORS = 2           # Minimum neighbors to become CH


