## Configuration for Part 7: Cluster Optimization Protocol
## Extends Part 6 with:
## - Cluster merging to minimize number of clusters
## - Load balancing across clusters
## - CH rotation for energy efficiency

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 50
SIM_NODE_PLACING_CELL_SIZE = 50
SIM_DURATION = 3000               # Longer duration for optimization to take effect
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (400, 400)
SIM_TITLE = 'Part 7: Cluster Optimization Protocol'
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
DATA_PACKET_START_TIME = 500
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
TX_POWER_DEFAULT = 120

CLUSTER_TX_POWER = {}

# ============== PACKET LOSS (from Part 4) ==============
PACKET_LOSS_RATE = 0.02
PACKET_LOSS_APPLY_TO_CONTROL = True
PACKET_LOSS_APPLY_TO_DATA = True
ENABLE_DISTANCE_DEPENDENT_LOSS = False
DISTANCE_LOSS_FACTOR = 0.1

# ============== TCP-LIKE RELIABILITY (from Part 4) ==============
ENABLE_RETRANSMISSION = True
MAX_RETRIES = 3
RETRANSMIT_TIMEOUT = 3.0
ENABLE_ACK = True
RELIABLE_PACKET_TYPES = ['DATA', 'NETWORK_REQUEST']
ENABLE_MULTIPATH_ROUTING = True
MULTIPATH_PACKET_TYPES = ['DATA']
MULTIPATH_REDUNDANCY = 2

# ============== MINIMAL OVERLAP & CH MIGRATION (from Part 5) ==============
ENABLE_MINIMAL_OVERLAP = True
MIN_CH_DISTANCE = 60
ENABLE_ROUTER_NODES = True
ROUTER_COLOR = (1.0, 0.5, 0.0)
ENABLE_CH_HANDOFF = True
CH_HANDOFF_LOW_ENERGY_THRESHOLD = 0.2
CH_HANDOFF_LOAD_IMBALANCE = 1.5
CH_HANDOFF_TIMEOUT = 10.0
CH_SELECTION_PREFER_HIGH_ENERGY = True
CH_SELECTION_PREFER_CENTRAL = True
CH_SELECTION_MIN_NEIGHBORS = 2

# ============== NODE/LINK FAILURE & RECOVERY (from Part 6) ==============
ENABLE_RANDOM_FAILURES = False     # Disable for optimization testing
FAILURE_START_TIME = 600
FAILURE_INTERVAL = 200
MAX_FAILURES = 3
FAILURE_DURATION = 300
EXCLUDE_ROOT_FROM_FAILURE = True
ORPHAN_DETECTION_INTERVAL = 30
HEARTBEAT_MISS_THRESHOLD = 3
PARENT_TIMEOUT = 60
ENABLE_NETWORK_REPAIR = True
REPAIR_DELAY = 5
REPAIR_MAX_ATTEMPTS = 5
BROADCAST_ORPHAN_STATUS = True
LOG_NODE_FAILURES = True
LOG_NODE_RECOVERIES = True
LOG_ORPHAN_EVENTS = True
LOG_ROLE_CHANGES = True
LOG_NETWORK_JOINS = True
DEAD_NODE_COLOR = (0.5, 0.0, 0.0)

# ======================================================================
# PART 7 NEW FEATURES: CLUSTER OPTIMIZATION PROTOCOL
# ======================================================================

# ============== 7a: CLUSTER MERGING ==============
ENABLE_CLUSTER_OPTIMIZATION = True        # Master switch for optimization
OPTIMIZATION_START_TIME = 400             # Start optimization after network forms
OPTIMIZATION_INTERVAL = 100               # Check for optimization every N seconds
MIN_CLUSTER_SIZE = 3                      # Clusters smaller than this try to merge
ENABLE_CLUSTER_MERGING = True             # Allow small clusters to merge with neighbors
MERGE_PREFERENCE = 'NEAREST'              # 'NEAREST' or 'LARGEST' - prefer merge target

# ============== 7b: LOAD BALANCING ==============
ENABLE_LOAD_BALANCING = True              # Balance members across clusters
LOAD_BALANCE_THRESHOLD = 2.0              # Rebalance if cluster size > avg * threshold
LOAD_BALANCE_MIN_DIFF = 4                 # Minimum size difference to trigger rebalance
MEMBER_TRANSFER_COUNT = 2                 # How many members to transfer at once

# ============== 7c: ENERGY-AWARE CH ROTATION ==============
ENABLE_CH_ROTATION = True                 # Rotate CH role for energy efficiency
CH_ROTATION_INTERVAL = 600                # Check for rotation every N seconds
CH_ROTATION_ENERGY_THRESHOLD = 0.3        # Rotate if CH energy < threshold
PREFER_HIGH_DEGREE_CH = True              # Prefer CH with more neighbors (central)

# ============== OPTIMIZATION PACKET TYPES ==============
# CLUSTER_MERGE_REQUEST: CH requests to merge into another cluster
# CLUSTER_MERGE_ACCEPT: Target CH accepts merge request
# CLUSTER_MERGE_NOTIFY: Notify members to move to new cluster
# MEMBER_TRANSFER_REQUEST: Request to move member from one cluster to another
# MEMBER_TRANSFER_ACCEPT: Accept member transfer
# CH_ROTATION_REQUEST: Request another node to become CH
# CH_ROTATION_ACCEPT: Accept becoming new CH

# ============== OPTIMIZATION METRICS ==============
# Tracked in OPTIMIZATION_STATS:
# - clusters_merged: Number of successful cluster merges
# - merge_attempts: Total merge attempts
# - members_transferred: Members moved during load balancing
# - load_balance_events: Number of rebalancing events
# - ch_rotations: Number of CH role rotations
# - clusters_eliminated: Clusters removed by optimization


