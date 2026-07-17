## Configuration for Part 6: Network Recovery from Node/Link Failures
## Extends Part 5 with:
## - Random node failures during simulation
## - Node recovery after failure
## - Orphan detection and network repair
## - Event logging and recovery metrics

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 50
SIM_NODE_PLACING_CELL_SIZE = 50
SIM_DURATION = 2500               # Longer duration to test recovery
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (400, 400)
SIM_TITLE = 'Part 6: Network Recovery from Node/Link Failures'
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

# ======================================================================
# PART 6 NEW FEATURES: NODE/LINK FAILURE & RECOVERY
# ======================================================================

# ============== 6a: RANDOM NODE FAILURES ==============
ENABLE_RANDOM_FAILURES = True      # Enable random node killing during simulation
FAILURE_START_TIME = 600           # Start killing nodes after this time (let network form first)
FAILURE_INTERVAL = 200             # Time between random node failures
MAX_FAILURES = 3                   # Maximum number of nodes to kill
FAILURE_DURATION = 300             # How long a node stays dead before recovery
EXCLUDE_ROOT_FROM_FAILURE = True   # Don't kill the ROOT node

# ============== 6b: ORPHAN DETECTION ==============
ORPHAN_DETECTION_INTERVAL = 30     # How often to check for orphan status
HEARTBEAT_MISS_THRESHOLD = 3       # Number of missed heartbeats before declaring orphan
PARENT_TIMEOUT = 60                # Seconds without parent heartbeat before becoming orphan

# ============== 6c: NETWORK REPAIR ==============
ENABLE_NETWORK_REPAIR = True       # Enable automatic network repair
REPAIR_DELAY = 5                   # Seconds to wait before attempting repair
REPAIR_MAX_ATTEMPTS = 5            # Maximum repair attempts before giving up
BROADCAST_ORPHAN_STATUS = True     # Broadcast I_AM_ORPHAN to find new parent

# ============== 6d: EVENT LOGGING ==============
LOG_NODE_FAILURES = True           # Log when nodes fail
LOG_NODE_RECOVERIES = True         # Log when nodes recover
LOG_ORPHAN_EVENTS = True           # Log orphan detection
LOG_ROLE_CHANGES = True            # Log all role changes
LOG_NETWORK_JOINS = True           # Log network join events

# Dead node color
DEAD_NODE_COLOR = (0.5, 0.0, 0.0)  # Dark red for dead nodes


