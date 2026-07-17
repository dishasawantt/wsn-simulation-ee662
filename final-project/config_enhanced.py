## Enhanced configuration for WSN simulation
## Complete implementation of instructions.txt requirements

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 200

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 25
SIM_NODE_PLACING_CELL_SIZE = 45  # Tighter spacing for better connectivity
SIM_DURATION = 1000
SIM_TIME_SCALE = 0.01  # Faster simulation
SIM_TERRAIN_SIZE = (600, 600)  # Smaller terrain
SIM_TITLE = 'Enhanced WSN Simulation'
SIM_VISUALIZATION = True
SCALE = 1

# ============== APPLICATION PROPERTIES ==============
HEARTH_BEAT_TIME_INTERVAL = 50
REPAIRING_METHOD = 'FIND_ANOTHER_PARENT'
EXPORT_CH_CSV_INTERVAL = 10
EXPORT_NEIGHBOR_CSV_INTERVAL = 10

# ============== CLUSTER CONFIGURATION ==============
MAX_CLUSTER_MEMBERS = 5  # Smaller clusters = more CHs created
MINIMAL_CLUSTER_OVERLAP = False  # Disabled - was blocking CH creation
MIN_CH_DISTANCE = 40  # Reduced

# ============== TX POWER CONFIGURATION (Requirement #4b) ==============
TX_POWER_MIN = 80
TX_POWER_MAX = 150
TX_POWER_DEFAULT = 120  # Increased for better coverage
TX_POWER_PER_CLUSTER = True

# ============== PACKET LOSS (Requirement #4c) ==============
PACKET_LOSS_RATE = 0.02

# ============== FAILURE & RECOVERY (Requirement #6) ==============
ENABLE_RANDOM_FAILURES = True
FAILURE_PROBABILITY = 0.15
FAILURE_START_TIME = 200
FAILURE_END_TIME = 600
ENABLE_RANDOM_RECOVERY = True
RECOVERY_MIN_TIME = 50
RECOVERY_MAX_TIME = 150

# ============== ENERGY MODEL - CC2420 (Requirement #8) ==============
ENERGY_MODEL_ENABLED = True
BATTERY_CAPACITY_JOULES = 21600
SUPPLY_VOLTAGE = 3.0
TX_CURRENT_MA = 17.4
RX_CURRENT_MA = 18.8
DATA_RATE_KBPS = 250
PHY_OVERHEAD_BYTES = 6
DEFAULT_PACKET_SIZE_BYTES = 50
ENERGY_PER_BYTE_TX = 1.67e-6
ENERGY_PER_BYTE_RX = 1.80e-6
PLL_TURNAROUND_ENERGY = 10e-6
MIN_ENERGY_THRESHOLD = 0.001

# ============== NEIGHBOR TABLE ==============
NEIGHBOR_TABLE_HOPS = 2
NEIGHBOR_AGING_FACTOR = 3

# ============== LOGGING & METRICS ==============
ENABLE_PACKET_TRACING = True
ENABLE_EVENT_LOGGING = True
METRICS_EXPORT_PATH = './'
