## Configuration for Part 2: Cluster-Tree Network with Enhanced Tables
## Extends original config with Appendix B & C table timeout factors

# ============== NETWORK PROPERTIES ==============
BROADCAST_NET_ADDR = 255
BROADCAST_NODE_ADDR = 255

# ============== NODE PROPERTIES ==============
NODE_TX_RANGE = 100
NODE_ARRIVAL_MAX = 100

# ============== SIMULATION PROPERTIES ==============
SIM_NODE_COUNT = 100
SIM_NODE_PLACING_CELL_SIZE = 60
SIM_DURATION = 600
SIM_TIME_SCALE = 0.01
SIM_TERRAIN_SIZE = (600, 600)
SIM_TITLE = 'Part 2: Cluster-Tree with Enhanced Tables (Appendix B & C)'
SIM_VISUALIZATION = True
SCALE = 1

# ============== HEARTBEAT & TIMERS ==============
HEARTH_BEAT_TIME_INTERVAL = 20

# ============== TABLE TIMEOUT FACTORS ==============
NEIGHBOR_TIMEOUT_FACTOR = 3  # Neighbor stale after HEARTBEAT * this
MEMBER_TIMEOUT_FACTOR = 5    # Member orphaned after HEARTBEAT * this
CHILD_NET_TIMEOUT_FACTOR = 5 # Child net stale after HEARTBEAT * this

# ============== LOGGING & EXPORT ==============
ENABLE_LOGGING = True
EXPORT_TABLES = True
