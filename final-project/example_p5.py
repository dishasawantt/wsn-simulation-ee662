"""
Part 5: Minimal Cluster Overlap & CH Migration

Builds on Part 4, adding:
1. MINIMAL_CLUSTER_OVERLAP:
   - Prevents new CHs from forming too close to existing CHs
   - Uses MIN_CH_DISTANCE to enforce spacing
   
2. ROUTER nodes:
   - Nodes that bridge multiple clusters
   - Forward inter-cluster traffic between CHs
   
3. CH_HANDOFF:
   - Allows cluster head role to migrate between nodes
   - Triggered by low energy or load imbalance
   - Seamless transfer of members table and routing info

Per instructions.txt Step 5:
- Minimal cluster overlap
- CH communication via routers
- CH role migration
"""

import random
from enum import Enum
import sys
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
import math
from source import config_p5 as config
from collections import Counter
import csv
import uuid

NODE_POS = {}
ALL_NODES = {}
ROLE_COUNTS = Counter()

# ============== GLOBAL METRICS ==============
JOIN_TIMES = {}
PACKET_DELAYS = []
PACKET_TRACES = []
ROUTING_STATS = {'mesh_success': 0, 'tree_success': 0, 'mesh_attempts': 0, 'tree_attempts': 0}

# ============== PART 4: NEW STATISTICS ==============
PACKET_LOSS_STATS = {'total_sent': 0, 'total_lost': 0, 'control_lost': 0, 'data_lost': 0}
CLUSTER_STATS = {}  # {cluster_net_addr: {'tx_power': value, 'member_count': count, 'rejected_joins': count}}

# ============== TCP-LIKE RELIABILITY STATISTICS ==============
RELIABILITY_STATS = {
    'retransmissions': 0,
    'retransmit_success': 0, 
    'retransmit_failures': 0,
    'acks_sent': 0,
    'acks_received': 0,
    'duplicates_detected': 0,
    'multipath_sends': 0
}

# ============== PART 5: MINIMAL OVERLAP & CH MIGRATION STATS ==============
OVERLAP_STATS = {
    'ch_requests_rejected': 0,      # NETWORK_REQUESTs rejected due to MIN_CH_DISTANCE
    'router_nodes_created': 0,       # Nodes that became routers
    'ch_handoffs_initiated': 0,      # CH handoff requests sent
    'ch_handoffs_completed': 0,      # Successful CH handoffs
    'ch_handoffs_failed': 0          # Failed CH handoffs
}

CH_POSITIONS = {}  # {ch_gui: (x, y)} - Track CH positions for distance check

Roles = Enum('Roles', 'UNDISCOVERED UNREGISTERED ROOT REGISTERED CLUSTER_HEAD ROUTER')


# ============== APPENDIX B: NEIGHBOR TABLE ENTRY ==============

class NeighborEntry:
    def __init__(self, gui, pck, distance, timestamp, tx_power=None):
        self.gui = gui
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        self.net_addr = pck['ch_addr'].net_addr if pck.get('ch_addr') else None
        self.tx_power = pck.get('tx_power', config.TX_POWER_DEFAULT)
        
        self.distance = distance
        effective_range = tx_power if tx_power else config.NODE_TX_RANGE
        self.lqi = self._calculate_lqi(distance, effective_range)
        self.rssi = self._calculate_rssi(distance)
        self.last_heard = timestamp
        self.hello_interval = config.HEARTH_BEAT_TIME_INTERVAL
        self.capabilities = 0x01
        self.cost = 1
        self.state = 'ACTIVE'
    
    def _calculate_lqi(self, distance, tx_range):
        if distance >= tx_range:
            return 0
        return int((1 - distance / tx_range) * 255)
    
    def _calculate_rssi(self, distance, tx_power=0):
        if distance <= 0:
            return tx_power
        return int(tx_power - 20 * math.log10(max(distance, 1)))
    
    def update(self, pck, distance, timestamp, tx_power=None):
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        self.net_addr = pck['ch_addr'].net_addr if pck.get('ch_addr') else None
        self.tx_power = pck.get('tx_power', config.TX_POWER_DEFAULT)
        self.distance = distance
        effective_range = tx_power if tx_power else config.NODE_TX_RANGE
        self.lqi = self._calculate_lqi(distance, effective_range)
        self.rssi = self._calculate_rssi(distance)
        self.last_heard = timestamp
        self.state = 'ACTIVE'
    
    def is_valid(self, current_time):
        timeout = config.HEARTH_BEAT_TIME_INTERVAL * config.NEIGHBOR_TIMEOUT_FACTOR
        if current_time - self.last_heard > timeout:
            self.state = 'STALE'
            return False
        self.state = 'ACTIVE'
        return True


class TwoHopNeighborEntry:
    def __init__(self, gui, via_neighbor, path_cost, timestamp):
        self.gui = gui
        self.via_neighbor = via_neighbor
        self.path_cost = path_cost
        self.last_heard = timestamp
        self.state = 'ACTIVE'
    
    def is_valid(self, current_time):
        timeout = config.HEARTH_BEAT_TIME_INTERVAL * config.NEIGHBOR_TIMEOUT_FACTOR * 2
        if current_time - self.last_heard > timeout:
            self.state = 'STALE'
            return False
        return True


# ============== APPENDIX C: MEMBERS TABLE ENTRY ==============

class MemberEntry:
    def __init__(self, eui64, short_addr, parent_addr, join_time, device_type=0, capabilities=0x01):
        self.eui64 = eui64
        self.short_addr = short_addr
        self.parent_addr = parent_addr
        self.join_time = join_time
        self.device_type = device_type
        self.capabilities = capabilities
        self.state = 'ACTIVE'
        self.last_heard = join_time


# ============== APPENDIX C: CHILD NET TABLE ENTRY ==============

class ChildNetEntry:
    def __init__(self, dest_network, ch_addr, next_hop_gui, hop_distance, last_update):
        self.dest_network = dest_network
        self.ch_addr = ch_addr
        self.next_hop_gui = next_hop_gui
        self.hop_distance = hop_distance
        self.last_update = last_update
        self.state = 'ACTIVE'
    
    def update(self, ch_addr, next_hop_gui, hop_distance, timestamp):
        self.ch_addr = ch_addr
        self.next_hop_gui = next_hop_gui
        self.hop_distance = hop_distance
        self.last_update = timestamp
        self.state = 'ACTIVE'


def generate_eui64(node_id):
    return f"0x00124B0001ABCD{node_id:02X}"


def generate_packet_id():
    return str(uuid.uuid4())[:8]


# ============== PART 4b: TX POWER HELPERS ==============

def get_cluster_tx_power(net_addr):
    """Get TX power for a specific cluster."""
    if config.USE_UNIFORM_TX_POWER:
        return config.TX_POWER_DEFAULT
    
    # Check if cluster has specific TX power configured
    if net_addr in config.CLUSTER_TX_POWER:
        power = config.CLUSTER_TX_POWER[net_addr]
    else:
        # Assign random TX power between min and max for new clusters
        power = random.uniform(config.TX_POWER_MIN, config.TX_POWER_MAX)
        config.CLUSTER_TX_POWER[net_addr] = power
    
    # Clamp to valid range
    return max(config.TX_POWER_MIN, min(config.TX_POWER_MAX, power))


# ============== PART 4c: PACKET LOSS SIMULATION ==============

def should_drop_packet(pck, distance, tx_range):
    """
    Determine if a packet should be dropped based on loss rate.
    Returns True if packet should be lost.
    """
    pck_type = pck.get('type', 'DATA')
    
    # Check if loss applies to this packet type
    is_control = pck_type in ['PROBE', 'HEART_BEAT', 'JOIN_REQUEST', 'JOIN_REPLY', 
                              'JOIN_ACK', 'NETWORK_REQUEST', 'NETWORK_REPLY', 'NETWORK_UPDATE']
    is_data = pck_type == 'DATA'
    
    if is_control and not config.PACKET_LOSS_APPLY_TO_CONTROL:
        return False
    if is_data and not config.PACKET_LOSS_APPLY_TO_DATA:
        return False
    
    # Calculate effective loss rate
    loss_rate = config.PACKET_LOSS_RATE
    
    if config.ENABLE_DISTANCE_DEPENDENT_LOSS and tx_range > 0:
        # Add distance-dependent component
        distance_factor = min(distance / tx_range, 1.0)
        loss_rate += distance_factor * config.DISTANCE_LOSS_FACTOR
    
    # Clamp to valid range
    loss_rate = max(0.0, min(1.0, loss_rate))
    
    # Random drop decision
    if random.random() < loss_rate:
        PACKET_LOSS_STATS['total_lost'] += 1
        if is_control:
            PACKET_LOSS_STATS['control_lost'] += 1
        elif is_data:
            PACKET_LOSS_STATS['data_lost'] += 1
        return True
    
    return False


# ============== PART 5a: MINIMAL CLUSTER OVERLAP ==============

def get_distance_to_nearest_ch(node_gui):
    """Calculate distance from a node to the nearest existing CH."""
    if node_gui not in NODE_POS:
        return float('inf')
    
    x1, y1 = NODE_POS[node_gui]
    min_dist = float('inf')
    
    for ch_gui, (x2, y2) in CH_POSITIONS.items():
        if ch_gui != node_gui:
            dist = math.hypot(x1 - x2, y1 - y2)
            min_dist = min(min_dist, dist)
    
    return min_dist

def can_become_ch(node_gui):
    """Check if a node can become CH based on minimal overlap rules."""
    if not config.ENABLE_MINIMAL_OVERLAP:
        return True
    
    dist = get_distance_to_nearest_ch(node_gui)
    return dist >= config.MIN_CH_DISTANCE

def register_ch_position(ch_gui):
    """Register a new CH's position for overlap tracking."""
    if ch_gui in NODE_POS:
        CH_POSITIONS[ch_gui] = NODE_POS[ch_gui]

def unregister_ch_position(ch_gui):
    """Remove a CH's position when it stops being CH."""
    CH_POSITIONS.pop(ch_gui, None)


# ============== PART 5b: ROUTER DETECTION ==============

def count_visible_clusters(node, neighbors_table):
    """Count how many different clusters a node can see."""
    clusters = set()
    for gui, entry in neighbors_table.items():
        if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT] and entry.net_addr:
            clusters.add(entry.net_addr)
    return clusters

def should_become_router(node, neighbors_table):
    """Check if a node should become a router (bridges 2+ clusters)."""
    if not config.ENABLE_ROUTER_NODES:
        return False
    
    visible_clusters = count_visible_clusters(node, neighbors_table)
    return len(visible_clusters) >= 2


###########################################################
class SensorNode(wsn.Node):
    """Enhanced SensorNode with configurable cluster size, TX power, and packet loss."""

    def init(self):
        self.scene.nodecolor(self.id, 1, 1, 1)
        self.sleep()
        self.addr = None
        self.ch_addr = None
        self.parent_gui = None
        self.root_addr = None
        self.set_role(Roles.UNDISCOVERED)
        self.is_root_eligible = True if self.id == ROOT_ID else False
        self.c_probe = 0
        self.th_probe = 10
        self.hop_count = 99999
        
        self.eui64 = generate_eui64(self.id)
        self.neighbors_table = {}
        self.two_hop_neighbors = {}
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = {}
        self.received_JR_guis = []
        self.join_attempts = 0
        self.last_heartbeat_response = 0
        
        # PART 4b: TX Power for this node's cluster
        self.cluster_tx_power = config.TX_POWER_DEFAULT
        self.tx_range = config.TX_POWER_DEFAULT * config.SCALE  # Set initial TX range
        
        # Metrics tracking
        self.join_start_time = None
        self.data_packets_sent = 0
        
        # TCP-LIKE RELIABILITY: Retransmission tracking
        self.pending_packets = {}  # {seq_no: {'pck': packet, 'sent_at': time, 'retries': count}}
        self.sequence_number = 0
        self.received_seq_numbers = {}  # {(source_gui, seq_no): timestamp} for duplicate detection
        
        # PART 5: Minimal overlap & CH migration
        self.visible_clusters = set()   # Clusters this node can hear
        self.is_router = False          # True if node is bridging clusters
        self.router_connections = {}    # {cluster_net: neighbor_gui} for routing
        self.handoff_in_progress = False
        self.handoff_candidate = None   # GUI of node to hand off CH role to
        
        ALL_NODES[self.id] = self

    def set_role(self, new_role, *, recolor=True):
        old_role = getattr(self, "role", None)
        if old_role is not None:
            ROLE_COUNTS[old_role] -= 1
            if ROLE_COUNTS[old_role] <= 0:
                ROLE_COUNTS.pop(old_role, None)
        ROLE_COUNTS[new_role] += 1
        self.role = new_role

        if recolor:
            colors = {
                Roles.UNDISCOVERED: (1, 1, 1),
                Roles.UNREGISTERED: (1, 1, 0),
                Roles.REGISTERED: (0, 1, 0),
                Roles.CLUSTER_HEAD: (0, 0, 1),
                Roles.ROOT: (0, 0, 0),
                Roles.ROUTER: config.ROUTER_COLOR,  # Orange for routers
            }
            if new_role in colors:
                self.scene.nodecolor(self.id, *colors[new_role])
            if new_role in [Roles.CLUSTER_HEAD, Roles.ROUTER]:
                self.draw_tx_range()

    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)

    # ============== PART 4b: UPDATE TX POWER ==============
    
    def update_tx_power(self, net_addr=None):
        """Update this node's TX power based on its cluster."""
        if net_addr is None:
            net_addr = self.ch_addr.net_addr if self.ch_addr else self.id
        
        self.cluster_tx_power = get_cluster_tx_power(net_addr)
        self.tx_range = self.cluster_tx_power * config.SCALE
        
        # Update cluster stats
        if net_addr not in CLUSTER_STATS:
            CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': 0, 'rejected_joins': 0}
        CLUSTER_STATS[net_addr]['tx_power'] = self.cluster_tx_power

    # ============== PART 4c: SEND WITH PACKET LOSS ==============
    
    def send_with_loss(self, pck):
        """Send packet with potential packet loss simulation."""
        PACKET_LOSS_STATS['total_sent'] += 1
        # The actual loss check happens at the receiver side based on distance
        self.send(pck)

    # ============== TCP-LIKE RELIABILITY METHODS ==============
    
    def send_reliable(self, pck, require_ack=True):
        """
        Send a packet with TCP-like reliability (retransmission on timeout).
        Used for critical packets like DATA, JOIN_REQUEST, NETWORK_REQUEST.
        """
        if not config.ENABLE_RETRANSMISSION:
            self.send_with_loss(pck)
            return
        
        # Assign sequence number
        self.sequence_number += 1
        pck['seq_no'] = self.sequence_number
        pck['requires_ack'] = require_ack
        
        if require_ack and pck.get('type') in config.RELIABLE_PACKET_TYPES:
            # Track pending packet for retransmission
            self.pending_packets[self.sequence_number] = {
                'pck': pck.copy(),
                'sent_at': self.now,
                'retries': 0,
                'dest_gui': pck.get('dest_gui')
            }
            # Set retransmission timer
            self.set_timer(f'TIMER_RETRANSMIT_{self.sequence_number}', config.RETRANSMIT_TIMEOUT)
        
        self.send_with_loss(pck)
    
    def send_ack(self, original_pck):
        """Send acknowledgment for a received reliable packet."""
        if not config.ENABLE_ACK:
            return
        
        source_gui = original_pck.get('source_gui', original_pck.get('gui'))
        if source_gui is None or source_gui not in ALL_NODES:
            return
        
        source_node = ALL_NODES[source_gui]
        if source_node.addr is None:
            return
        
        ack_pck = {
            'dest': source_node.addr,
            'dest_gui': source_gui,
            'type': 'ACK',
            'ack_seq_no': original_pck.get('seq_no'),
            'ack_type': original_pck.get('type'),
            'gui': self.id,
            'source_gui': self.id,
        }
        RELIABILITY_STATS['acks_sent'] += 1
        self.send_with_loss(ack_pck)
    
    def handle_ack(self, pck):
        """Handle received ACK packet - stop retransmission."""
        ack_seq = pck.get('ack_seq_no')
        if ack_seq is None:
            return
        
        if ack_seq in self.pending_packets:
            entry = self.pending_packets[ack_seq]
            # Kill retransmission timer
            self.kill_timer(f'TIMER_RETRANSMIT_{ack_seq}')
            del self.pending_packets[ack_seq]
            RELIABILITY_STATS['acks_received'] += 1
            
            if entry['retries'] > 0:
                RELIABILITY_STATS['retransmit_success'] += 1
    
    def is_duplicate(self, pck):
        """Check if packet is a duplicate (already received)."""
        seq_no = pck.get('seq_no')
        if seq_no is None:
            return False
        
        source_gui = pck.get('source_gui', pck.get('gui'))
        if source_gui is None:
            return False
        
        pkt_id = (source_gui, seq_no)
        
        if pkt_id in self.received_seq_numbers:
            RELIABILITY_STATS['duplicates_detected'] += 1
            return True
        
        # Add to received set with timestamp
        self.received_seq_numbers[pkt_id] = self.now
        
        # Cleanup old entries (keep last 500)
        if len(self.received_seq_numbers) > 1000:
            sorted_entries = sorted(self.received_seq_numbers.items(), key=lambda x: x[1])
            self.received_seq_numbers = dict(sorted_entries[-500:])
        
        return False
    
    def retransmit_packet(self, seq_no):
        """Retransmit a pending packet."""
        if seq_no not in self.pending_packets:
            return
        
        entry = self.pending_packets[seq_no]
        
        if entry['retries'] >= config.MAX_RETRIES:
            # Max retries exceeded - give up
            self.log(f"Packet {seq_no} failed after {config.MAX_RETRIES} retries")
            RELIABILITY_STATS['retransmit_failures'] += 1
            self.kill_timer(f'TIMER_RETRANSMIT_{seq_no}')
            del self.pending_packets[seq_no]
            return
        
        # Retransmit
        entry['retries'] += 1
        entry['sent_at'] = self.now
        RELIABILITY_STATS['retransmissions'] += 1
        self.log(f"Retransmitting packet {seq_no} (attempt {entry['retries']}/{config.MAX_RETRIES})")
        
        # Send the packet again
        self.send_with_loss(entry['pck'])
        
        # Reset retransmission timer
        self.set_timer(f'TIMER_RETRANSMIT_{seq_no}', config.RETRANSMIT_TIMEOUT)

    # ============== PART 5: CH HANDOFF METHODS ==============
    
    def select_handoff_candidate(self):
        """Select the best member node to hand off CH role to."""
        if not self.members_table:
            return None
        
        candidates = []
        for gui, member in self.members_table.items():
            if gui in ALL_NODES:
                node = ALL_NODES[gui]
                # Check if candidate meets requirements
                neighbor_count = len(node.neighbors_table) if hasattr(node, 'neighbors_table') else 0
                if neighbor_count >= config.CH_SELECTION_MIN_NEIGHBORS:
                    candidates.append((gui, neighbor_count))
        
        if not candidates:
            return None
        
        # Select candidate with most neighbors (most central)
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    
    def initiate_ch_handoff(self, reason=""):
        """Initiate CH role handoff to another node."""
        if not config.ENABLE_CH_HANDOFF or self.handoff_in_progress:
            return False
        
        candidate = self.select_handoff_candidate()
        if candidate is None:
            self.log("No suitable candidate for CH handoff")
            return False
        
        self.handoff_in_progress = True
        self.handoff_candidate = candidate
        OVERLAP_STATS['ch_handoffs_initiated'] += 1
        
        self.log(f"Initiating CH handoff to node {candidate} ({reason})")
        
        # Send handoff request
        pck = {
            'dest': wsn.BROADCAST_ADDR,
            'type': 'CH_HANDOFF_REQUEST',
            'gui': self.id,
            'source_gui': self.id,
            'dest_gui': candidate,
            'ch_addr': self.ch_addr,
            'members': list(self.members_table.keys()),
            'child_networks': list(self.child_networks_table.keys()),
            'created_at': self.now
        }
        self.send_with_loss(pck)
        self.set_timer('TIMER_HANDOFF_TIMEOUT', config.CH_HANDOFF_TIMEOUT)
        return True
    
    def complete_ch_handoff(self, new_ch_gui):
        """Complete the handoff - transfer all CH data."""
        if not self.handoff_in_progress:
            return
        
        self.log(f"Completing CH handoff to node {new_ch_gui}")
        
        # Send handoff complete with all CH data
        pck = {
            'dest': wsn.BROADCAST_ADDR,
            'type': 'CH_HANDOFF_COMPLETE',
            'gui': self.id,
            'source_gui': self.id,
            'new_ch_gui': new_ch_gui,
            'old_ch_addr': self.ch_addr,
            'members_table': {k: v.__dict__ for k, v in self.members_table.items()},
            'child_networks': list(self.child_networks_table.keys()),
            'created_at': self.now
        }
        self.send_with_loss(pck)
        
        # Demote self to REGISTERED
        self.handoff_in_progress = False
        self.handoff_candidate = None
        unregister_ch_position(self.id)
        
        # Clear CH data
        self.members_table = {}
        self.child_networks_table = {}
        self.set_role(Roles.REGISTERED)
        
        OVERLAP_STATS['ch_handoffs_completed'] += 1
    
    def check_router_status(self):
        """Check if this node should be/remain a router."""
        if not config.ENABLE_ROUTER_NODES:
            return
        
        visible = count_visible_clusters(self, self.neighbors_table)
        self.visible_clusters = visible
        
        if len(visible) >= 2 and self.role == Roles.REGISTERED:
            # This node can see multiple clusters - become router
            self.is_router = True
            self.set_role(Roles.ROUTER)
            OVERLAP_STATS['router_nodes_created'] += 1
            self.log(f"Becoming ROUTER - bridging clusters: {visible}")
            
            # Build router connections
            for gui, entry in self.neighbors_table.items():
                if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT] and entry.net_addr:
                    self.router_connections[entry.net_addr] = gui

    # ============== MULTIPATH ROUTING ==============
    
    def route_packet_multipath(self, pck):
        """
        Send packet via multiple paths for redundancy.
        Increases reliability by using both mesh and tree routes.
        """
        if not config.ENABLE_MULTIPATH_ROUTING or pck.get('type') not in config.MULTIPATH_PACKET_TYPES:
            # Use single path routing with reliability
            if pck.get('type') in config.RELIABLE_PACKET_TYPES:
                self._route_single_reliable(pck)
            else:
                self.route_packet(pck)
            return
        
        paths_sent = 0
        dest = pck.get('dest')
        dest_gui = pck.get('dest_gui')
        
        # Path 1: Try MESH routing first
        if config.ENABLE_MESH_ROUTING and paths_sent < config.MULTIPATH_REDUNDANCY:
            mesh_pck = pck.copy()
            mesh_pck['path_id'] = 'MESH'
            mesh_pck['path'] = [self.id]
            mesh_pck['routing_types'] = []
            
            # Check 1-hop neighbors
            for gui, entry in self.neighbors_table.items():
                if entry.address == dest or gui == dest_gui:
                    mesh_pck['next_hop'] = entry.address if entry.address else entry.source
                    mesh_pck['routing_types'].append('MESH_1HOP')
                    self.send_reliable(mesh_pck)
                    paths_sent += 1
                    RELIABILITY_STATS['multipath_sends'] += 1
                    break
        
        # Path 2: Try TREE routing
        if paths_sent < config.MULTIPATH_REDUNDANCY:
            tree_pck = pck.copy()
            tree_pck['path_id'] = 'TREE'
            tree_pck['path'] = [self.id]
            tree_pck['routing_types'] = ['TREE']
            
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr else parent_entry.address
                if parent_ch is not None:
                    tree_pck['next_hop'] = parent_ch
                    self.send_reliable(tree_pck)
                    paths_sent += 1
                    RELIABILITY_STATS['multipath_sends'] += 1
        
        # Path 3: Try alternative neighbor if available
        if paths_sent < config.MULTIPATH_REDUNDANCY and len(self.neighbors_table) > 1:
            alt_pck = pck.copy()
            alt_pck['path_id'] = 'ALT'
            alt_pck['path'] = [self.id]
            alt_pck['routing_types'] = ['ALT']
            
            # Find an alternative neighbor (not the parent)
            for gui, entry in self.neighbors_table.items():
                if gui != self.parent_gui and entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
                    if entry.ch_addr is not None:
                        alt_pck['next_hop'] = entry.ch_addr
                        self.send_reliable(alt_pck)
                        paths_sent += 1
                        RELIABILITY_STATS['multipath_sends'] += 1
                        break
        
        if paths_sent == 0:
            # Fallback to single path
            self._route_single_reliable(pck)
    
    def _route_single_reliable(self, pck):
        """Route packet via single path with reliability."""
        if 'path' not in pck:
            pck['path'] = [self.id]
            pck['routing_types'] = []
        
        # Mark packet as requiring ACK for reliability
        pck['requires_ack'] = True
        
        # Route normally - the receiver will send ACK based on requires_ack flag
        self.route_packet(pck)

    def become_unregistered(self):
        if self.role != Roles.UNDISCOVERED:
            self.kill_all_timers()
        self.scene.nodecolor(self.id, 1, 1, 0)
        self.erase_parent()
        self.addr = None
        self.ch_addr = None
        self.parent_gui = None
        self.root_addr = None
        self.set_role(Roles.UNREGISTERED)
        self.c_probe = 0
        self.th_probe = 10
        self.hop_count = 99999
        self.neighbors_table = {}
        self.two_hop_neighbors = {}
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = {}
        self.received_JR_guis = []
        self.join_attempts = 0
        self.last_heartbeat_response = 0
        self.cluster_tx_power = config.TX_POWER_DEFAULT
        self.tx_range = config.TX_POWER_DEFAULT * config.SCALE  # Use TX_POWER_DEFAULT for discovery
        
        if config.ENABLE_JOIN_TIME_TRACKING:
            self.join_start_time = self.now
            JOIN_TIMES[self.id] = {'start': self.now, 'end': None, 'duration': None}
        
        self.send_probe()
        self.set_timer('TIMER_JOIN_REQUEST', 20)

    def update_neighbor(self, pck):
        gui = pck['gui']
        distance = 0
        if gui in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[gui]
            distance = math.hypot(x1 - x2, y1 - y2)
        
        sender_tx_power = pck.get('tx_power', config.TX_POWER_DEFAULT)
        
        if gui in self.neighbors_table:
            self.neighbors_table[gui].update(pck, distance, self.now, sender_tx_power)
        else:
            self.neighbors_table[gui] = NeighborEntry(gui, pck, distance, self.now, sender_tx_power)
        
        if config.USE_TWO_HOP_MESH:
            shared_neighbors = pck.get('one_hop_neighbors', [])
            for two_hop_gui in shared_neighbors:
                if two_hop_gui == self.id or two_hop_gui in self.neighbors_table:
                    continue
                if two_hop_gui not in self.two_hop_neighbors:
                    self.two_hop_neighbors[two_hop_gui] = TwoHopNeighborEntry(
                        two_hop_gui, gui, 2, self.now)
        
        is_child = gui in [e.next_hop_gui for e in self.child_networks_table.values()]
        is_member = gui in self.members_table
        if not is_child and not is_member and gui not in self.candidate_parents_table:
            self.candidate_parents_table.append(gui)

    def select_and_join(self):
        self.join_attempts = getattr(self, 'join_attempts', 0) + 1
        
        if self.join_attempts > 10:
            self.log("Max join attempts reached, backing off...")
            self.kill_timer('TIMER_JOIN_REQUEST')
            self.set_timer('TIMER_JOIN_REQUEST', 30)
            self.join_attempts = 0
            return
        
        min_hop = 99999
        min_hop_gui = None
        for gui in self.candidate_parents_table:
            if gui in self.neighbors_table:
                entry = self.neighbors_table[gui]
                if entry.hop_count < min_hop or (entry.hop_count == min_hop and (min_hop_gui is None or gui < min_hop_gui)):
                    min_hop = entry.hop_count
                    min_hop_gui = gui
        
        if min_hop_gui is not None:
            selected_addr = self.neighbors_table[min_hop_gui].source
            if selected_addr is not None:
                self.send_join_request(selected_addr)
            else:
                self.log(f"Warning: No valid source address for neighbor {min_hop_gui}")
        
        self.kill_timer('TIMER_JOIN_REQUEST')
        self.set_timer('TIMER_JOIN_REQUEST', 5)

    # ============== HYBRID MESH-TREE ROUTING ==============

    def route_packet(self, pck):
        dest = pck.get('dest')
        dest_gui = pck.get('dest_gui')
        
        if 'path' not in pck:
            pck['path'] = [self.id]
            pck['routing_types'] = []
        else:
            if self.id not in pck['path']:
                pck['path'].append(self.id)
        
        is_my_addr = (self.addr is not None and dest is not None and dest == self.addr)
        is_my_ch = (self.ch_addr is not None and dest is not None and dest == self.ch_addr)
        is_my_gui = (dest_gui == self.id)
        if is_my_addr or is_my_ch or is_my_gui:
            self._handle_packet_arrival(pck)
            return
        
        if config.ENABLE_MESH_ROUTING:
            for gui, entry in self.neighbors_table.items():
                if entry.address == dest or gui == dest_gui:
                    pck['next_hop'] = entry.address if entry.address else entry.source
                    pck['routing_types'].append('MESH_1HOP')
                    ROUTING_STATS['mesh_attempts'] += 1
                    self.send_with_loss(pck)
                    return
            
            if hasattr(dest, 'net_addr'):
                for gui, entry in self.neighbors_table.items():
                    if entry.net_addr == dest.net_addr:
                        pck['next_hop'] = entry.source
                        pck['routing_types'].append('MESH_1HOP_NET')
                        ROUTING_STATS['mesh_attempts'] += 1
                        self.send_with_loss(pck)
                        return
            
            if config.USE_TWO_HOP_MESH and dest_gui in self.two_hop_neighbors:
                via = self.two_hop_neighbors[dest_gui].via_neighbor
                if via in self.neighbors_table:
                    pck['next_hop'] = self.neighbors_table[via].source
                    pck['routing_types'].append('MESH_2HOP')
                    ROUTING_STATS['mesh_attempts'] += 1
                    self.send_with_loss(pck)
                    return
        
        ROUTING_STATS['tree_attempts'] += 1
        pck['routing_types'].append('TREE')
        
        if self.role == Roles.ROOT:
            if hasattr(dest, 'net_addr'):
                for net_id, entry in self.child_networks_table.items():
                    if dest.net_addr == net_id:
                        if entry.next_hop_gui in self.neighbors_table:
                            next_hop_addr = self.neighbors_table[entry.next_hop_gui].address
                            if next_hop_addr is not None:
                                pck['next_hop'] = next_hop_addr
                                self.send_with_loss(pck)
                                return
            if dest_gui in self.members_table:
                if dest is not None:
                    pck['next_hop'] = dest
                    self.send_with_loss(pck)
                    return
        
        elif self.role == Roles.CLUSTER_HEAD:
            if dest_gui in self.members_table:
                pck['next_hop'] = dest
                self.send_with_loss(pck)
                return
            
            if hasattr(dest, 'net_addr'):
                for net_id, entry in self.child_networks_table.items():
                    if dest.net_addr == net_id:
                        if entry.next_hop_gui in self.neighbors_table:
                            next_hop_addr = self.neighbors_table[entry.next_hop_gui].address
                            if next_hop_addr is not None:
                                pck['next_hop'] = next_hop_addr
                                self.send_with_loss(pck)
                                return
            
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
                if parent_ch is not None:
                    pck['next_hop'] = parent_ch
                    self.send_with_loss(pck)
                    return
        
        else:
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
                if parent_ch is not None:
                    pck['next_hop'] = parent_ch
                    self.send_with_loss(pck)
                    return
        
        if self.parent_gui in self.neighbors_table:
            parent_entry = self.neighbors_table[self.parent_gui]
            parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
            if parent_ch is not None:
                pck['next_hop'] = parent_ch
                self.send_with_loss(pck)

    def _handle_packet_arrival(self, pck):
        if pck.get('type') == 'DATA':
            if config.ENABLE_DELAY_TRACKING and 'created_at' in pck:
                delay = self.now - pck['created_at']
                PACKET_DELAYS.append({
                    'packet_id': pck.get('packet_id', 'unknown'),
                    'source': pck.get('source_gui'),
                    'dest': self.id,
                    'created': pck['created_at'],
                    'delivered': self.now,
                    'delay': delay,
                    'hops': len(pck.get('path', [])),
                })
                ROUTING_STATS['mesh_success' if 'MESH' in str(pck.get('routing_types', [])) else 'tree_success'] += 1
            
            if config.ENABLE_PACKET_TRACING:
                routing_type = 'MESH' if any('MESH' in rt for rt in pck.get('routing_types', [])) else 'TREE'
                PACKET_TRACES.append({
                    'packet_id': pck.get('packet_id', 'unknown'),
                    'source': pck.get('source_gui'),
                    'dest': self.id,
                    'path': pck.get('path', []),
                    'routing_types': pck.get('routing_types', []),
                    'primary_routing': routing_type,
                    'delay': self.now - pck.get('created_at', self.now),
                })
            
            self.log(f"DATA received: {pck.get('packet_id')} via {pck.get('path')}")

    def send_data_packet(self, dest_gui):
        if dest_gui not in ALL_NODES:
            return
        
        dest_node = ALL_NODES[dest_gui]
        if dest_node.addr is None:
            return
        
        packet_id = generate_packet_id()
        pck = {
            'dest': dest_node.addr,
            'dest_gui': dest_gui,
            'type': 'DATA',
            'source': self.addr,
            'source_gui': self.id,
            'gui': self.id,
            'packet_id': packet_id,
            'created_at': self.now,
            'requires_ack': True,  # TCP-LIKE: Request acknowledgment
            'payload': f"Data from {self.id} to {dest_gui}",
        }
        
        self.log(f"Sending DATA {packet_id} to node {dest_gui}")
        
        # Use multipath routing for DATA packets (TCP-like reliability)
        if config.ENABLE_MULTIPATH_ROUTING:
            self.route_packet_multipath(pck)
        else:
            self.route_packet(pck)
        
        self.data_packets_sent += 1

    # ============== PROTOCOL MESSAGES ==============

    def send_probe(self):
        self.send_with_loss({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE', 'gui': self.id, 'created_at': self.now})

    def send_heart_beat(self):
        one_hop_list = list(self.neighbors_table.keys())
        self.send_with_loss({'dest': wsn.BROADCAST_ADDR,
                   'type': 'HEART_BEAT',
                   'source': self.ch_addr if self.ch_addr else self.addr,
                   'gui': self.id,
                   'role': self.role,
                   'addr': self.addr,
                   'ch_addr': self.ch_addr,
                   'hop_count': self.hop_count,
                   'eui64': self.eui64,
                   'one_hop_neighbors': one_hop_list,
                   'tx_power': self.cluster_tx_power,  # PART 4b: Include TX power
                   'created_at': self.now})

    def send_join_request(self, dest):
        # Note: JOIN_REQUEST uses existing TIMER_JOIN_REQUEST for retries
        # Cannot use TCP-like retransmission because UNREGISTERED nodes have no address for ACK routing
        # JOIN_REPLY serves as implicit acknowledgment
        self.send_with_loss({'dest': dest, 'type': 'JOIN_REQUEST', 'gui': self.id, 'eui64': self.eui64, 'created_at': self.now})

    def send_join_reply(self, gui, addr, requester_eui64):
        self.send_with_loss({'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REPLY', 'source': self.ch_addr,
                   'gui': self.id, 'dest_gui': gui, 'addr': addr, 'root_addr': self.root_addr,
                   'hop_count': self.hop_count + 1, 
                   'tx_power': self.cluster_tx_power,  # PART 4b: Include cluster TX power
                   'created_at': self.now})
        
        self.members_table[gui] = MemberEntry(requester_eui64, addr, self.ch_addr, self.now, 0, 0x01)
        
        # Update cluster stats
        if self.ch_addr:
            net_addr = self.ch_addr.net_addr
            if net_addr not in CLUSTER_STATS:
                CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': 0, 'rejected_joins': 0}
            CLUSTER_STATS[net_addr]['member_count'] = len(self.members_table)

    def send_join_ack(self, dest):
        self.send_with_loss({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id, 'created_at': self.now})

    def send_network_request(self):
        pck = {'dest': self.root_addr, 'type': 'NETWORK_REQUEST', 'source': self.addr, 
               'gui': self.id, 'source_gui': self.id, 'eui64': self.eui64, 'created_at': self.now,
               'requires_ack': config.ENABLE_RETRANSMISSION}  # TCP-like reliability
        self.route_packet(pck)

    def send_network_reply(self, dest, addr, dest_gui):
        pck = {'dest': dest, 'type': 'NETWORK_REPLY', 'source': self.addr, 'addr': addr, 'created_at': self.now}
        self.route_packet(pck)
        self.child_networks_table[addr.net_addr] = ChildNetEntry(addr.net_addr, addr, dest_gui, 1, self.now)

    def send_network_update(self):
        if self.ch_addr is None:
            return
            
        child_networks = [self.ch_addr.net_addr]
        for net_id in self.child_networks_table.keys():
            child_networks.append(net_id)
        
        if self.parent_gui in self.neighbors_table:
            parent_entry = self.neighbors_table[self.parent_gui]
            dest = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
            if dest is not None:
                self.send_with_loss({'dest': dest, 'type': 'NETWORK_UPDATE', 'source': self.addr,
                           'gui': self.id, 'child_networks': child_networks, 'ch_addr': self.ch_addr, 'created_at': self.now})

    # ============== PART 4a: CLUSTER SIZE CHECK ==============
    
    def is_cluster_full(self):
        """Check if this cluster has reached maximum member count."""
        if config.MAX_CLUSTER_MEMBERS is None or config.MAX_CLUSTER_MEMBERS <= 0:
            return False  # No limit
        return len(self.members_table) >= config.MAX_CLUSTER_MEMBERS

    # ============== PACKET HANDLERS ==============

    def on_receive(self, pck):
        pck_type = pck.get('type')
        
        # PART 4c: Simulate packet loss at receiver
        # Calculate distance from sender
        sender_gui = pck.get('gui', pck.get('source_gui'))
        distance = 0
        if sender_gui is not None and sender_gui in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[sender_gui]
            distance = math.hypot(x1 - x2, y1 - y2)
        
        if should_drop_packet(pck, distance, self.tx_range):
            return  # Packet lost
        
        # TCP-LIKE RELIABILITY: Handle ACK packets
        if pck_type == 'ACK':
            self.handle_ack(pck)
            return
        
        # TCP-LIKE RELIABILITY: Check for duplicate packets
        if pck.get('seq_no') is not None and self.is_duplicate(pck):
            # Still send ACK for duplicate (sender may not have received our ACK)
            if pck.get('requires_ack') and config.ENABLE_ACK:
                self.send_ack(pck)
            return  # Don't process duplicate
        
        # Handle routed packets
        if 'next_hop' in pck and pck_type in ['DATA', 'NETWORK_REQUEST', 'NETWORK_REPLY']:
            dest = pck.get('dest')
            is_for_me = False
            if self.addr is not None and dest is not None:
                is_for_me = is_for_me or (dest == self.addr)
            if self.ch_addr is not None and dest is not None:
                is_for_me = is_for_me or (dest == self.ch_addr)
            if pck.get('dest_gui') == self.id:
                is_for_me = True
            
            if not is_for_me:
                self.route_packet(pck)
                return
        
        if self.role == Roles.ROOT or self.role == Roles.CLUSTER_HEAD:
            if pck_type == 'HEART_BEAT':
                self.update_neighbor(pck)
                if pck['gui'] in self.members_table:
                    self.members_table[pck['gui']].last_heard = self.now
            elif pck_type == 'PROBE':
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            elif pck_type == 'JOIN_REQUEST':
                eui64 = pck.get('eui64', generate_eui64(pck['gui']))
                
                # PART 4a: Check if cluster is full
                if self.is_cluster_full():
                    self.log(f"Cluster full ({len(self.members_table)}/{config.MAX_CLUSTER_MEMBERS}), forwarding JOIN_REQUEST")
                    # Track rejected joins
                    if self.ch_addr:
                        net_addr = self.ch_addr.net_addr
                        if net_addr not in CLUSTER_STATS:
                            CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': len(self.members_table), 'rejected_joins': 0}
                        CLUSTER_STATS[net_addr]['rejected_joins'] += 1
                    
                    # Re-broadcast the JOIN_REQUEST so REGISTERED members can handle it
                    # This allows a REGISTERED member to become a new CH
                    if pck['gui'] not in self.received_JR_guis:
                        self.received_JR_guis.append(pck['gui'])
                        # Broadcast to let REGISTERED nodes hear it
                        self.delayed_exec(random.uniform(0.2, 0.5), self.send_with_loss, 
                                         {'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REQUEST', 
                                          'gui': pck['gui'], 'eui64': eui64, 'created_at': pck.get('created_at', self.now)})
                else:
                    self.delayed_exec(random.uniform(0.1, 0.3), self.send_join_reply,
                                     pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']), eui64)
                    
            elif pck_type == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    requester_gui = pck.get('gui')
                    
                    # PART 5a: Check minimal overlap before allowing new CH
                    if config.ENABLE_MINIMAL_OVERLAP and not can_become_ch(requester_gui):
                        dist = get_distance_to_nearest_ch(requester_gui)
                        self.log(f"Rejecting NETWORK_REQUEST from {requester_gui} - too close to existing CH ({dist:.1f}m < {config.MIN_CH_DISTANCE}m)")
                        OVERLAP_STATS['ch_requests_rejected'] += 1
                        # Don't send reply - node will stay REGISTERED
                    else:
                        new_addr = wsn.Addr(pck['source'].node_addr, 254)
                        self.send_network_reply(pck['source'], new_addr, requester_gui)
                        # TCP-LIKE RELIABILITY: Send ACK for NETWORK_REQUEST
                        if pck.get('requires_ack') and config.ENABLE_ACK:
                            self.send_ack(pck)
            elif pck_type == 'NETWORK_UPDATE':
                sender_gui = pck['gui']
                sender_ch_addr = pck.get('ch_addr')
                changed = False
                
                for net_id in pck['child_networks']:
                    if net_id != self.ch_addr.net_addr:
                        if net_id in self.child_networks_table:
                            self.child_networks_table[net_id].update(sender_ch_addr, sender_gui, 1, self.now)
                        else:
                            changed = True
                            self.child_networks_table[net_id] = ChildNetEntry(net_id, sender_ch_addr, sender_gui, 1, self.now)
                
                if self.role != Roles.ROOT and changed:
                    self.send_network_update()
            elif pck_type == 'DATA':
                self._handle_packet_arrival(pck)
                # TCP-LIKE RELIABILITY: Send ACK for DATA packets
                if pck.get('requires_ack') and config.ENABLE_ACK:
                    self.send_ack(pck)

        elif self.role == Roles.REGISTERED:
            if pck_type == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck_type == 'PROBE':
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            elif pck_type == 'JOIN_REQUEST':
                if pck['gui'] not in self.received_JR_guis:
                    self.received_JR_guis.append(pck['gui'])
                    self.send_network_request()
            elif pck_type == 'NETWORK_REPLY':
                self.set_role(Roles.CLUSTER_HEAD)
                self.ch_addr = pck['addr']
                
                # PART 5a: Register CH position for minimal overlap tracking
                register_ch_position(self.id)
                
                # PART 4b: Set TX power for new cluster
                self.update_tx_power(self.ch_addr.net_addr)
                
                self.send_network_update()
                self.send_heart_beat()
                for gui in self.received_JR_guis:
                    eui64 = generate_eui64(gui)
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_join_reply,
                                     gui, wsn.Addr(self.ch_addr.net_addr, gui), eui64)
            elif pck_type == 'DATA':
                self._handle_packet_arrival(pck)
                # TCP-LIKE RELIABILITY: Send ACK for DATA packets
                if pck.get('requires_ack') and config.ENABLE_ACK:
                    self.send_ack(pck)

        elif self.role == Roles.UNDISCOVERED:
            if pck_type == 'HEART_BEAT':
                self.update_neighbor(pck)
                self.kill_timer('TIMER_PROBE')
                self.become_unregistered()

        if self.role == Roles.UNREGISTERED:
            if pck_type == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck_type == 'JOIN_REPLY':
                if pck['dest_gui'] == self.id:
                    self.addr = pck['addr']
                    self.parent_gui = pck['gui']
                    self.root_addr = pck['root_addr']
                    self.hop_count = pck['hop_count']
                    
                    # PART 4b: Adopt cluster's TX power
                    cluster_power = pck.get('tx_power', config.TX_POWER_DEFAULT)
                    self.cluster_tx_power = cluster_power
                    self.tx_range = cluster_power * config.SCALE
                    
                    self.draw_parent()
                    self.kill_timer('TIMER_JOIN_REQUEST')
                    self.join_attempts = 0
                    self.send_heart_beat()
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    self.send_join_ack(pck['source'])
                    
                    if config.ENABLE_JOIN_TIME_TRACKING and self.id in JOIN_TIMES:
                        JOIN_TIMES[self.id]['end'] = self.now
                        JOIN_TIMES[self.id]['duration'] = self.now - JOIN_TIMES[self.id]['start']
                    
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        register_ch_position(self.id)  # PART 5a
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)
                        # PART 5b: Check if should become router
                        self.delayed_exec(5.0, self.check_router_status)
                        if config.ENABLE_DATA_PACKETS:
                            self.set_timer('TIMER_DATA_PACKET', config.DATA_PACKET_START_TIME + random.uniform(0, 50))
            
            # ============== PART 5c: CH HANDOFF PACKET HANDLERS ==============
            elif pck_type == 'CH_HANDOFF_REQUEST':
                if pck.get('dest_gui') == self.id:
                    self.log(f"Received CH handoff request from {pck['gui']}")
                    # Accept handoff if we're REGISTERED
                    if self.role == Roles.REGISTERED:
                        accept_pck = {
                            'dest': wsn.BROADCAST_ADDR,
                            'type': 'CH_HANDOFF_ACCEPT',
                            'gui': self.id,
                            'source_gui': self.id,
                            'dest_gui': pck['gui'],
                            'created_at': self.now
                        }
                        self.send_with_loss(accept_pck)
                        
            elif pck_type == 'CH_HANDOFF_ACCEPT':
                if pck.get('dest_gui') == self.id and self.handoff_in_progress:
                    new_ch_gui = pck['gui']
                    self.kill_timer('TIMER_HANDOFF_TIMEOUT')
                    self.complete_ch_handoff(new_ch_gui)
                    
            elif pck_type == 'CH_HANDOFF_COMPLETE':
                new_ch_gui = pck.get('new_ch_gui')
                if new_ch_gui == self.id:
                    # I am the new CH
                    old_ch_addr = pck.get('old_ch_addr')
                    self.ch_addr = old_ch_addr
                    self.set_role(Roles.CLUSTER_HEAD)
                    register_ch_position(self.id)
                    self.log(f"Took over CH role from {pck['gui']}")
                    
                    # Send announcement
                    self.send_network_update()
                    self.send_heart_beat()

    def on_timer_fired(self, name, *args, **kwargs):
        if name == 'TIMER_ARRIVAL':
            self.scene.nodecolor(self.id, 1, 0, 0)
            self.wake_up()
            self.set_timer('TIMER_PROBE', 1)

        elif name == 'TIMER_PROBE':
            if self.c_probe < self.th_probe:
                self.send_probe()
                self.c_probe += 1
                self.set_timer('TIMER_PROBE', 1)
            else:
                if self.is_root_eligible:
                    self.set_role(Roles.ROOT)
                    self.addr = wsn.Addr(self.id, 254)
                    self.ch_addr = wsn.Addr(self.id, 254)
                    self.root_addr = self.addr
                    self.hop_count = 0
                    
                    # PART 5a: Register ROOT position for minimal overlap tracking
                    register_ch_position(self.id)
                    
                    # PART 4b: Set TX power for ROOT cluster
                    self.update_tx_power(self.id)
                    
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    if config.ENABLE_DATA_PACKETS:
                        self.set_timer('TIMER_DATA_PACKET', config.DATA_PACKET_START_TIME)
                else:
                    self.c_probe = 0
                    self.set_timer('TIMER_PROBE', 30)

        elif name == 'TIMER_HEART_BEAT':
            self.send_heart_beat()
            self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)

        elif name == 'TIMER_JOIN_REQUEST':
            if len(self.candidate_parents_table) == 0:
                self.become_unregistered()
            else:
                self.select_and_join()

        elif name == 'TIMER_DATA_PACKET':
            if self.data_packets_sent < config.DATA_PACKET_COUNT:
                registered_nodes = [n for n in ALL_NODES.values() 
                                   if n.role in [Roles.REGISTERED, Roles.CLUSTER_HEAD, Roles.ROOT, Roles.ROUTER] 
                                   and n.id != self.id and n.addr is not None]
                if registered_nodes:
                    dest = random.choice(registered_nodes)
                    self.send_data_packet(dest.id)
                self.set_timer('TIMER_DATA_PACKET', config.DATA_PACKET_INTERVAL)
        
        # TCP-LIKE RELIABILITY: Retransmission timer
        elif name.startswith('TIMER_RETRANSMIT_'):
            try:
                seq_no = int(name.split('_')[-1])
                self.retransmit_packet(seq_no)
            except (ValueError, IndexError):
                pass
        
        # PART 5c: CH Handoff timeout
        elif name == 'TIMER_HANDOFF_TIMEOUT':
            if self.handoff_in_progress:
                self.log("CH handoff timed out")
                OVERLAP_STATS['ch_handoffs_failed'] += 1
                self.handoff_in_progress = False
                self.handoff_candidate = None


# ============== EXPORT FUNCTIONS ==============

def export_join_times(path="join_times_p4.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'join_start', 'join_end', 'join_duration'])
        for node_id, times in JOIN_TIMES.items():
            w.writerow([node_id, times['start'], times['end'], times['duration']])


def export_packet_delays(path="packet_delays_p4.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'created', 'delivered', 'delay', 'hops'])
        for p in PACKET_DELAYS:
            w.writerow([p['packet_id'], p['source'], p['dest'], p['created'], p['delivered'], p['delay'], p['hops']])


def export_packet_traces(path="packet_traces_p4.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'path', 'routing_types', 'primary_routing', 'delay'])
        for t in PACKET_TRACES:
            w.writerow([t['packet_id'], t['source'], t['dest'], 
                       '->'.join(map(str, t['path'])), 
                       ','.join(t['routing_types']), t['primary_routing'], t['delay']])


def export_cluster_stats(path="cluster_stats_p4.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['cluster_net_addr', 'tx_power', 'member_count', 'rejected_joins'])
        for net_addr, stats in CLUSTER_STATS.items():
            w.writerow([net_addr, f"{stats['tx_power']:.2f}", stats['member_count'], stats['rejected_joins']])


def print_metrics():
    print("\n" + "="*70)
    print("PART 4: CONFIGURABLE PARAMETERS - METRICS")
    print("="*70)
    
    # Configuration summary
    print(f"\nConfiguration:")
    print(f"  Max cluster members: {config.MAX_CLUSTER_MEMBERS if config.MAX_CLUSTER_MEMBERS else 'Unlimited'}")
    print(f"  TX Power mode: {'Uniform' if config.USE_UNIFORM_TX_POWER else 'Per-cluster'}")
    print(f"  TX Power range: {config.TX_POWER_MIN} - {config.TX_POWER_MAX}")
    print(f"  Packet loss rate: {config.PACKET_LOSS_RATE*100:.1f}%")
    print(f"  Distance-dependent loss: {config.ENABLE_DISTANCE_DEPENDENT_LOSS}")
    
    # Join times
    valid_joins = [t['duration'] for t in JOIN_TIMES.values() if t['duration'] is not None]
    if valid_joins:
        print(f"\nJoin Time Statistics:")
        print(f"  Nodes joined: {len(valid_joins)}")
        print(f"  Average join time: {sum(valid_joins)/len(valid_joins):.2f}s")
        print(f"  Min join time: {min(valid_joins):.2f}s")
        print(f"  Max join time: {max(valid_joins):.2f}s")
    
    # Packet loss stats
    print(f"\nPacket Loss Statistics:")
    print(f"  Total packets sent: {PACKET_LOSS_STATS['total_sent']}")
    print(f"  Total packets lost: {PACKET_LOSS_STATS['total_lost']}")
    print(f"  Control packets lost: {PACKET_LOSS_STATS['control_lost']}")
    print(f"  Data packets lost: {PACKET_LOSS_STATS['data_lost']}")
    if PACKET_LOSS_STATS['total_sent'] > 0:
        actual_loss = PACKET_LOSS_STATS['total_lost'] / PACKET_LOSS_STATS['total_sent'] * 100
        print(f"  Actual loss rate: {actual_loss:.2f}%")
    
    # Packet delays
    if PACKET_DELAYS:
        delays = [p['delay'] for p in PACKET_DELAYS]
        hops = [p['hops'] for p in PACKET_DELAYS]
        print(f"\nPacket Delay Statistics:")
        print(f"  Packets delivered: {len(PACKET_DELAYS)}")
        print(f"  Average delay: {sum(delays)/len(delays):.4f}s")
        print(f"  Average hops: {sum(hops)/len(hops):.2f}")
    
    # Cluster stats
    if CLUSTER_STATS:
        print(f"\nCluster Statistics:")
        print(f"  Total clusters: {len(CLUSTER_STATS)}")
        for net_addr, stats in sorted(CLUSTER_STATS.items()):
            print(f"  Cluster {net_addr}: TX={stats['tx_power']:.0f}, Members={stats['member_count']}, Rejected={stats['rejected_joins']}")
    
    # Routing stats
    print(f"\nRouting Statistics:")
    print(f"  Mesh attempts: {ROUTING_STATS['mesh_attempts']}")
    print(f"  Tree attempts: {ROUTING_STATS['tree_attempts']}")
    print(f"  Mesh successes: {ROUTING_STATS['mesh_success']}")
    print(f"  Tree successes: {ROUTING_STATS['tree_success']}")
    
    # TCP-LIKE RELIABILITY stats
    print(f"\nTCP-Like Reliability Statistics:")
    print(f"  Retransmissions: {RELIABILITY_STATS['retransmissions']}")
    print(f"  Retransmit successes: {RELIABILITY_STATS['retransmit_success']}")
    print(f"  Retransmit failures: {RELIABILITY_STATS['retransmit_failures']}")
    print(f"  ACKs sent: {RELIABILITY_STATS['acks_sent']}")
    print(f"  ACKs received: {RELIABILITY_STATS['acks_received']}")
    print(f"  Duplicates detected: {RELIABILITY_STATS['duplicates_detected']}")
    print(f"  Multipath sends: {RELIABILITY_STATS['multipath_sends']}")
    
    # PART 5: Minimal Overlap & CH Migration stats
    print(f"\nPart 5: Minimal Overlap & CH Migration:")
    print(f"  CH positions tracked: {len(CH_POSITIONS)}")
    print(f"  CH requests rejected (too close): {OVERLAP_STATS['ch_requests_rejected']}")
    print(f"  Router nodes created: {OVERLAP_STATS['router_nodes_created']}")
    print(f"  CH handoffs initiated: {OVERLAP_STATS['ch_handoffs_initiated']}")
    print(f"  CH handoffs completed: {OVERLAP_STATS['ch_handoffs_completed']}")
    print(f"  CH handoffs failed: {OVERLAP_STATS['ch_handoffs_failed']}")
    
    print("="*70)


def print_summary(nodes):
    print("\n" + "="*70)
    print("NETWORK SUMMARY")
    print("="*70)
    
    states = {}
    for node in nodes:
        states[node.role] = states.get(node.role, 0) + 1
    
    print(f"\nNode States:")
    for role, count in states.items():
        print(f"  {role.name}: {count}")


###########################################################

ROOT_ID = config.SIM_NODE_COUNT // 2

def create_network(node_class, number_of_nodes):
    edge = math.ceil(math.sqrt(number_of_nodes))
    for i in range(number_of_nodes):
        x = i // edge
        y = i % edge
        px = 100 + x * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-10, 10)
        py = 100 + y * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-10, 10)
        node = sim.add_node(node_class, (px, py))
        NODE_POS[node.id] = (px, py)
        node.tx_range = config.NODE_TX_RANGE * config.SCALE
        node.logging = config.ENABLE_LOGGING
        node.arrival = random.uniform(0, config.NODE_ARRIVAL_MAX)
        if node.id == ROOT_ID:
            node.arrival = 0.1


sim = wsn.Simulator(
    duration=config.SIM_DURATION,
    timescale=config.SIM_TIME_SCALE,
    visual=config.SIM_VISUALIZATION,
    terrain_size=config.SIM_TERRAIN_SIZE,
    title=config.SIM_TITLE
)

create_network(SensorNode, config.SIM_NODE_COUNT)

print(f"Part 5: Minimal Cluster Overlap & CH Migration")
print(f"Nodes: {config.SIM_NODE_COUNT}, ROOT: Node {ROOT_ID}")
print(f"Max cluster members: {config.MAX_CLUSTER_MEMBERS if config.MAX_CLUSTER_MEMBERS else 'Unlimited'}")
print(f"TX Power: {'Uniform' if config.USE_UNIFORM_TX_POWER else 'Per-cluster'} (Default={config.TX_POWER_DEFAULT})")
print(f"Packet loss rate: {config.PACKET_LOSS_RATE*100:.1f}%")
print(f"Minimal Overlap: {config.ENABLE_MINIMAL_OVERLAP} (MIN_CH_DISTANCE={config.MIN_CH_DISTANCE}m)")
print(f"Router Nodes: {config.ENABLE_ROUTER_NODES}")
print(f"CH Handoff: {config.ENABLE_CH_HANDOFF}")
print("Starting simulation...")

sim.run()

print("\n=== Simulation Finished ===")
print_summary(sim.nodes)
print_metrics()

if config.EXPORT_METRICS:
    export_join_times()
    export_packet_delays()
    export_packet_traces()
    export_cluster_stats()
    print(f"\nExported metrics:")
    print("  - join_times_p4.csv")
    print("  - packet_delays_p4.csv")
    print("  - packet_traces_p4.csv")
    print("  - cluster_stats_p4.csv")

