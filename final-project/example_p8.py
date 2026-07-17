"""
Part 8: Modular Cluster Optimization Protocol

Refactored from Part 7 with modular config structure in configs_p8/.
Features:
- Cluster merging (7a)
- Load balancing (7b)  
- Energy-aware CH rotation (7c)
- Minimal cluster overlap (Part 5)
- Node failure/recovery (Part 6)
- TCP-like reliability (Part 4)
"""

import os
import sys
import math
import random
import csv
import uuid
from collections import Counter

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

from configs_p8.config import *
from configs_p8 import config as cfg
from source import wsnlab_vis as wsn

# ============== GLOBAL STATE ==============
NODE_POS = {}
ALL_NODES = {}
ROLE_COUNTS = Counter()
CH_POSITIONS = {}

# ============== GLOBAL METRICS ==============
JOIN_TIMES = {}
PACKET_DELAYS = []
PACKET_TRACES = []
ROUTING_STATS = {'mesh_success': 0, 'tree_success': 0, 'mesh_attempts': 0, 'tree_attempts': 0}
PACKET_LOSS_STATS = {'total_sent': 0, 'total_lost': 0, 'control_lost': 0, 'data_lost': 0}
CLUSTER_STATS = {}
RELIABILITY_STATS = {
    'retransmissions': 0, 'retransmit_success': 0, 'retransmit_failures': 0,
    'acks_sent': 0, 'acks_received': 0, 'duplicates_detected': 0, 'multipath_sends': 0
}
OVERLAP_STATS = {
    'ch_requests_rejected': 0, 'router_nodes_created': 0,
    'ch_handoffs_initiated': 0, 'ch_handoffs_completed': 0, 'ch_handoffs_failed': 0
}
FAILURE_STATS = {
    'nodes_killed': 0, 'nodes_recovered': 0, 'orphans_detected': 0,
    'orphans_recovered': 0, 'total_recovery_time': 0.0, 'max_orphan_count': 0
}
OPTIMIZATION_STATS = {
    'clusters_merged': 0, 'merge_attempts': 0, 'merge_failures': 0,
    'members_transferred': 0, 'load_balance_events': 0, 'ch_rotations': 0,
    'clusters_eliminated': 0, 'initial_cluster_count': 0, 'final_cluster_count': 0
}
CLUSTER_SIZES = {}
EVENT_LOG = []
DEAD_NODES = {}
ORPHAN_NODES = {}


# ============== TABLE ENTRY CLASSES ==============

class NeighborEntry:
    def __init__(self, gui, pck, distance, timestamp, tx_power=None):
        self.gui = gui
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        self.net_addr = pck['ch_addr'].net_addr if pck.get('ch_addr') else None
        self.tx_power = pck.get('tx_power', cfg.TX_POWER_DEFAULT)
        self.distance = distance
        effective_range = tx_power if tx_power else cfg.NODE_TX_RANGE
        self.lqi = self._calculate_lqi(distance, effective_range)
        self.rssi = self._calculate_rssi(distance)
        self.last_heard = timestamp
        self.hello_interval = cfg.HEARTH_BEAT_TIME_INTERVAL
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
        self.tx_power = pck.get('tx_power', cfg.TX_POWER_DEFAULT)
        self.distance = distance
        effective_range = tx_power if tx_power else cfg.NODE_TX_RANGE
        self.lqi = self._calculate_lqi(distance, effective_range)
        self.rssi = self._calculate_rssi(distance)
        self.last_heard = timestamp
        self.state = 'ACTIVE'

    def is_valid(self, current_time):
        timeout = cfg.HEARTH_BEAT_TIME_INTERVAL * cfg.NEIGHBOR_TIMEOUT_FACTOR
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
        timeout = cfg.HEARTH_BEAT_TIME_INTERVAL * cfg.NEIGHBOR_TIMEOUT_FACTOR * 2
        if current_time - self.last_heard > timeout:
            self.state = 'STALE'
            return False
        return True


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


# ============== HELPER FUNCTIONS ==============

def generate_eui64(node_id):
    return f"0x00124B0001ABCD{node_id:02X}"

def generate_packet_id():
    return str(uuid.uuid4())[:8]

def get_cluster_tx_power(net_addr):
    if cfg.USE_UNIFORM_TX_POWER:
        return cfg.TX_POWER_DEFAULT
    if net_addr in cfg.CLUSTER_TX_POWER:
        power = cfg.CLUSTER_TX_POWER[net_addr]
    else:
        power = random.uniform(cfg.TX_POWER_MIN, cfg.TX_POWER_MAX)
        cfg.CLUSTER_TX_POWER[net_addr] = power
    return max(cfg.TX_POWER_MIN, min(cfg.TX_POWER_MAX, power))

def should_drop_packet(pck, distance, tx_range):
    pck_type = pck.get('type', 'DATA')
    is_control = pck_type in ['PROBE', 'HEART_BEAT', 'JOIN_REQUEST', 'JOIN_REPLY',
                              'JOIN_ACK', 'NETWORK_REQUEST', 'NETWORK_REPLY', 'NETWORK_UPDATE']
    is_data = pck_type == 'DATA'
    if is_control and not cfg.PACKET_LOSS_APPLY_TO_CONTROL:
        return False
    if is_data and not cfg.PACKET_LOSS_APPLY_TO_DATA:
        return False
    loss_rate = cfg.PACKET_LOSS_RATE
    if cfg.ENABLE_DISTANCE_DEPENDENT_LOSS and tx_range > 0:
        distance_factor = min(distance / tx_range, 1.0)
        loss_rate += distance_factor * cfg.DISTANCE_LOSS_FACTOR
    loss_rate = max(0.0, min(1.0, loss_rate))
    if random.random() < loss_rate:
        PACKET_LOSS_STATS['total_lost'] += 1
        if is_control:
            PACKET_LOSS_STATS['control_lost'] += 1
        elif is_data:
            PACKET_LOSS_STATS['data_lost'] += 1
        return True
    return False

def get_distance_to_nearest_ch(node_gui, exclude_root=True):
    if node_gui not in NODE_POS:
        return float('inf')
    x1, y1 = NODE_POS[node_gui]
    min_dist = float('inf')
    root_id = cfg.SIM_NODE_COUNT // 2
    for ch_gui, (x2, y2) in CH_POSITIONS.items():
        if ch_gui == node_gui:
            continue
        if exclude_root and ch_gui == root_id:
            continue
        dist = math.hypot(x1 - x2, y1 - y2)
        min_dist = min(min_dist, dist)
    return min_dist

def can_become_ch(node_gui):
    if not cfg.ENABLE_MINIMAL_OVERLAP:
        return True
    dist = get_distance_to_nearest_ch(node_gui, exclude_root=True)
    return dist >= cfg.MIN_CH_DISTANCE

def register_ch_position(ch_gui):
    if ch_gui in NODE_POS:
        CH_POSITIONS[ch_gui] = NODE_POS[ch_gui]

def unregister_ch_position(ch_gui):
    CH_POSITIONS.pop(ch_gui, None)

def count_visible_clusters(node, neighbors_table):
    clusters = {}
    for gui, entry in neighbors_table.items():
        if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT] and entry.net_addr:
            clusters[entry.net_addr] = {'gui': gui, 'distance': entry.distance}
    return clusters

def should_become_router(node, neighbors_table, parent_gui):
    return False

def get_closest_candidate_for_router(ch_node):
    min_dist = float('inf')
    closest = None
    ch_x, ch_y = NODE_POS.get(ch_node.id, (0, 0))
    for member_gui in ch_node.members_table.keys():
        if member_gui in NODE_POS and member_gui in ALL_NODES:
            if ALL_NODES[member_gui].role == Roles.REGISTERED:
                x, y = NODE_POS[member_gui]
                dist = math.hypot(ch_x - x, ch_y - y)
                if dist < min_dist and dist > 20:
                    min_dist = dist
                    closest = member_gui
    for gui in NODE_POS:
        if gui in ALL_NODES and gui != ch_node.id:
            target = ALL_NODES[gui]
            if target.role in [Roles.UNREGISTERED, Roles.UNDISCOVERED]:
                x, y = NODE_POS[gui]
                dist = math.hypot(ch_x - x, ch_y - y)
                if dist <= cfg.NODE_TX_RANGE and dist < min_dist and dist > 20:
                    min_dist = dist
                    closest = gui
    return closest

def get_all_router_candidates(ch_node):
    candidates = []
    ch_x, ch_y = NODE_POS.get(ch_node.id, (0, 0))
    for member_gui in list(ch_node.members_table.keys()):
        if member_gui in NODE_POS and member_gui in ALL_NODES:
            if ALL_NODES[member_gui].role == Roles.REGISTERED:
                x, y = NODE_POS[member_gui]
                dist = math.hypot(ch_x - x, ch_y - y)
                if dist > 30:
                    has_undiscovered = False
                    for other_gui in NODE_POS:
                        if other_gui in ALL_NODES:
                            other = ALL_NODES[other_gui]
                            if other.role in [Roles.UNDISCOVERED, Roles.UNREGISTERED]:
                                ox, oy = NODE_POS[other_gui]
                                d = math.hypot(x - ox, y - oy)
                                if d <= cfg.NODE_TX_RANGE:
                                    has_undiscovered = True
                                    break
                    if has_undiscovered:
                        candidates.append(member_gui)
    return candidates

def get_farthest_unregistered_neighbor(node):
    max_dist = 0
    farthest = None
    for gui, entry in node.neighbors_table.items():
        if gui in ALL_NODES:
            target = ALL_NODES[gui]
            if target.role in [Roles.UNREGISTERED, Roles.UNDISCOVERED]:
                if entry.distance > max_dist:
                    max_dist = entry.distance
                    farthest = gui
    if farthest is None:
        for gui in NODE_POS:
            if gui in ALL_NODES and gui != node.id:
                target = ALL_NODES[gui]
                if target.role in [Roles.UNREGISTERED, Roles.UNDISCOVERED]:
                    if node.id in NODE_POS:
                        x1, y1 = NODE_POS[node.id]
                        x2, y2 = NODE_POS[gui]
                        dist = math.hypot(x1 - x2, y1 - y2)
                        if dist <= cfg.NODE_TX_RANGE and dist > max_dist:
                            max_dist = dist
                            farthest = gui
    return farthest


# ============== EVENT LOGGING ==============

def log_event(timestamp, event_type, node_id, details=""):
    EVENT_LOG.append({'timestamp': timestamp, 'event_type': event_type, 'node_id': node_id, 'details': details})
    if cfg.ENABLE_LOGGING:
        print(f"[{timestamp:.2f}] {event_type}: Node {node_id} - {details}")

def log_failure(timestamp, node_id, previous_role):
    if cfg.LOG_NODE_FAILURES:
        log_event(timestamp, 'NODE_FAILURE', node_id, f"Previous role: {previous_role.name}")
    FAILURE_STATS['nodes_killed'] += 1

def log_recovery(timestamp, node_id, recovery_time):
    if cfg.LOG_NODE_RECOVERIES:
        log_event(timestamp, 'NODE_RECOVERY', node_id, f"Recovery time: {recovery_time:.2f}s")
    FAILURE_STATS['nodes_recovered'] += 1
    FAILURE_STATS['total_recovery_time'] += recovery_time

def log_orphan(timestamp, node_id, parent_id):
    if cfg.LOG_ORPHAN_EVENTS:
        log_event(timestamp, 'ORPHAN_DETECTED', node_id, f"Lost parent: {parent_id}")
    FAILURE_STATS['orphans_detected'] += 1
    FAILURE_STATS['max_orphan_count'] = max(FAILURE_STATS['max_orphan_count'], len(ORPHAN_NODES))

def log_orphan_recovered(timestamp, node_id, new_parent_id):
    if cfg.LOG_ORPHAN_EVENTS:
        log_event(timestamp, 'ORPHAN_RECOVERED', node_id, f"New parent: {new_parent_id}")
    FAILURE_STATS['orphans_recovered'] += 1

def log_role_change(timestamp, node_id, old_role, new_role):
    if cfg.LOG_ROLE_CHANGES:
        old_name = old_role.name if old_role else 'None'
        log_event(timestamp, 'ROLE_CHANGE', node_id, f"{old_name} -> {new_role.name}")

def log_network_join(timestamp, node_id, parent_id, cluster_net):
    if cfg.LOG_NETWORK_JOINS:
        log_event(timestamp, 'NETWORK_JOIN', node_id, f"Parent: {parent_id}, Cluster: {cluster_net}")


# ============== SENSOR NODE CLASS ==============

class SensorNode(wsn.Node):

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
        self.cluster_tx_power = cfg.TX_POWER_DEFAULT
        self.tx_range = cfg.TX_POWER_DEFAULT * cfg.SCALE
        self.join_start_time = None
        self.data_packets_sent = 0
        self.pending_packets = {}
        self.sequence_number = 0
        self.received_seq_numbers = {}
        self.visible_clusters = set()
        self.is_router = False
        self.router_connections = {}
        self.router_bridges = {}
        self.handoff_in_progress = False
        self.handoff_candidate = None
        ALL_NODES[self.id] = self

    def set_role(self, new_role, *, recolor=True):
        old_role = getattr(self, "role", None)
        if old_role is not None:
            ROLE_COUNTS[old_role] -= 1
            if ROLE_COUNTS[old_role] <= 0:
                ROLE_COUNTS.pop(old_role, None)
        ROLE_COUNTS[new_role] += 1
        self.role = new_role
        if old_role is not None and old_role != new_role:
            log_role_change(self.now, self.id, old_role, new_role)
        if recolor:
            colors = {
                Roles.UNDISCOVERED: (1, 1, 1),
                Roles.UNREGISTERED: (1, 1, 0),
                Roles.REGISTERED: (0, 1, 0),
                Roles.CLUSTER_HEAD: (0, 0, 1),
                Roles.ROOT: (0, 0, 0),
                Roles.ROUTER: cfg.ROUTER_COLOR,
                Roles.DEAD: cfg.DEAD_NODE_COLOR,
            }
            if new_role in colors:
                self.scene.nodecolor(self.id, *colors[new_role])
            if new_role == Roles.CLUSTER_HEAD:
                self.draw_tx_range()

    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)

    def update_tx_power(self, net_addr=None):
        if net_addr is None:
            net_addr = self.ch_addr.net_addr if self.ch_addr else self.id
        self.cluster_tx_power = get_cluster_tx_power(net_addr)
        self.tx_range = self.cluster_tx_power * cfg.SCALE
        if net_addr not in CLUSTER_STATS:
            CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': 0, 'rejected_joins': 0}
        CLUSTER_STATS[net_addr]['tx_power'] = self.cluster_tx_power

    def send_with_loss(self, pck):
        PACKET_LOSS_STATS['total_sent'] += 1
        self.send(pck)

    def send_reliable(self, pck, require_ack=True):
        if not cfg.ENABLE_RETRANSMISSION:
            self.send_with_loss(pck)
            return
        self.sequence_number += 1
        pck['seq_no'] = self.sequence_number
        pck['requires_ack'] = require_ack
        if require_ack and pck.get('type') in cfg.RELIABLE_PACKET_TYPES:
            self.pending_packets[self.sequence_number] = {
                'pck': pck.copy(), 'sent_at': self.now, 'retries': 0, 'dest_gui': pck.get('dest_gui')
            }
            self.set_timer(f'TIMER_RETRANSMIT_{self.sequence_number}', cfg.RETRANSMIT_TIMEOUT)
        self.send_with_loss(pck)

    def send_ack(self, original_pck):
        if not cfg.ENABLE_ACK:
            return
        source_gui = original_pck.get('source_gui', original_pck.get('gui'))
        if source_gui is None or source_gui not in ALL_NODES:
            return
        source_node = ALL_NODES[source_gui]
        if source_node.addr is None:
            return
        ack_pck = {
            'dest': source_node.addr, 'dest_gui': source_gui, 'type': 'ACK',
            'ack_seq_no': original_pck.get('seq_no'), 'ack_type': original_pck.get('type'),
            'gui': self.id, 'source_gui': self.id,
        }
        RELIABILITY_STATS['acks_sent'] += 1
        self.send_with_loss(ack_pck)

    def handle_ack(self, pck):
        ack_seq = pck.get('ack_seq_no')
        if ack_seq is None:
            return
        if ack_seq in self.pending_packets:
            entry = self.pending_packets[ack_seq]
            self.kill_timer(f'TIMER_RETRANSMIT_{ack_seq}')
            del self.pending_packets[ack_seq]
            RELIABILITY_STATS['acks_received'] += 1
            if entry['retries'] > 0:
                RELIABILITY_STATS['retransmit_success'] += 1

    def is_duplicate(self, pck):
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
        self.received_seq_numbers[pkt_id] = self.now
        if len(self.received_seq_numbers) > 1000:
            sorted_entries = sorted(self.received_seq_numbers.items(), key=lambda x: x[1])
            self.received_seq_numbers = dict(sorted_entries[-500:])
        return False

    def retransmit_packet(self, seq_no):
        if seq_no not in self.pending_packets:
            return
        entry = self.pending_packets[seq_no]
        if entry['retries'] >= cfg.MAX_RETRIES:
            self.log(f"Packet {seq_no} failed after {cfg.MAX_RETRIES} retries")
            RELIABILITY_STATS['retransmit_failures'] += 1
            self.kill_timer(f'TIMER_RETRANSMIT_{seq_no}')
            del self.pending_packets[seq_no]
            return
        entry['retries'] += 1
        entry['sent_at'] = self.now
        RELIABILITY_STATS['retransmissions'] += 1
        self.send_with_loss(entry['pck'])
        self.set_timer(f'TIMER_RETRANSMIT_{seq_no}', cfg.RETRANSMIT_TIMEOUT)

    def select_handoff_candidate(self):
        if not self.members_table:
            return None
        candidates = []
        for gui, member in self.members_table.items():
            if gui in ALL_NODES:
                node = ALL_NODES[gui]
                neighbor_count = len(node.neighbors_table) if hasattr(node, 'neighbors_table') else 0
                if neighbor_count >= cfg.CH_SELECTION_MIN_NEIGHBORS:
                    candidates.append((gui, neighbor_count))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def initiate_ch_handoff(self, reason=""):
        if not cfg.ENABLE_CH_HANDOFF or self.handoff_in_progress:
            return False
        candidate = self.select_handoff_candidate()
        if candidate is None:
            return False
        self.handoff_in_progress = True
        self.handoff_candidate = candidate
        OVERLAP_STATS['ch_handoffs_initiated'] += 1
        pck = {
            'dest': wsn.BROADCAST_ADDR, 'type': 'CH_HANDOFF_REQUEST', 'gui': self.id,
            'source_gui': self.id, 'dest_gui': candidate, 'ch_addr': self.ch_addr,
            'members': list(self.members_table.keys()),
            'child_networks': list(self.child_networks_table.keys()), 'created_at': self.now
        }
        self.send_with_loss(pck)
        self.set_timer('TIMER_HANDOFF_TIMEOUT', cfg.CH_HANDOFF_TIMEOUT)
        return True

    def complete_ch_handoff(self, new_ch_gui):
        if not self.handoff_in_progress:
            return
        pck = {
            'dest': wsn.BROADCAST_ADDR, 'type': 'CH_HANDOFF_COMPLETE', 'gui': self.id,
            'source_gui': self.id, 'new_ch_gui': new_ch_gui, 'old_ch_addr': self.ch_addr,
            'members_table': {k: v.__dict__ for k, v in self.members_table.items()},
            'child_networks': list(self.child_networks_table.keys()), 'created_at': self.now
        }
        self.send_with_loss(pck)
        self.handoff_in_progress = False
        self.handoff_candidate = None
        unregister_ch_position(self.id)
        self.members_table = {}
        self.child_networks_table = {}
        self.ch_addr = None
        self.parent_gui = new_ch_gui
        self.set_role(Roles.REGISTERED)
        self.scene.nodecolor(self.id, 0, 0.5, 1)
        self.delayed_exec(5.0, self.check_router_status)
        self.set_timer('TIMER_ORPHAN_CHECK', cfg.ORPHAN_DETECTION_INTERVAL)
        self.set_timer('TIMER_HEART_BEAT', cfg.HEARTH_BEAT_TIME_INTERVAL)
        OVERLAP_STATS['ch_handoffs_completed'] += 1

    def check_router_status(self):
        pass

    def promote_closest_to_router(self):
        if self.role not in [Roles.ROOT, Roles.CLUSTER_HEAD]:
            return
        if not cfg.ENABLE_ROUTER_NODES:
            return
        if len(self.members_table) < 1:
            self.set_timer('TIMER_PROMOTE_ROUTER', 5.0)
            return
        candidates = get_all_router_candidates(self)
        ch_net = self.ch_addr.net_addr if self.ch_addr else self.id
        promoted = 0
        for cand_gui in candidates:
            if cand_gui in ALL_NODES:
                target = ALL_NODES[cand_gui]
                if target.role == Roles.REGISTERED:
                    self.send_with_loss({
                        'dest': wsn.BROADCAST_ADDR, 'type': 'BECOME_ROUTER',
                        'gui': self.id, 'target_gui': cand_gui, 'ch_gui': self.id,
                        'ch_net_addr': ch_net, 'created_at': self.now
                    })
                    if cand_gui in self.members_table:
                        del self.members_table[cand_gui]
                    promoted += 1
        if promoted == 0:
            self.set_timer('TIMER_PROMOTE_ROUTER', 10.0)

    def become_router_node(self, ch_gui, ch_net_addr):
        self.is_router = True
        self.set_role(Roles.ROUTER)
        OVERLAP_STATS['router_nodes_created'] += 1
        self.router_connections[ch_net_addr] = ch_gui
        self.scene.addlink(ch_gui, self.id, "router")
        self.set_timer('TIMER_FIND_NEW_CH', 5.0)

    def find_and_promote_new_ch(self):
        if self.role != Roles.ROUTER:
            return
        farthest = get_farthest_unregistered_neighbor(self)
        if farthest and farthest in ALL_NODES:
            target = ALL_NODES[farthest]
            if target.role == Roles.UNREGISTERED:
                self.router_connections[farthest] = farthest
                self.scene.addlink(self.id, farthest, "router")
                self.send_with_loss({
                    'dest': wsn.BROADCAST_ADDR, 'type': 'BECOME_CH',
                    'gui': self.id, 'target_gui': farthest,
                    'router_gui': self.id, 'created_at': self.now
                })
        else:
            self.set_timer('TIMER_FIND_NEW_CH', 5.0)

    def evaluate_cluster_efficiency(self):
        if self.role not in [Roles.CLUSTER_HEAD]:
            return None
        if not cfg.ENABLE_CLUSTER_MERGING:
            return None
        member_count = len(self.members_table)
        if member_count >= cfg.MIN_CLUSTER_SIZE:
            return None
        merge_candidates = []
        for gui, entry in self.neighbors_table.items():
            if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
                if gui in ALL_NODES:
                    target_node = ALL_NODES[gui]
                    target_size = len(target_node.members_table)
                    capacity = cfg.MAX_CLUSTER_MEMBERS - target_size if cfg.MAX_CLUSTER_MEMBERS else 999
                    if capacity >= member_count + 1:
                        merge_candidates.append({'gui': gui, 'distance': entry.distance, 'size': target_size, 'capacity': capacity})
        if not merge_candidates:
            return None
        if cfg.MERGE_PREFERENCE == 'NEAREST':
            merge_candidates.sort(key=lambda x: x['distance'])
        else:
            merge_candidates.sort(key=lambda x: x['size'], reverse=True)
        return merge_candidates[0]

    def request_cluster_merge(self, target_gui):
        if self.role not in [Roles.CLUSTER_HEAD]:
            return False
        OPTIMIZATION_STATS['merge_attempts'] += 1
        pck = {
            'dest': wsn.BROADCAST_ADDR, 'type': 'CLUSTER_MERGE_REQUEST', 'gui': self.id,
            'source_gui': self.id, 'dest_gui': target_gui, 'member_count': len(self.members_table),
            'members': list(self.members_table.keys()), 'ch_addr': self.ch_addr, 'created_at': self.now
        }
        self.send_with_loss(pck)
        self.set_timer('TIMER_MERGE_TIMEOUT', 15.0)
        return True

    def merge_cluster(self, new_ch_gui, new_ch_addr):
        for member_gui in list(self.members_table.keys()):
            pck = {
                'dest': wsn.BROADCAST_ADDR, 'type': 'CLUSTER_MERGE_NOTIFY', 'gui': self.id,
                'source_gui': self.id, 'dest_gui': member_gui, 'new_ch_gui': new_ch_gui,
                'new_ch_addr': new_ch_addr, 'created_at': self.now
            }
            self.send_with_loss(pck)
        unregister_ch_position(self.id)
        old_members = len(self.members_table)
        self.members_table = {}
        self.child_networks_table = {}
        OPTIMIZATION_STATS['clusters_merged'] += 1
        OPTIMIZATION_STATS['clusters_eliminated'] += 1
        OPTIMIZATION_STATS['members_transferred'] += old_members
        self.ch_addr = None
        self.set_role(Roles.UNREGISTERED)
        self.candidate_parents_table = [new_ch_gui] if new_ch_gui else []
        self.join_attempts = 0
        self.scene.nodecolor(self.id, 1, 1, 0)
        self.parent_gui = new_ch_gui
        if new_ch_addr:
            self.send_join_request(new_ch_addr)
            self.set_timer('TIMER_JOIN_REQUEST', 10)
        else:
            self.become_unregistered()

    def check_load_balance(self):
        if self.role not in [Roles.CLUSTER_HEAD, Roles.ROOT]:
            return None
        if not cfg.ENABLE_LOAD_BALANCING:
            return None
        my_size = len(self.members_table)
        neighbor_clusters = []
        for gui, entry in self.neighbors_table.items():
            if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT] and gui != self.id:
                if gui in ALL_NODES:
                    neighbor_size = len(ALL_NODES[gui].members_table)
                    neighbor_clusters.append({'gui': gui, 'size': neighbor_size, 'entry': entry})
        if not neighbor_clusters:
            return None
        all_sizes = [my_size] + [c['size'] for c in neighbor_clusters]
        avg_size = sum(all_sizes) / len(all_sizes)
        if my_size > avg_size * cfg.LOAD_BALANCE_THRESHOLD:
            neighbor_clusters.sort(key=lambda x: x['size'])
            for target in neighbor_clusters:
                size_diff = my_size - target['size']
                if size_diff >= cfg.LOAD_BALANCE_MIN_DIFF:
                    return target
        return None

    def transfer_members(self, target_gui, count):
        if not self.members_table:
            return
        members_list = list(self.members_table.keys())
        transfer_count = min(count, len(members_list))
        target_pos = NODE_POS.get(target_gui)
        if target_pos:
            def dist_to_target(gui):
                if gui not in NODE_POS:
                    return float('inf')
                x1, y1 = NODE_POS[gui]
                x2, y2 = target_pos
                return math.hypot(x1 - x2, y1 - y2)
            members_list.sort(key=dist_to_target)
        for i in range(transfer_count):
            member_gui = members_list[i]
            pck = {
                'dest': wsn.BROADCAST_ADDR, 'type': 'MEMBER_TRANSFER_REQUEST', 'gui': self.id,
                'source_gui': self.id, 'member_gui': member_gui, 'target_ch_gui': target_gui, 'created_at': self.now
            }
            self.send_with_loss(pck)
            if member_gui in self.members_table:
                del self.members_table[member_gui]
        OPTIMIZATION_STATS['members_transferred'] += transfer_count
        OPTIMIZATION_STATS['load_balance_events'] += 1

    def check_ch_rotation(self):
        if self.role not in [Roles.CLUSTER_HEAD]:
            return None
        if not cfg.ENABLE_CH_ROTATION:
            return None
        candidates = []
        for gui, member in self.members_table.items():
            if gui in ALL_NODES:
                node = ALL_NODES[gui]
                neighbor_count = len(node.neighbors_table) if hasattr(node, 'neighbors_table') else 0
                if neighbor_count >= cfg.CH_SELECTION_MIN_NEIGHBORS:
                    candidates.append({'gui': gui, 'neighbors': neighbor_count})
        if not candidates:
            return None
        if cfg.PREFER_HIGH_DEGREE_CH:
            candidates.sort(key=lambda x: x['neighbors'], reverse=True)
        my_neighbors = len(self.neighbors_table)
        best = candidates[0]
        if best['neighbors'] > my_neighbors + 1:
            return best['gui']
        return None

    def initiate_ch_rotation(self, new_ch_gui):
        if self.handoff_in_progress:
            return False
        OPTIMIZATION_STATS['ch_rotations'] += 1
        self.handoff_in_progress = True
        self.handoff_candidate = new_ch_gui
        pck = {
            'dest': wsn.BROADCAST_ADDR, 'type': 'CH_HANDOFF_REQUEST', 'gui': self.id,
            'source_gui': self.id, 'dest_gui': new_ch_gui, 'ch_addr': self.ch_addr,
            'members': list(self.members_table.keys()),
            'child_networks': list(self.child_networks_table.keys()), 'reason': 'OPTIMIZATION', 'created_at': self.now
        }
        self.send_with_loss(pck)
        self.set_timer('TIMER_HANDOFF_TIMEOUT', cfg.CH_HANDOFF_TIMEOUT)
        return True

    def run_cluster_optimization(self):
        if self.role not in [Roles.CLUSTER_HEAD]:
            return
        if not cfg.ENABLE_CLUSTER_OPTIMIZATION:
            return
        if self.ch_addr:
            CLUSTER_SIZES[self.ch_addr.net_addr] = len(self.members_table)
        merge_target = self.evaluate_cluster_efficiency()
        if merge_target:
            self.request_cluster_merge(merge_target['gui'])
            return
        balance_target = self.check_load_balance()
        if balance_target:
            self.transfer_members(balance_target['gui'], cfg.MEMBER_TRANSFER_COUNT)
            return
        rotation_target = self.check_ch_rotation()
        if rotation_target:
            self.initiate_ch_rotation(rotation_target)

    def kill_node(self):
        if self.role == Roles.DEAD:
            return
        previous_role = self.role
        log_failure(self.now, self.id, previous_role)
        DEAD_NODES[self.id] = {
            'killed_at': self.now, 'recover_at': self.now + cfg.FAILURE_DURATION,
            'previous_role': previous_role, 'previous_addr': self.addr,
            'previous_ch_addr': self.ch_addr, 'previous_parent': self.parent_gui
        }
        if previous_role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
            unregister_ch_position(self.id)
            for member_gui in self.members_table.keys():
                if member_gui in ALL_NODES:
                    ALL_NODES[member_gui].become_orphan(self.id)
        for timer_name in ['TIMER_PROBE', 'TIMER_JOIN_REQUEST', 'TIMER_HEART_BEAT', 'TIMER_DATA_PACKET', 'TIMER_ORPHAN_CHECK']:
            self.kill_timer(timer_name)
        self.addr = None
        self.ch_addr = None
        self.parent_gui = None
        self.members_table = {}
        self.child_networks_table = {}
        self.neighbors_table = {}
        self.set_role(Roles.DEAD)
        self.set_timer('TIMER_RECOVER', cfg.FAILURE_DURATION)

    def recover_node(self):
        if self.role != Roles.DEAD:
            return
        if self.id in DEAD_NODES:
            killed_at = DEAD_NODES[self.id]['killed_at']
            recovery_time = self.now - killed_at
            log_recovery(self.now, self.id, recovery_time)
            del DEAD_NODES[self.id]
        self.set_role(Roles.UNDISCOVERED)
        self.wake_up()
        self.set_timer('TIMER_PROBE', 1)

    def become_orphan(self, dead_parent_gui):
        if self.role == Roles.DEAD:
            return
        log_orphan(self.now, self.id, dead_parent_gui)
        ORPHAN_NODES[self.id] = {'orphaned_at': self.now, 'previous_parent': dead_parent_gui}
        self.parent_gui = None
        if cfg.ENABLE_NETWORK_REPAIR:
            self.delayed_exec(cfg.REPAIR_DELAY, self.attempt_repair)

    def attempt_repair(self):
        if self.id not in ORPHAN_NODES:
            return
        previous_parent = ORPHAN_NODES[self.id]['previous_parent']
        for gui, entry in self.neighbors_table.items():
            if entry.role in [Roles.CLUSTER_HEAD, Roles.ROOT] and gui != previous_parent:
                self.parent_gui = gui
                self.set_role(Roles.UNREGISTERED)
                dest_addr = entry.ch_addr if entry.ch_addr else entry.address
                if dest_addr:
                    self.send_join_request(dest_addr)
                    self.set_timer('TIMER_JOIN_REQUEST', 20)
                return
        if cfg.BROADCAST_ORPHAN_STATUS:
            pck = {
                'dest': wsn.BROADCAST_ADDR, 'type': 'I_AM_ORPHAN', 'gui': self.id,
                'source_gui': self.id, 'addr': self.addr, 'created_at': self.now
            }
            self.send_with_loss(pck)
        self.become_unregistered()

    def check_parent_alive(self):
        if self.role not in [Roles.REGISTERED, Roles.ROUTER]:
            return True
        if self.parent_gui is None:
            return True
        if self.parent_gui not in self.neighbors_table:
            return False
        neighbor = self.neighbors_table[self.parent_gui]
        time_since_heard = self.now - neighbor.last_heard
        if time_since_heard > cfg.PARENT_TIMEOUT:
            return False
        return True

    def route_packet_multipath(self, pck):
        if not cfg.ENABLE_MULTIPATH_ROUTING or pck.get('type') not in cfg.MULTIPATH_PACKET_TYPES:
            if pck.get('type') in cfg.RELIABLE_PACKET_TYPES:
                self._route_single_reliable(pck)
            else:
                self.route_packet(pck)
            return
        paths_sent = 0
        dest = pck.get('dest')
        dest_gui = pck.get('dest_gui')
        if cfg.ENABLE_MESH_ROUTING and paths_sent < cfg.MULTIPATH_REDUNDANCY:
            mesh_pck = pck.copy()
            mesh_pck['path_id'] = 'MESH'
            mesh_pck['path'] = [self.id]
            mesh_pck['routing_types'] = []
            for gui, entry in self.neighbors_table.items():
                if entry.address == dest or gui == dest_gui:
                    mesh_pck['next_hop'] = entry.address if entry.address else entry.source
                    mesh_pck['routing_types'].append('MESH_1HOP')
                    self.send_reliable(mesh_pck)
                    paths_sent += 1
                    RELIABILITY_STATS['multipath_sends'] += 1
                    break
        if paths_sent < cfg.MULTIPATH_REDUNDANCY:
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
        if paths_sent == 0:
            self._route_single_reliable(pck)

    def _route_single_reliable(self, pck):
        if 'path' not in pck:
            pck['path'] = [self.id]
            pck['routing_types'] = []
        pck['requires_ack'] = True
        self.route_packet(pck)

    def become_unregistered(self):
        if self.role != Roles.UNDISCOVERED:
            self.kill_all_timers()
        self.scene.nodecolor(self.id, 1, 1, 0)
        try:
            self.erase_parent()
        except (KeyError, Exception):
            pass
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
        self.cluster_tx_power = cfg.TX_POWER_DEFAULT
        self.tx_range = cfg.TX_POWER_DEFAULT * cfg.SCALE
        if cfg.ENABLE_JOIN_TIME_TRACKING:
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
        sender_tx_power = pck.get('tx_power', cfg.TX_POWER_DEFAULT)
        if gui in self.neighbors_table:
            self.neighbors_table[gui].update(pck, distance, self.now, sender_tx_power)
        else:
            self.neighbors_table[gui] = NeighborEntry(gui, pck, distance, self.now, sender_tx_power)
        if cfg.USE_TWO_HOP_MESH:
            shared_neighbors = pck.get('one_hop_neighbors', [])
            for two_hop_gui in shared_neighbors:
                if two_hop_gui == self.id or two_hop_gui in self.neighbors_table:
                    continue
                if two_hop_gui not in self.two_hop_neighbors:
                    self.two_hop_neighbors[two_hop_gui] = TwoHopNeighborEntry(two_hop_gui, gui, 2, self.now)
        is_child = gui in [e.next_hop_gui for e in self.child_networks_table.values()]
        is_member = gui in self.members_table
        if not is_child and not is_member and gui not in self.candidate_parents_table:
            self.candidate_parents_table.append(gui)

    def select_and_join(self):
        self.join_attempts = getattr(self, 'join_attempts', 0) + 1
        if self.join_attempts > 10:
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
        self.kill_timer('TIMER_JOIN_REQUEST')
        self.set_timer('TIMER_JOIN_REQUEST', 5)

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
        if cfg.ENABLE_MESH_ROUTING:
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
            if cfg.USE_TWO_HOP_MESH and dest_gui in self.two_hop_neighbors:
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
                for router_gui, bridge_info in self.router_bridges.items():
                    if dest.net_addr in bridge_info.get('bridged_networks', []):
                        if router_gui in self.neighbors_table:
                            router_entry = self.neighbors_table[router_gui]
                            router_addr = router_entry.address if router_entry.address else router_entry.source
                            if router_addr is not None:
                                pck['next_hop'] = router_addr
                                pck['routing_types'].append('ROUTER_BRIDGE')
                                self.send_with_loss(pck)
                                return
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
                if parent_ch is not None:
                    pck['next_hop'] = parent_ch
                    self.send_with_loss(pck)
                    return
        elif self.role == Roles.ROUTER:
            if hasattr(dest, 'net_addr'):
                for net_addr, ch_gui in self.router_connections.items():
                    if dest.net_addr == net_addr:
                        if ch_gui in self.neighbors_table:
                            ch_entry = self.neighbors_table[ch_gui]
                            ch_addr = ch_entry.ch_addr if ch_entry.ch_addr else ch_entry.address
                            if ch_addr is not None:
                                pck['next_hop'] = ch_addr
                                pck['routing_types'].append('ROUTER_FORWARD')
                                self.send_with_loss(pck)
                                return
            for net_addr, ch_gui in self.router_connections.items():
                if ch_gui in self.neighbors_table:
                    ch_entry = self.neighbors_table[ch_gui]
                    ch_addr = ch_entry.ch_addr if ch_entry.ch_addr else ch_entry.address
                    if ch_addr is not None:
                        pck['next_hop'] = ch_addr
                        pck['routing_types'].append('ROUTER_DEFAULT')
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
            if cfg.ENABLE_DELAY_TRACKING and 'created_at' in pck:
                delay = self.now - pck['created_at']
                PACKET_DELAYS.append({
                    'packet_id': pck.get('packet_id', 'unknown'), 'source': pck.get('source_gui'),
                    'dest': self.id, 'created': pck['created_at'], 'delivered': self.now,
                    'delay': delay, 'hops': len(pck.get('path', [])),
                })
                ROUTING_STATS['mesh_success' if 'MESH' in str(pck.get('routing_types', [])) else 'tree_success'] += 1
            if cfg.ENABLE_PACKET_TRACING:
                routing_type = 'MESH' if any('MESH' in rt for rt in pck.get('routing_types', [])) else 'TREE'
                PACKET_TRACES.append({
                    'packet_id': pck.get('packet_id', 'unknown'), 'source': pck.get('source_gui'),
                    'dest': self.id, 'path': pck.get('path', []), 'routing_types': pck.get('routing_types', []),
                    'primary_routing': routing_type, 'delay': self.now - pck.get('created_at', self.now),
                })

    def send_data_packet(self, dest_gui):
        if dest_gui not in ALL_NODES:
            return
        dest_node = ALL_NODES[dest_gui]
        if dest_node.addr is None:
            return
        packet_id = generate_packet_id()
        pck = {
            'dest': dest_node.addr, 'dest_gui': dest_gui, 'type': 'DATA', 'source': self.addr,
            'source_gui': self.id, 'gui': self.id, 'packet_id': packet_id, 'created_at': self.now,
            'requires_ack': True, 'payload': f"Data from {self.id} to {dest_gui}",
        }
        if cfg.ENABLE_MULTIPATH_ROUTING:
            self.route_packet_multipath(pck)
        else:
            self.route_packet(pck)
        self.data_packets_sent += 1

    def send_probe(self):
        self.send_with_loss({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE', 'gui': self.id, 'created_at': self.now})

    def send_router_register(self, ch_gui, ch_net_addr):
        if ch_gui not in self.neighbors_table:
            return
        ch_entry = self.neighbors_table[ch_gui]
        dest_addr = ch_entry.ch_addr if ch_entry.ch_addr else ch_entry.address
        if dest_addr is None:
            return
        bridged_nets = [net for net in self.router_connections.keys() if net != ch_net_addr]
        self.send_with_loss({
            'dest': dest_addr, 'type': 'ROUTER_REGISTER', 'gui': self.id,
            'router_id': self.id, 'bridged_networks': bridged_nets,
            'created_at': self.now
        })

    def send_heart_beat(self):
        one_hop_list = list(self.neighbors_table.keys())
        self.send_with_loss({
            'dest': wsn.BROADCAST_ADDR, 'type': 'HEART_BEAT',
            'source': self.ch_addr if self.ch_addr else self.addr, 'gui': self.id, 'role': self.role,
            'addr': self.addr, 'ch_addr': self.ch_addr, 'hop_count': self.hop_count, 'eui64': self.eui64,
            'one_hop_neighbors': one_hop_list, 'tx_power': self.cluster_tx_power, 'created_at': self.now
        })

    def send_join_request(self, dest):
        if dest is None:
            return
        self.send_with_loss({'dest': dest, 'type': 'JOIN_REQUEST', 'gui': self.id, 'eui64': self.eui64, 'created_at': self.now})

    def send_join_reply(self, gui, addr, requester_eui64):
        self.send_with_loss({
            'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REPLY', 'source': self.ch_addr,
            'gui': self.id, 'dest_gui': gui, 'addr': addr, 'root_addr': self.root_addr,
            'hop_count': self.hop_count + 1, 'tx_power': self.cluster_tx_power, 'created_at': self.now
        })
        self.members_table[gui] = MemberEntry(requester_eui64, addr, self.ch_addr, self.now, 0, 0x01)
        if self.ch_addr:
            net_addr = self.ch_addr.net_addr
            if net_addr not in CLUSTER_STATS:
                CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': 0, 'rejected_joins': 0}
            CLUSTER_STATS[net_addr]['member_count'] = len(self.members_table)

    def send_join_ack(self, dest):
        if dest is None or self.addr is None:
            return
        self.send_with_loss({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id, 'created_at': self.now})

    def send_network_request(self):
        if self.root_addr is None:
            return
        pck = {
            'dest': self.root_addr, 'type': 'NETWORK_REQUEST', 'source': self.addr, 'gui': self.id,
            'source_gui': self.id, 'eui64': self.eui64, 'created_at': self.now,
            'requires_ack': cfg.ENABLE_RETRANSMISSION
        }
        self.route_packet(pck)

    def send_network_reply(self, dest, addr, dest_gui):
        if dest is None or addr is None:
            return
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
                self.send_with_loss({
                    'dest': dest, 'type': 'NETWORK_UPDATE', 'source': self.addr, 'gui': self.id,
                    'child_networks': child_networks, 'ch_addr': self.ch_addr, 'created_at': self.now
                })

    def is_cluster_full(self):
        if cfg.MAX_CLUSTER_MEMBERS is None or cfg.MAX_CLUSTER_MEMBERS <= 0:
            return False
        return len(self.members_table) >= cfg.MAX_CLUSTER_MEMBERS

    def on_receive(self, pck):
        pck_type = pck.get('type')
        sender_gui = pck.get('gui', pck.get('source_gui'))
        distance = 0
        if sender_gui is not None and sender_gui in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[sender_gui]
            distance = math.hypot(x1 - x2, y1 - y2)
        if should_drop_packet(pck, distance, self.tx_range):
            return
        if pck_type == 'ACK':
            self.handle_ack(pck)
            return
        if pck.get('seq_no') is not None and self.is_duplicate(pck):
            if pck.get('requires_ack') and cfg.ENABLE_ACK:
                self.send_ack(pck)
            return
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
                if self.is_cluster_full():
                    if self.ch_addr:
                        net_addr = self.ch_addr.net_addr
                        if net_addr not in CLUSTER_STATS:
                            CLUSTER_STATS[net_addr] = {'tx_power': self.cluster_tx_power, 'member_count': len(self.members_table), 'rejected_joins': 0}
                        CLUSTER_STATS[net_addr]['rejected_joins'] += 1
                    if pck['gui'] not in self.received_JR_guis:
                        self.received_JR_guis.append(pck['gui'])
                        self.delayed_exec(random.uniform(0.2, 0.5), self.send_with_loss,
                            {'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REQUEST', 'gui': pck['gui'], 'eui64': eui64, 'created_at': pck.get('created_at', self.now)})
                else:
                    self.delayed_exec(random.uniform(0.1, 0.3), self.send_join_reply, pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']), eui64)
            elif pck_type == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    requester_gui = pck.get('gui')
                    if cfg.ENABLE_MINIMAL_OVERLAP and not can_become_ch(requester_gui):
                        OVERLAP_STATS['ch_requests_rejected'] += 1
                    else:
                        new_addr = wsn.Addr(pck['source'].node_addr, 254)
                        self.send_network_reply(pck['source'], new_addr, requester_gui)
                        if pck.get('requires_ack') and cfg.ENABLE_ACK:
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
                if pck.get('requires_ack') and cfg.ENABLE_ACK:
                    self.send_ack(pck)
            elif pck_type == 'CLUSTER_MERGE_REQUEST':
                if pck.get('dest_gui') == self.id:
                    member_count = pck.get('member_count', 0)
                    current_size = len(self.members_table)
                    capacity = cfg.MAX_CLUSTER_MEMBERS - current_size if cfg.MAX_CLUSTER_MEMBERS else 999
                    if capacity >= member_count + 1:
                        accept_pck = {'dest': wsn.BROADCAST_ADDR, 'type': 'CLUSTER_MERGE_ACCEPT', 'gui': self.id, 'source_gui': self.id, 'dest_gui': pck.get('gui'), 'ch_addr': self.ch_addr, 'created_at': self.now}
                        self.send_with_loss(accept_pck)
            elif pck_type == 'CLUSTER_MERGE_ACCEPT':
                if pck.get('dest_gui') == self.id:
                    self.kill_timer('TIMER_MERGE_TIMEOUT')
                    self.merge_cluster(pck.get('gui'), pck.get('ch_addr'))
            elif pck_type == 'CH_HANDOFF_ACCEPT':
                if pck.get('dest_gui') == self.id and self.handoff_in_progress:
                    self.kill_timer('TIMER_HANDOFF_TIMEOUT')
                    self.complete_ch_handoff(pck['gui'])
            elif pck_type == 'I_AM_ORPHAN':
                orphan_gui = pck.get('gui')
                if orphan_gui and not self.is_cluster_full():
                    eui64 = generate_eui64(orphan_gui)
                    new_addr = wsn.Addr(self.ch_addr.net_addr, orphan_gui)
                    self.send_join_reply(orphan_gui, new_addr, eui64)
            elif pck_type == 'ROUTER_REGISTER':
                router_gui = pck.get('router_id')
                bridged_nets = pck.get('bridged_networks', [])
                if router_gui:
                    self.router_bridges[router_gui] = {
                        'bridged_networks': bridged_nets,
                        'last_heard': self.now
                    }
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
                    if self.ch_addr:
                        self.delayed_exec(random.uniform(0.1, 0.3), self.send_with_loss, 
                            {'dest': self.ch_addr, 'type': 'JOIN_REQUEST', 'gui': pck['gui'], 
                             'eui64': pck.get('eui64', generate_eui64(pck['gui'])), 
                             'created_at': pck.get('created_at', self.now)})
            elif pck_type == 'DATA':
                self._handle_packet_arrival(pck)
                if pck.get('requires_ack') and cfg.ENABLE_ACK:
                    self.send_ack(pck)
            elif pck_type == 'CH_HANDOFF_REQUEST':
                if pck.get('dest_gui') == self.id:
                    accept_pck = {'dest': wsn.BROADCAST_ADDR, 'type': 'CH_HANDOFF_ACCEPT', 'gui': self.id, 'source_gui': self.id, 'dest_gui': pck['gui'], 'created_at': self.now}
                    self.send_with_loss(accept_pck)
            elif pck_type == 'CH_HANDOFF_COMPLETE':
                if pck.get('new_ch_gui') == self.id:
                    self.ch_addr = pck.get('old_ch_addr')
                    self.set_role(Roles.CLUSTER_HEAD)
                    register_ch_position(self.id)
                    if cfg.ENABLE_CLUSTER_OPTIMIZATION:
                        self.set_timer('TIMER_OPTIMIZATION', cfg.OPTIMIZATION_INTERVAL)
                    self.send_network_update()
                    self.send_heart_beat()
            elif pck_type == 'CLUSTER_MERGE_NOTIFY':
                if pck.get('dest_gui') == self.id:
                    new_ch_gui = pck.get('new_ch_gui')
                    new_ch_addr = pck.get('new_ch_addr')
                    self.kill_timer('TIMER_ORPHAN_CHECK')
                    self.kill_timer('TIMER_HEART_BEAT')
                    self.scene.nodecolor(self.id, 1, 1, 0)
                    try:
                        self.erase_parent()
                    except (KeyError, Exception):
                        pass
                    self.parent_gui = new_ch_gui
                    self.set_role(Roles.UNREGISTERED)
                    self.candidate_parents_table = [new_ch_gui] if new_ch_gui else []
                    self.join_attempts = 0
                    if new_ch_addr:
                        self.send_join_request(new_ch_addr)
                        self.set_timer('TIMER_JOIN_REQUEST', 10)
                    else:
                        self.become_unregistered()
            elif pck_type == 'MEMBER_TRANSFER_REQUEST':
                member_gui = pck.get('member_gui')
                target_ch_gui = pck.get('target_ch_gui')
                if member_gui == self.id:
                    self.kill_timer('TIMER_ORPHAN_CHECK')
                    self.kill_timer('TIMER_HEART_BEAT')
                    self.scene.nodecolor(self.id, 1, 1, 0)
                    try:
                        self.erase_parent()
                    except (KeyError, Exception):
                        pass
                    self.parent_gui = target_ch_gui
                    self.set_role(Roles.UNREGISTERED)
                    self.candidate_parents_table = [target_ch_gui] if target_ch_gui else []
                    self.join_attempts = 0
                    if target_ch_gui in self.neighbors_table:
                        target_entry = self.neighbors_table[target_ch_gui]
                        if target_entry.ch_addr:
                            self.send_join_request(target_entry.ch_addr)
                            self.set_timer('TIMER_JOIN_REQUEST', 10)
                        else:
                            self.become_unregistered()
                    else:
                        self.become_unregistered()
            elif pck_type == 'BECOME_ROUTER':
                if pck.get('target_gui') == self.id:
                    ch_gui = pck.get('ch_gui')
                    ch_net_addr = pck.get('ch_net_addr')
                    self.become_router_node(ch_gui, ch_net_addr)
        elif self.role == Roles.ROUTER:
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
                    pck['hop_count'] = pck.get('hop_count', 0) + 1
                    self.delayed_exec(random.uniform(0.1, 0.3), self.send_with_loss, {'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REQUEST', 'gui': pck['gui'], 'eui64': pck.get('eui64', generate_eui64(pck['gui'])), 'created_at': pck.get('created_at', self.now)})
            elif pck_type == 'DATA':
                dest_gui = pck.get('dest_gui')
                dest = pck.get('dest')
                is_for_me = (dest_gui == self.id)
                if self.addr is not None and dest is not None:
                    is_for_me = is_for_me or (dest == self.addr)
                if is_for_me:
                    self._handle_packet_arrival(pck)
                else:
                    self.route_packet(pck)
            elif pck_type == 'CLUSTER_MERGE_NOTIFY':
                if pck.get('dest_gui') == self.id:
                    new_ch_gui = pck.get('new_ch_gui')
                    new_ch_addr = pck.get('new_ch_addr')
                    self.kill_timer('TIMER_ORPHAN_CHECK')
                    self.kill_timer('TIMER_HEART_BEAT')
                    self.scene.nodecolor(self.id, 1, 1, 0)
                    try:
                        self.erase_parent()
                    except (KeyError, Exception):
                        pass
                    self.parent_gui = new_ch_gui
                    self.set_role(Roles.UNREGISTERED)
                    self.candidate_parents_table = [new_ch_gui] if new_ch_gui else []
                    self.join_attempts = 0
                    if new_ch_addr:
                        self.send_join_request(new_ch_addr)
                        self.set_timer('TIMER_JOIN_REQUEST', 10)
                    else:
                        self.become_unregistered()
            elif pck_type == 'MEMBER_TRANSFER_REQUEST':
                member_gui = pck.get('member_gui')
                target_ch_gui = pck.get('target_ch_gui')
                if member_gui == self.id:
                    self.kill_timer('TIMER_ORPHAN_CHECK')
                    self.kill_timer('TIMER_HEART_BEAT')
                    self.scene.nodecolor(self.id, 1, 1, 0)
                    try:
                        self.erase_parent()
                    except (KeyError, Exception):
                        pass
                    self.parent_gui = target_ch_gui
                    self.set_role(Roles.UNREGISTERED)
                    self.candidate_parents_table = [target_ch_gui] if target_ch_gui else []
                    self.join_attempts = 0
                    if target_ch_gui in self.neighbors_table:
                        target_entry = self.neighbors_table[target_ch_gui]
                        if target_entry.ch_addr:
                            self.send_join_request(target_entry.ch_addr)
                            self.set_timer('TIMER_JOIN_REQUEST', 10)
                        else:
                            self.become_unregistered()
                    else:
                        self.become_unregistered()
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
                    cluster_power = pck.get('tx_power', cfg.TX_POWER_DEFAULT)
                    self.cluster_tx_power = cluster_power
                    self.tx_range = cluster_power * cfg.SCALE
                    self.draw_parent()
                    self.kill_timer('TIMER_JOIN_REQUEST')
                    self.join_attempts = 0
                    self.send_heart_beat()
                    self.set_timer('TIMER_HEART_BEAT', cfg.HEARTH_BEAT_TIME_INTERVAL)
                    self.send_join_ack(pck['source'])
                    if cfg.ENABLE_JOIN_TIME_TRACKING and self.id in JOIN_TIMES:
                        JOIN_TIMES[self.id]['end'] = self.now
                        JOIN_TIMES[self.id]['duration'] = self.now - JOIN_TIMES[self.id]['start']
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        register_ch_position(self.id)
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)
                        self.delayed_exec(5.0, self.check_router_status)
                        self.set_timer('TIMER_ORPHAN_CHECK', cfg.ORPHAN_DETECTION_INTERVAL)
                        if cfg.ENABLE_DATA_PACKETS:
                            self.set_timer('TIMER_DATA_PACKET', cfg.DATA_PACKET_START_TIME + random.uniform(0, 50))
                    cluster_net = self.ch_addr.net_addr if self.ch_addr else (pck.get('addr').net_addr if pck.get('addr') else None)
                    log_network_join(self.now, self.id, self.parent_gui, cluster_net)
                    if self.id in ORPHAN_NODES:
                        log_orphan_recovered(self.now, self.id, self.parent_gui)
                        del ORPHAN_NODES[self.id]
            elif pck_type == 'BECOME_CH':
                if pck.get('target_gui') == self.id:
                    router_gui = pck.get('router_gui')
                    self.set_role(Roles.CLUSTER_HEAD)
                    self.addr = wsn.Addr(self.id, 254)
                    self.ch_addr = wsn.Addr(self.id, 254)
                    self.root_addr = wsn.Addr(ROOT_ID, 254)
                    self.hop_count = 2
                    register_ch_position(self.id)
                    self.parent_gui = router_gui
                    if router_gui:
                        self.scene.addlink(router_gui, self.id, "router")
                    self.set_timer('TIMER_HEART_BEAT', cfg.HEARTH_BEAT_TIME_INTERVAL)
                    self.set_timer('TIMER_PROMOTE_ROUTER', 10.0)
            elif pck_type == 'BECOME_ROUTER':
                if pck.get('target_gui') == self.id:
                    ch_gui = pck.get('ch_gui')
                    ch_net_addr = pck.get('ch_net_addr')
                    self.become_router_node(ch_gui, ch_net_addr)

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
                    register_ch_position(self.id)
                    self.update_tx_power(self.id)
                    self.set_timer('TIMER_HEART_BEAT', cfg.HEARTH_BEAT_TIME_INTERVAL)
                    self.set_timer('TIMER_PROMOTE_ROUTER', 10.0)
                    if cfg.ENABLE_DATA_PACKETS:
                        self.set_timer('TIMER_DATA_PACKET', cfg.DATA_PACKET_START_TIME)
                else:
                    self.c_probe = 0
                    self.set_timer('TIMER_PROBE', 30)
        elif name == 'TIMER_HEART_BEAT':
            self.send_heart_beat()
            self.set_timer('TIMER_HEART_BEAT', cfg.HEARTH_BEAT_TIME_INTERVAL)
        elif name == 'TIMER_PROMOTE_ROUTER':
            self.promote_closest_to_router()
        elif name == 'TIMER_FIND_NEW_CH':
            self.find_and_promote_new_ch()
        elif name == 'TIMER_JOIN_REQUEST':
            if len(self.candidate_parents_table) == 0:
                self.become_unregistered()
            else:
                self.select_and_join()
        elif name == 'TIMER_DATA_PACKET':
            if self.data_packets_sent < cfg.DATA_PACKET_COUNT:
                registered_nodes = [n for n in ALL_NODES.values() if n.role in [Roles.REGISTERED, Roles.CLUSTER_HEAD, Roles.ROOT, Roles.ROUTER] and n.id != self.id and n.addr is not None]
                if registered_nodes:
                    dest = random.choice(registered_nodes)
                    self.send_data_packet(dest.id)
                self.set_timer('TIMER_DATA_PACKET', cfg.DATA_PACKET_INTERVAL)
        elif name.startswith('TIMER_RETRANSMIT_'):
            try:
                seq_no = int(name.split('_')[-1])
                self.retransmit_packet(seq_no)
            except (ValueError, IndexError):
                pass
        elif name == 'TIMER_HANDOFF_TIMEOUT':
            if self.handoff_in_progress:
                OVERLAP_STATS['ch_handoffs_failed'] += 1
                self.handoff_in_progress = False
                self.handoff_candidate = None
        elif name == 'TIMER_RECOVER':
            self.recover_node()
        elif name == 'TIMER_ORPHAN_CHECK':
            if self.role in [Roles.REGISTERED, Roles.ROUTER]:
                if not self.check_parent_alive():
                    self.become_orphan(self.parent_gui)
                else:
                    self.set_timer('TIMER_ORPHAN_CHECK', cfg.ORPHAN_DETECTION_INTERVAL)
        elif name == 'TIMER_OPTIMIZATION':
            self.run_cluster_optimization()
            if self.role in [Roles.CLUSTER_HEAD]:
                self.set_timer('TIMER_OPTIMIZATION', cfg.OPTIMIZATION_INTERVAL)
        elif name == 'TIMER_MERGE_TIMEOUT':
            OPTIMIZATION_STATS['merge_failures'] += 1


# ============== EXPORT FUNCTIONS ==============

def export_join_times(path="join_times_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'join_start', 'join_end', 'join_duration'])
        for node_id, times in JOIN_TIMES.items():
            w.writerow([node_id, times['start'], times['end'], times['duration']])

def export_packet_delays(path="packet_delays_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'created', 'delivered', 'delay', 'hops'])
        for p in PACKET_DELAYS:
            w.writerow([p['packet_id'], p['source'], p['dest'], p['created'], p['delivered'], p['delay'], p['hops']])

def export_packet_traces(path="packet_traces_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'path', 'routing_types', 'primary_routing', 'delay'])
        for t in PACKET_TRACES:
            w.writerow([t['packet_id'], t['source'], t['dest'], '->'.join(map(str, t['path'])), ','.join(t['routing_types']), t['primary_routing'], t['delay']])

def export_cluster_stats(path="cluster_stats_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['cluster_net_addr', 'tx_power', 'member_count', 'rejected_joins'])
        for net_addr, stats in CLUSTER_STATS.items():
            w.writerow([net_addr, f"{stats['tx_power']:.2f}", stats['member_count'], stats['rejected_joins']])

def export_event_log(path="event_log_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['timestamp', 'event_type', 'node_id', 'details'])
        for event in EVENT_LOG:
            w.writerow([f"{event['timestamp']:.2f}", event['event_type'], event['node_id'], event['details']])

def export_failure_stats(path="failure_stats_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['metric', 'value'])
        for key, value in FAILURE_STATS.items():
            w.writerow([key, value])

def export_optimization_stats(path="optimization_stats_p8.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['metric', 'value'])
        for key, value in OPTIMIZATION_STATS.items():
            w.writerow([key, value])
        current_clusters = len([n for n in ALL_NODES.values() if n.role in [Roles.CLUSTER_HEAD, Roles.ROOT]])
        w.writerow(['current_cluster_count', current_clusters])


def print_metrics():
    print("\n" + "="*70)
    print("PART 8: MODULAR CLUSTER OPTIMIZATION - METRICS")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Max cluster members: {cfg.MAX_CLUSTER_MEMBERS if cfg.MAX_CLUSTER_MEMBERS else 'Unlimited'}")
    print(f"  TX Power mode: {'Uniform' if cfg.USE_UNIFORM_TX_POWER else 'Per-cluster'}")
    print(f"  Packet loss rate: {cfg.PACKET_LOSS_RATE*100:.1f}%")
    valid_joins = [t['duration'] for t in JOIN_TIMES.values() if t['duration'] is not None]
    if valid_joins:
        print(f"\nJoin Time Statistics:")
        print(f"  Nodes joined: {len(valid_joins)}")
        print(f"  Average join time: {sum(valid_joins)/len(valid_joins):.2f}s")
    print(f"\nPacket Loss Statistics:")
    print(f"  Total packets sent: {PACKET_LOSS_STATS['total_sent']}")
    print(f"  Total packets lost: {PACKET_LOSS_STATS['total_lost']}")
    if PACKET_DELAYS:
        delays = [p['delay'] for p in PACKET_DELAYS]
        print(f"\nPacket Delay Statistics:")
        print(f"  Packets delivered: {len(PACKET_DELAYS)}")
        print(f"  Average delay: {sum(delays)/len(delays):.4f}s")
    print(f"\nRouting Statistics:")
    print(f"  Mesh attempts: {ROUTING_STATS['mesh_attempts']}, Tree attempts: {ROUTING_STATS['tree_attempts']}")
    print(f"\nPart 5: Minimal Overlap & CH Migration:")
    print(f"  CH requests rejected: {OVERLAP_STATS['ch_requests_rejected']}")
    print(f"  Router nodes created: {OVERLAP_STATS['router_nodes_created']}")
    print(f"  CH handoffs: {OVERLAP_STATS['ch_handoffs_completed']}/{OVERLAP_STATS['ch_handoffs_initiated']}")
    print(f"\nPart 6: Node Failure & Recovery:")
    print(f"  Nodes killed: {FAILURE_STATS['nodes_killed']}, Recovered: {FAILURE_STATS['nodes_recovered']}")
    print(f"  Orphans detected: {FAILURE_STATS['orphans_detected']}, Recovered: {FAILURE_STATS['orphans_recovered']}")
    print(f"\nPart 7: Cluster Optimization:")
    print(f"  Merge attempts: {OPTIMIZATION_STATS['merge_attempts']}, Merged: {OPTIMIZATION_STATS['clusters_merged']}")
    print(f"  Members transferred: {OPTIMIZATION_STATS['members_transferred']}")
    print(f"  CH rotations: {OPTIMIZATION_STATS['ch_rotations']}")
    current_clusters = len([n for n in ALL_NODES.values() if n.role in [Roles.CLUSTER_HEAD, Roles.ROOT]])
    print(f"  Current cluster count: {current_clusters}")
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


# ============== MAIN SIMULATION ==============

ROOT_ID = 0

def create_network(node_class, number_of_nodes):
    edge = math.ceil(math.sqrt(number_of_nodes))
    for i in range(number_of_nodes):
        x = i // edge
        y = i % edge
        px = 100 + x * cfg.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-10, 10)
        py = 100 + y * cfg.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-10, 10)
        node = sim.add_node(node_class, (px, py))
        NODE_POS[node.id] = (px, py)
        node.tx_range = cfg.NODE_TX_RANGE * cfg.SCALE
        node.logging = cfg.ENABLE_LOGGING
        node.arrival = random.uniform(0, cfg.NODE_ARRIVAL_MAX)
        if node.id == ROOT_ID:
            node.arrival = 0.1

sim = wsn.Simulator(
    duration=cfg.SIM_DURATION,
    timescale=cfg.SIM_TIME_SCALE,
    visual=cfg.SIM_VISUALIZATION,
    terrain_size=cfg.SIM_TERRAIN_SIZE,
    title=cfg.SIM_TITLE
)

create_network(SensorNode, cfg.SIM_NODE_COUNT)

print(f"Part 8: Modular Cluster Optimization Protocol")
print(f"Nodes: {cfg.SIM_NODE_COUNT}, ROOT: Node {ROOT_ID}")
print(f"Cluster Optimization: {cfg.ENABLE_CLUSTER_OPTIMIZATION}")
if cfg.ENABLE_CLUSTER_OPTIMIZATION:
    print(f"  - Cluster merging: {cfg.ENABLE_CLUSTER_MERGING} (min size: {cfg.MIN_CLUSTER_SIZE})")
    print(f"  - Load balancing: {cfg.ENABLE_LOAD_BALANCING}")
    print(f"  - CH rotation: {cfg.ENABLE_CH_ROTATION}")
print("Starting simulation...")

failure_count = 0
def schedule_random_failure():
    global failure_count
    if not cfg.ENABLE_RANDOM_FAILURES:
        return
    if failure_count >= cfg.MAX_FAILURES:
        return
    eligible_nodes = [n for n in ALL_NODES.values() if n.role in [Roles.REGISTERED, Roles.CLUSTER_HEAD, Roles.ROUTER] and n.id != ROOT_ID]
    if eligible_nodes:
        victim = random.choice(eligible_nodes)
        print(f"\n*** KILLING NODE {victim.id} (role: {victim.role.name}) ***\n")
        victim.kill_node()
        failure_count += 1
    if failure_count < cfg.MAX_FAILURES:
        sim.delayed_exec(cfg.FAILURE_INTERVAL, schedule_random_failure)

if cfg.ENABLE_RANDOM_FAILURES:
    sim.delayed_exec(cfg.FAILURE_START_TIME, schedule_random_failure)

sim.run()

print("\n=== Simulation Finished ===")
print_summary(sim.nodes)
print_metrics()

if cfg.EXPORT_METRICS:
    export_join_times()
    export_packet_delays()
    export_packet_traces()
    export_cluster_stats()
    export_event_log()
    export_failure_stats()
    export_optimization_stats()
    print(f"\nExported metrics to *_p8.csv files")

