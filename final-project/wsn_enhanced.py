"""
Enhanced WSN Simulation - Complete Implementation of instructions.txt requirements:
1. Multi-hop neighbor table with neighbor sharing
2. Packet timestamping & path tracing
3. Hybrid mesh-tree routing
4. Configurable cluster size, TxPower per cluster, packet loss
5. Minimal cluster overlap with ROUTER bridging
6. CH handoff/migration protocol
7. Complete failure recovery with metrics
8. Cluster optimization (minimize clusters & energy)
9. Energy model (CC2420)
"""

import random
import math
import csv
import json
from enum import Enum
from collections import Counter
import sys
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
from source import config_enhanced as config

# ============== GLOBAL TRACKING ==============
NODE_POS = {}
ALL_NODES = []
CLUSTER_HEADS = []
ROUTERS = []
ROLE_COUNTS = Counter()
EVENT_LOG = []
PACKET_TRACES = []
RECOVERY_TRACKER = {}  # {node_id: {'orphan_time': t, 'recovery_time': t}}

METRICS = {
    'join_times': [],
    'packet_delays': [],
    'recovery_times': [],
    'orphan_counts': [],
    'energy_consumed': [],
    'packets_sent': 0,
    'packets_received': 0,
    'packets_dropped': 0,
    'cluster_merges': 0,
    'ch_handoffs': 0,
    'total_orphan_events': 0,
    'avg_recovery_time': 0
}

Roles = Enum('Roles', 'UNDISCOVERED UNREGISTERED ROOT REGISTERED CLUSTER_HEAD ROUTER DEAD')


def log_event(event_type, node_id, timestamp, details=None):
    EVENT_LOG.append({
        'type': event_type,
        'node_id': node_id,
        'time': timestamp,
        'details': details or {}
    })


def log_packet_trace(packet_id, path, delay, packet_type):
    PACKET_TRACES.append({
        'packet_id': packet_id,
        'path': path.copy() if path else [],
        'delay': delay,
        'type': packet_type,
        'hops': len(path) - 1 if path else 0
    })


def get_existing_cluster_heads():
    return [n for n in ALL_NODES if hasattr(n, 'role') and n.role == Roles.CLUSTER_HEAD]


def get_min_distance_to_existing_ch(node_id):
    if node_id not in NODE_POS:
        return float('inf')
    x1, y1 = NODE_POS[node_id]
    min_dist = float('inf')
    for ch in get_existing_cluster_heads():
        if ch.id in NODE_POS and ch.id != node_id:
            x2, y2 = NODE_POS[ch.id]
            dist = math.hypot(x1 - x2, y1 - y2)
            min_dist = min(min_dist, dist)
    return min_dist


class EnergyModel:
    def __init__(self, tx_power_factor=1.0):
        self.energy = config.BATTERY_CAPACITY_JOULES
        self.total_tx_energy = 0
        self.total_rx_energy = 0
        self.packets_sent = 0
        self.packets_received = 0
        self.tx_power_factor = tx_power_factor

    def consume_tx(self, packet_size_bytes):
        energy = (packet_size_bytes + config.PHY_OVERHEAD_BYTES) * config.ENERGY_PER_BYTE_TX * self.tx_power_factor
        energy += config.PLL_TURNAROUND_ENERGY
        self.energy -= energy
        self.total_tx_energy += energy
        self.packets_sent += 1
        return self.energy > config.MIN_ENERGY_THRESHOLD

    def consume_rx(self, packet_size_bytes):
        energy = (packet_size_bytes + config.PHY_OVERHEAD_BYTES) * config.ENERGY_PER_BYTE_RX
        self.energy -= energy
        self.total_rx_energy += energy
        self.packets_received += 1
        return self.energy > config.MIN_ENERGY_THRESHOLD

    def get_remaining_percent(self):
        return (self.energy / config.BATTERY_CAPACITY_JOULES) * 100


class SensorNode(wsn.Node):
    _packet_counter = 0

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
        
        # Tables
        self.neighbors_table = {}
        self.two_hop_neighbors_table = {}
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = []
        self.received_JR_guis = []
        self.router_connections = {}  # For ROUTER role: {ch_gui: ch_addr}
        
        # Per-cluster TxPower
        self.cluster_tx_power = config.TX_POWER_DEFAULT
        
        # Energy model with TX power factor
        tx_factor = self.cluster_tx_power / config.TX_POWER_DEFAULT if config.TX_POWER_DEFAULT > 0 else 1.0
        self.energy_model = EnergyModel(tx_factor) if config.ENERGY_MODEL_ENABLED else None
        
        # Metrics tracking
        self.join_start_time = None
        self.orphan_start_time = None
        self.pending_packets = {}
        self._last_hb_response_time = 0
        self._join_attempts = 0
        self._is_dead_scheduled = False
        self._recovery_scheduled = False

    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)
        # Random node death for failure testing (requirement #6)
        if self.id != ROOT_ID and config.ENABLE_RANDOM_FAILURES:
            if random.random() < config.FAILURE_PROBABILITY:
                death_time = random.uniform(config.FAILURE_START_TIME, config.FAILURE_END_TIME)
                self.set_timer('TIMER_RANDOM_DEATH', death_time)
                self._is_dead_scheduled = True

    def set_role(self, new_role, recolor=True):
        old_role = getattr(self, "role", None)
        if old_role is not None:
            ROLE_COUNTS[old_role] -= 1
            if ROLE_COUNTS[old_role] <= 0:
                ROLE_COUNTS.pop(old_role, None)
        ROLE_COUNTS[new_role] += 1
        self.role = new_role

        if config.ENABLE_EVENT_LOGGING:
            log_event('ROLE_CHANGE', self.id, self.now, {
                'old_role': old_role.name if old_role else None,
                'new_role': new_role.name
            })

        if recolor:
            colors = {
                Roles.UNDISCOVERED: (1, 1, 1),
                Roles.UNREGISTERED: (1, 1, 0),
                Roles.REGISTERED: (0, 1, 0),
                Roles.CLUSTER_HEAD: (0, 0, 1),
                Roles.ROOT: (0, 0, 0),
                Roles.ROUTER: (1, 0.5, 0),  # Orange for routers
                Roles.DEAD: (0.5, 0.5, 0.5)
            }
            if new_role in colors:
                self.scene.nodecolor(self.id, *colors[new_role])
            if new_role == Roles.CLUSTER_HEAD:
                self.draw_tx_range()
                CLUSTER_HEADS.append(self)
            elif new_role == Roles.ROUTER:
                ROUTERS.append(self)
            elif new_role == Roles.ROOT:
                self.set_timer('TIMER_EXPORT_METRICS', 50)
                self.set_timer('TIMER_CLUSTER_OPTIMIZE', 300)

    def check_energy(self):
        if not config.ENERGY_MODEL_ENABLED or self.energy_model is None:
            return True
        if self.energy_model.energy <= config.MIN_ENERGY_THRESHOLD:
            self.die_from_energy()
            return False
        return True

    def die_from_energy(self):
        self.sleep()
        self.set_role(Roles.DEAD)
        self.kill_all_timers()
        self.erase_parent()
        log_event('ENERGY_DEPLETED', self.id, self.now, {
            'remaining_energy': self.energy_model.energy if self.energy_model else 0
        })

    def die_random(self):
        """Random death for failure testing."""
        self.sleep()
        prev_role = self.role
        self.set_role(Roles.DEAD)
        self.kill_all_timers()
        self.erase_parent()
        log_event('RANDOM_DEATH', self.id, self.now, {'previous_role': prev_role.name if prev_role else None})
        METRICS['total_orphan_events'] += 1
        
        # Schedule recovery after some time
        if config.ENABLE_RANDOM_RECOVERY:
            recovery_delay = random.uniform(config.RECOVERY_MIN_TIME, config.RECOVERY_MAX_TIME)
            self.sim.delayed_exec(recovery_delay, self.recover_from_death)
            self._recovery_scheduled = True

    def recover_from_death(self):
        """Recover from random death."""
        if self.role != Roles.DEAD:
            return
        
        log_event('NODE_RECOVERY_START', self.id, self.now)
        self.wake_up()
        self.set_role(Roles.UNDISCOVERED)
        self.scene.nodecolor(self.id, 1, 0, 0)
        
        # Reset state
        self.addr = None
        self.ch_addr = None
        self.parent_gui = None
        self.root_addr = None
        self.c_probe = 0
        self.hop_count = 99999
        self.neighbors_table = {}
        self.two_hop_neighbors_table = {}
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = []
        self.received_JR_guis = []
        self._join_attempts = 0
        self.join_start_time = self.now
        self.orphan_start_time = self.now
        
        # Reset energy for testing (optional)
        if self.energy_model:
            self.energy_model.energy = config.BATTERY_CAPACITY_JOULES * 0.5  # 50% recovery
        
        self.set_timer('TIMER_PROBE', 1)

    def send(self, pck):
        if self.role == Roles.DEAD:
            return
        
        # Packet loss simulation
        if random.random() < config.PACKET_LOSS_RATE:
            METRICS['packets_dropped'] += 1
            return
        
        # Add timestamp and path if not present
        if 'created_at' not in pck:
            pck['created_at'] = self.now
            SensorNode._packet_counter += 1
            pck['packet_id'] = SensorNode._packet_counter
        
        if config.ENABLE_PACKET_TRACING:
            if 'path' not in pck:
                pck['path'] = []
            pck['path'].append(self.id)
        
        # Include info for multi-hop neighbor table
        if pck.get('type') == 'HEART_BEAT':
            pck['one_hop_neighbors'] = list(self.neighbors_table.keys())
            pck['cluster_tx_power'] = self.cluster_tx_power
        
        # Energy consumption
        if config.ENERGY_MODEL_ENABLED and self.energy_model:
            packet_size = config.DEFAULT_PACKET_SIZE_BYTES
            if not self.energy_model.consume_tx(packet_size):
                self.die_from_energy()
                return
        
        METRICS['packets_sent'] += 1
        super().send(pck)

    def become_unregistered(self):
        if self.role == Roles.UNREGISTERED:
            return
        
        if self.role != Roles.UNDISCOVERED:
            self.kill_all_timers()
            log_event('BECAME_ORPHAN', self.id, self.now)
            METRICS['orphan_counts'].append({'node_id': self.id, 'time': self.now})
            self.orphan_start_time = self.now
            RECOVERY_TRACKER[self.id] = {'orphan_time': self.now, 'recovery_time': None}
        
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
        self.two_hop_neighbors_table = {}
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = []
        self.received_JR_guis = []
        self._join_attempts = 0
        self.join_start_time = self.now
        self.send_probe()
        self.kill_timer('TIMER_JOIN_REQUEST')
        self.set_timer('TIMER_JOIN_REQUEST', 20)

    def update_neighbor(self, pck):
        pck['arrival_time'] = self.now
        gui = pck['gui']
        
        if gui in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[gui]
            pck['distance'] = math.hypot(x1 - x2, y1 - y2)
        
        self.neighbors_table[gui] = pck
        
        # Update per-cluster TxPower if received from CH
        if 'cluster_tx_power' in pck and pck.get('role') in [Roles.CLUSTER_HEAD, Roles.ROOT]:
            if self.role == Roles.REGISTERED and gui == self.parent_gui:
                self.update_cluster_tx_power(pck['cluster_tx_power'])
        
        # Update 2-hop neighbor table
        if 'one_hop_neighbors' in pck:
            for two_hop_gui in pck['one_hop_neighbors']:
                if two_hop_gui != self.id and two_hop_gui not in self.neighbors_table:
                    self.two_hop_neighbors_table[two_hop_gui] = {
                        'via_neighbor': gui,
                        'hop_count': pck.get('hop_count', 99999) + 1,
                        'arrival_time': self.now
                    }
        
        if gui not in self.child_networks_table.keys() and gui not in self.members_table:
            if gui not in self.candidate_parents_table:
                self.candidate_parents_table.append(gui)

    def update_cluster_tx_power(self, new_power):
        """Update TX power for this cluster member."""
        self.cluster_tx_power = new_power
        self.tx_range = new_power * config.SCALE
        if self.energy_model:
            self.energy_model.tx_power_factor = new_power / config.TX_POWER_DEFAULT

    def check_neighbors(self):
        will_be_removed = []
        childs_updated = False
        parent_dead = False
        
        for gui, pck in list(self.neighbors_table.items()):
            if self.now - pck['arrival_time'] > config.NEIGHBOR_AGING_FACTOR * config.HEARTH_BEAT_TIME_INTERVAL:
                will_be_removed.append(gui)
                if gui == self.parent_gui:
                    parent_dead = True
                if gui in self.child_networks_table:
                    del self.child_networks_table[gui]
                    childs_updated = True
                if gui in self.candidate_parents_table:
                    self.candidate_parents_table.remove(gui)
        
        for gui in will_be_removed:
            del self.neighbors_table[gui]
        
        two_hop_remove = []
        for gui, info in list(self.two_hop_neighbors_table.items()):
            if self.now - info['arrival_time'] > config.NEIGHBOR_AGING_FACTOR * config.HEARTH_BEAT_TIME_INTERVAL:
                two_hop_remove.append(gui)
        for gui in two_hop_remove:
            del self.two_hop_neighbors_table[gui]
        
        if self.role != Roles.UNREGISTERED:
            if parent_dead:
                self.repair()

    def select_and_join(self):
        min_hop = 99999
        min_hop_gui = 99999
        for gui in self.candidate_parents_table:
            if gui in self.neighbors_table:
                hop = self.neighbors_table[gui].get('hop_count', 99999)
                if hop < min_hop or (hop == min_hop and gui < min_hop_gui):
                    min_hop = hop
                    min_hop_gui = gui
        
        if min_hop_gui != 99999 and min_hop_gui in self.neighbors_table:
            selected_addr = self.neighbors_table[min_hop_gui]['source']
            self.send_join_request(selected_addr)
            self.set_timer('TIMER_JOIN_REQUEST', 5)

    def should_become_ch_minimal_overlap(self):
        """Check if this node should become CH based on minimal overlap policy."""
        if not config.MINIMAL_CLUSTER_OVERLAP:
            return True
        
        min_dist = get_min_distance_to_existing_ch(self.id)
        return min_dist > config.MIN_CH_DISTANCE

    def become_router(self, ch1_gui, ch2_gui):
        """Become a router bridging two cluster heads."""
        if self.role == Roles.ROUTER:
            return
        
        self.set_role(Roles.ROUTER)
        self.router_connections = {
            ch1_gui: self.neighbors_table.get(ch1_gui, {}).get('ch_addr'),
            ch2_gui: self.neighbors_table.get(ch2_gui, {}).get('ch_addr')
        }
        log_event('BECAME_ROUTER', self.id, self.now, {
            'bridging': [ch1_gui, ch2_gui]
        })

    def initiate_ch_handoff(self, new_ch_gui):
        """Initiate CH role handoff to another node."""
        if self.role != Roles.CLUSTER_HEAD:
            return
        if new_ch_gui not in self.neighbors_table:
            return
        
        log_event('CH_HANDOFF_INITIATED', self.id, self.now, {'new_ch': new_ch_gui})
        
        self.send({
            'dest': self.neighbors_table[new_ch_gui].get('addr', wsn.BROADCAST_ADDR),
            'type': 'CH_HANDOFF_REQUEST',
            'source': self.ch_addr,
            'gui': self.id,
            'members': self.members_table.copy(),
            'child_networks': dict(self.child_networks_table),
            'cluster_tx_power': self.cluster_tx_power
        })

    def accept_ch_handoff(self, pck):
        """Accept CH role from another node."""
        self.ch_addr = wsn.Addr(self.id, 254)
        self.members_table = pck.get('members', [])
        self.child_networks_table = pck.get('child_networks', {})
        self.cluster_tx_power = pck.get('cluster_tx_power', config.TX_POWER_DEFAULT)
        self.set_role(Roles.CLUSTER_HEAD)
        
        log_event('CH_HANDOFF_ACCEPTED', self.id, self.now, {
            'from_ch': pck['gui'],
            'members_inherited': len(self.members_table)
        })
        METRICS['ch_handoffs'] += 1
        
        # Notify members of new CH
        self.send({
            'dest': wsn.BROADCAST_ADDR,
            'type': 'CH_HANDOFF_COMPLETE',
            'old_ch_gui': pck['gui'],
            'new_ch_gui': self.id,
            'new_ch_addr': self.ch_addr
        })

    def route_and_forward_package(self, pck):
        dest = pck['dest']
        
        # MESH ROUTING: Check 1-hop neighbors
        for gui, neighbor in self.neighbors_table.items():
            n_addr = neighbor.get('addr')
            n_ch_addr = neighbor.get('ch_addr')
            if n_addr and n_addr == dest:
                pck['next_hop'] = dest
                self.send(pck)
                return
            if n_ch_addr and n_ch_addr == dest:
                pck['next_hop'] = dest
                self.send(pck)
                return
        
        # MESH ROUTING: Check 2-hop via neighbors
        for gui, two_hop in self.two_hop_neighbors_table.items():
            via = two_hop.get('via_neighbor')
            if via and via in self.neighbors_table:
                via_info = self.neighbors_table[via]
                via_addr = via_info.get('ch_addr') or via_info.get('addr')
                if via_addr:
                    pck['next_hop'] = via_addr
                    self.send(pck)
                    return
        
        # ROUTER bridging: Use router if available
        if self.role == Roles.ROUTER:
            for ch_gui, ch_addr in self.router_connections.items():
                if ch_addr and pck['dest'].net_addr == ch_addr.net_addr:
                    pck['next_hop'] = ch_addr
                    self.send(pck)
                    return
        
        # TREE ROUTING: Fall back to tree
        if self.role != Roles.ROOT:
            if self.parent_gui in self.neighbors_table:
                pck['next_hop'] = self.neighbors_table[self.parent_gui].get('ch_addr')
        
        if self.ch_addr is not None:
            if pck['dest'].net_addr == self.ch_addr.net_addr:
                pck['next_hop'] = pck['dest']
            else:
                for child_gui, child_networks in self.child_networks_table.items():
                    if pck['dest'].net_addr in child_networks:
                        pck['next_hop'] = self.neighbors_table[child_gui]['addr']
                        break
        
        self.send(pck)

    def send_probe(self):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE'})

    def send_heart_beat(self):
        self.send({
            'dest': wsn.BROADCAST_ADDR,
            'type': 'HEART_BEAT',
            'source': self.ch_addr if self.ch_addr else self.addr,
            'gui': self.id,
            'role': self.role,
            'addr': self.addr,
            'ch_addr': self.ch_addr,
            'hop_count': self.hop_count,
            'cluster_tx_power': self.cluster_tx_power
        })

    def send_join_request(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_REQUEST', 'gui': self.id})

    def send_join_reply(self, gui, addr):
        self.send({
            'dest': wsn.BROADCAST_ADDR,
            'type': 'JOIN_REPLY',
            'source': self.ch_addr,
            'gui': self.id,
            'dest_gui': gui,
            'addr': addr,
            'root_addr': self.root_addr,
            'hop_count': self.hop_count + 1,
            'cluster_tx_power': self.cluster_tx_power
        })

    def send_join_ack(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id})

    def send_network_request(self):
        self.route_and_forward_package({
            'dest': self.root_addr,
            'type': 'NETWORK_REQUEST',
            'source': self.addr,
            'requester_gui': self.id
        })

    def send_network_reply(self, dest, addr, tx_power=None):
        self.route_and_forward_package({
            'dest': dest,
            'type': 'NETWORK_REPLY',
            'source': self.addr,
            'addr': addr,
            'cluster_tx_power': tx_power or self.assign_cluster_tx_power()
        })

    def assign_cluster_tx_power(self):
        """Assign TX power for a new cluster."""
        if config.TX_POWER_PER_CLUSTER:
            return random.uniform(config.TX_POWER_MIN, config.TX_POWER_MAX)
        return config.TX_POWER_DEFAULT

    def send_network_update(self):
        if self.parent_gui not in self.neighbors_table:
            return
        child_networks = [self.ch_addr.net_addr]
        for networks in self.child_networks_table.values():
            child_networks.extend(networks)
        
        self.send({
            'dest': self.neighbors_table[self.parent_gui]['ch_addr'],
            'type': 'NETWORK_UPDATE',
            'source': self.addr,
            'gui': self.id,
            'child_networks': child_networks
        })

    def send_i_am_orphan(self):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'I_AM_ORPHAN', 'source': self.ch_addr})

    def repair(self):
        log_event('REPAIR_STARTED', self.id, self.now)
        
        if self.role == Roles.REGISTERED:
            self.become_unregistered()
        else:
            if config.REPAIRING_METHOD == 'ALL_ORPHAN':
                self.send_i_am_orphan()
                self.become_unregistered()
            elif config.REPAIRING_METHOD == 'FIND_ANOTHER_PARENT':
                if self.parent_gui in self.candidate_parents_table:
                    self.candidate_parents_table.remove(self.parent_gui)
                    if self.parent_gui in self.neighbors_table:
                        del self.neighbors_table[self.parent_gui]
                
                if len(self.candidate_parents_table) != 0:
                    self.kill_all_timers()
                    self.erase_parent()
                    self.set_role(Roles.UNREGISTERED)
                    self.select_and_join()
                else:
                    self.send_i_am_orphan()
                    self.become_unregistered()

    def on_receive(self, pck):
        if self.role == Roles.DEAD:
            return
        
        if config.ENERGY_MODEL_ENABLED and self.energy_model:
            if not self.energy_model.consume_rx(config.DEFAULT_PACKET_SIZE_BYTES):
                self.die_from_energy()
                return
        
        METRICS['packets_received'] += 1
        
        if config.ENABLE_PACKET_TRACING and 'path' in pck:
            delay = self.now - pck.get('created_at', self.now)
            if pck.get('type') in ['DATA', 'SENSOR']:
                log_packet_trace(pck.get('packet_id'), pck['path'], delay, pck.get('type'))
                METRICS['packet_delays'].append(delay)
        
        # Handle CH handoff messages (all roles)
        if pck.get('type') == 'CH_HANDOFF_REQUEST':
            if self.role == Roles.REGISTERED:
                self.accept_ch_handoff(pck)
            return
        
        if pck.get('type') == 'CH_HANDOFF_COMPLETE':
            if pck['old_ch_gui'] == self.parent_gui:
                self.parent_gui = pck['new_ch_gui']
            return
        
        if self.role in [Roles.ROOT, Roles.CLUSTER_HEAD]:
            if 'next_hop' in pck and pck['dest'] != self.addr and pck['dest'] != self.ch_addr:
                self.route_and_forward_package(pck)
                return
            
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck['type'] == 'PROBE':
                if self.now - self._last_hb_response_time < 1.0:
                    return
                self._last_hb_response_time = self.now
                jitter = random.uniform(0.1, 0.5)
                self.delayed_exec(jitter, self.send_heart_beat)
            elif pck['type'] == 'JOIN_REQUEST':
                if len(self.members_table) >= config.MAX_CLUSTER_MEMBERS:
                    return
                if pck['gui'] in self.members_table:
                    return
                jitter = random.uniform(0.1, 0.3)
                self.delayed_exec(jitter, self.send_join_reply, pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']))
            elif pck['type'] == 'JOIN_REQUEST_FORWARD':
                # Handle forwarded join request from registered members
                requester_gui = pck['gui']
                if len(self.members_table) >= config.MAX_CLUSTER_MEMBERS:
                    return
                if requester_gui in self.members_table:
                    return
                jitter = random.uniform(0.1, 0.3)
                self.delayed_exec(jitter, self.send_join_reply, requester_gui, wsn.Addr(self.ch_addr.net_addr, requester_gui))
            elif pck['type'] == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    requester = pck.get('requester_gui', pck['source'].node_addr)
                    # Check minimal overlap before assigning new network
                    if config.MINIMAL_CLUSTER_OVERLAP:
                        min_dist = get_min_distance_to_existing_ch(requester)
                        if min_dist < config.MIN_CH_DISTANCE:
                            log_event('CH_REQUEST_DENIED_OVERLAP', requester, self.now, {'min_dist': min_dist})
                            return
                    new_addr = wsn.Addr(pck['source'].node_addr, 254)
                    tx_power = self.assign_cluster_tx_power()
                    self.send_network_reply(pck['source'], new_addr, tx_power)
            elif pck['type'] == 'JOIN_ACK':
                self.members_table.append(pck['gui'])
            elif pck['type'] == 'NETWORK_UPDATE':
                self.child_networks_table[pck['gui']] = pck['child_networks']
                if self.role != Roles.ROOT:
                    self.send_network_update()
            elif pck['type'] == 'I_AM_ORPHAN':
                if self.parent_gui in self.neighbors_table:
                    if pck['source'] == self.neighbors_table[self.parent_gui].get('ch_addr'):
                        self.repair()

        elif self.role == Roles.ROUTER:
            # Router forwards between cluster heads
            if 'next_hop' in pck:
                self.route_and_forward_package(pck)
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)

        elif self.role == Roles.REGISTERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck['type'] == 'PROBE':
                if self.now - self._last_hb_response_time < 1.0:
                    return
                self._last_hb_response_time = self.now
                jitter = random.uniform(0.1, 0.5)
                self.delayed_exec(jitter, self.send_heart_beat)
            elif pck['type'] == 'JOIN_REQUEST':
                if pck['gui'] not in self.received_JR_guis:
                    self.received_JR_guis.append(pck['gui'])
                    # Forward JOIN_REQUEST to parent CH instead of becoming CH ourselves
                    if self.parent_gui in self.neighbors_table:
                        parent_info = self.neighbors_table[self.parent_gui]
                        parent_ch_addr = parent_info.get('ch_addr') or parent_info.get('source')
                        if parent_ch_addr:
                            jitter = random.uniform(0.1, 0.3)
                            self.delayed_exec(jitter, self.send, {
                                'dest': parent_ch_addr,
                                'type': 'JOIN_REQUEST_FORWARD',
                                'gui': pck['gui'],
                                'forwarded_by': self.id
                            })
                            return
                    # Fallback: try to become CH if no parent available
                    jitter = random.uniform(0.1, 0.3)
                    self.delayed_exec(jitter, self.send_network_request)
            elif pck['type'] == 'NETWORK_REPLY':
                self.set_role(Roles.CLUSTER_HEAD)
                self.ch_addr = pck['addr']
                self.cluster_tx_power = pck.get('cluster_tx_power', config.TX_POWER_DEFAULT)
                self.update_cluster_tx_power(self.cluster_tx_power)
                self.send_network_update()
                self.send_heart_beat()
                for gui in self.received_JR_guis:
                    self.send_join_reply(gui, wsn.Addr(self.ch_addr.net_addr, gui))
            elif pck['type'] == 'I_AM_ORPHAN':
                if self.parent_gui in self.neighbors_table:
                    if pck['source'] == self.neighbors_table[self.parent_gui].get('ch_addr'):
                        self.repair()

        elif self.role == Roles.UNDISCOVERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
                self.kill_timer('TIMER_PROBE')
                self.become_unregistered()

        if self.role == Roles.UNREGISTERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck['type'] == 'JOIN_REPLY':
                if pck['dest_gui'] == self.id:
                    self.addr = pck['addr']
                    self.parent_gui = pck['gui']
                    self.root_addr = pck['root_addr']
                    self.hop_count = pck['hop_count']
                    self.cluster_tx_power = pck.get('cluster_tx_power', config.TX_POWER_DEFAULT)
                    self.update_cluster_tx_power(self.cluster_tx_power)
                    self.draw_parent()
                    self.kill_timer('TIMER_JOIN_REQUEST')
                    
                    # Track join/recovery time
                    if self.join_start_time:
                        join_time = self.now - self.join_start_time
                        METRICS['join_times'].append({
                            'node_id': self.id,
                            'join_time': join_time
                        })
                        log_event('JOINED_NETWORK', self.id, self.now, {'join_time': join_time})
                    
                    # Track recovery time
                    if self.id in RECOVERY_TRACKER and RECOVERY_TRACKER[self.id]['recovery_time'] is None:
                        recovery_time = self.now - RECOVERY_TRACKER[self.id]['orphan_time']
                        RECOVERY_TRACKER[self.id]['recovery_time'] = recovery_time
                        METRICS['recovery_times'].append({
                            'node_id': self.id,
                            'recovery_time': recovery_time
                        })
                        log_event('RECOVERY_COMPLETE', self.id, self.now, {'recovery_time': recovery_time})
                    
                    self.send_heart_beat()
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    self.send_join_ack(pck['source'])
                    
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)

    def on_timer_fired(self, name, *args, **kwargs):
        if not self.check_energy():
            return
        
        if name == 'TIMER_ARRIVAL':
            self.scene.nodecolor(self.id, 1, 0, 0)
            self.wake_up()
            self.join_start_time = self.now
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
                    self.cluster_tx_power = config.TX_POWER_DEFAULT
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    log_event('BECAME_ROOT', self.id, self.now)
                else:
                    self.c_probe = 0
                    self.set_timer('TIMER_PROBE', 30)

        elif name == 'TIMER_HEART_BEAT':
            if self.role in [Roles.ROOT, Roles.CLUSTER_HEAD, Roles.REGISTERED, Roles.ROUTER]:
                self.check_neighbors()
                self.send_heart_beat()
                self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)

        elif name == 'TIMER_JOIN_REQUEST':
            if self.role != Roles.UNREGISTERED:
                return
            self.check_neighbors()
            if len(self.candidate_parents_table) == 0:
                if self.ch_addr is not None:
                    self.send_i_am_orphan()
                self._join_attempts += 1
                backoff = min(30 * (2 ** self._join_attempts), 300)
                self.c_probe = 0
                self.set_timer('TIMER_PROBE', backoff)
            else:
                self.select_and_join()

        elif name == 'TIMER_EXPORT_METRICS':
            if self.role == Roles.ROOT:
                export_all_metrics()
                self.set_timer('TIMER_EXPORT_METRICS', 100)

        elif name == 'TIMER_RANDOM_DEATH':
            self.die_random()

        elif name == 'TIMER_CLUSTER_OPTIMIZE':
            if self.role == Roles.ROOT:
                self.run_cluster_optimization()
                self.set_timer('TIMER_CLUSTER_OPTIMIZE', 200)

    def run_cluster_optimization(self):
        """Cluster optimization: minimize clusters and energy consumption."""
        cluster_heads = [n for n in sim.nodes if hasattr(n, 'role') and n.role == Roles.CLUSTER_HEAD]
        
        # Find small clusters that can merge
        for ch in cluster_heads:
            if len(ch.members_table) < config.MAX_CLUSTER_MEMBERS // 3:
                best_merge = None
                best_energy = float('inf')
                
                for gui, neighbor in ch.neighbors_table.items():
                    if neighbor.get('role') == Roles.CLUSTER_HEAD:
                        other_ch = sim.nodes[gui] if gui < len(sim.nodes) else None
                        if other_ch and hasattr(other_ch, 'members_table'):
                            combined = len(ch.members_table) + len(other_ch.members_table)
                            if combined <= config.MAX_CLUSTER_MEMBERS:
                                # Prefer merging with lower energy CH (preserve higher energy)
                                other_energy = other_ch.energy_model.energy if other_ch.energy_model else float('inf')
                                if other_energy < best_energy:
                                    best_energy = other_energy
                                    best_merge = gui
                
                if best_merge is not None:
                    # Initiate handoff to merge clusters
                    ch.initiate_ch_handoff(best_merge)
                    METRICS['cluster_merges'] += 1
                    log_event('CLUSTER_MERGE', ch.id, self.now, {
                        'merged_into': best_merge,
                        'combined_members': len(ch.members_table) + len(sim.nodes[best_merge].members_table)
                    })
        
        # Check for nodes that could become routers
        for node in sim.nodes:
            if node.role == Roles.REGISTERED:
                ch_neighbors = [gui for gui, n in node.neighbors_table.items() 
                               if n.get('role') == Roles.CLUSTER_HEAD]
                if len(ch_neighbors) >= 2:
                    # Check if these CHs aren't direct neighbors
                    ch1, ch2 = ch_neighbors[0], ch_neighbors[1]
                    ch1_node = sim.nodes[ch1] if ch1 < len(sim.nodes) else None
                    if ch1_node and ch2 not in ch1_node.neighbors_table:
                        node.become_router(ch1, ch2)

    def get_energy_per_packet(self):
        if not self.energy_model:
            return 0
        total_packets = self.energy_model.packets_sent + self.energy_model.packets_received
        if total_packets == 0:
            return 0
        total_energy = self.energy_model.total_tx_energy + self.energy_model.total_rx_energy
        return total_energy / total_packets


# ============== METRICS EXPORT FUNCTIONS ==============

def export_all_metrics():
    export_event_log()
    export_packet_traces()
    export_metrics_summary()
    export_energy_stats()
    export_recovery_stats()
    export_cluster_stats()


def export_event_log(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}event_log.csv"
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['event_type', 'node_id', 'time', 'details'])
        for event in EVENT_LOG:
            w.writerow([
                event['type'],
                event['node_id'],
                f"{event['time']:.4f}",
                json.dumps(event['details'])
            ])


def export_packet_traces(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}packet_traces.csv"
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'type', 'path', 'hops', 'delay'])
        for trace in PACKET_TRACES:
            w.writerow([
                trace['packet_id'],
                trace['type'],
                '->'.join(map(str, trace['path'])),
                trace['hops'],
                f"{trace['delay']:.4f}"
            ])


def export_metrics_summary(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}metrics_summary.csv"
    
    avg_join_time = sum(j['join_time'] for j in METRICS['join_times']) / len(METRICS['join_times']) if METRICS['join_times'] else 0
    avg_delay = sum(METRICS['packet_delays']) / len(METRICS['packet_delays']) if METRICS['packet_delays'] else 0
    avg_recovery = sum(r['recovery_time'] for r in METRICS['recovery_times']) / len(METRICS['recovery_times']) if METRICS['recovery_times'] else 0
    
    total_energy = sum((config.BATTERY_CAPACITY_JOULES - n.energy_model.energy) 
                      for n in sim.nodes if hasattr(n, 'energy_model') and n.energy_model)
    total_packets = METRICS['packets_sent'] + METRICS['packets_received']
    energy_per_packet = total_energy / total_packets if total_packets > 0 else 0
    
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['metric', 'value'])
        w.writerow(['avg_join_time', f"{avg_join_time:.4f}"])
        w.writerow(['avg_packet_delay', f"{avg_delay:.4f}"])
        w.writerow(['avg_recovery_time', f"{avg_recovery:.4f}"])
        w.writerow(['total_packets_sent', METRICS['packets_sent']])
        w.writerow(['total_packets_received', METRICS['packets_received']])
        w.writerow(['total_packets_dropped', METRICS['packets_dropped']])
        w.writerow(['total_orphan_events', METRICS['total_orphan_events']])
        w.writerow(['total_events_logged', len(EVENT_LOG)])
        w.writerow(['cluster_merges', METRICS['cluster_merges']])
        w.writerow(['ch_handoffs', METRICS['ch_handoffs']])
        w.writerow(['energy_per_packet_uJ', f"{energy_per_packet * 1e6:.4f}"])
        w.writerow(['total_cluster_heads', len([n for n in sim.nodes if n.role == Roles.CLUSTER_HEAD])])
        w.writerow(['total_routers', len([n for n in sim.nodes if n.role == Roles.ROUTER])])


def export_energy_stats(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}energy_stats.csv"
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'role', 'remaining_energy_j', 'remaining_percent', 'tx_energy', 'rx_energy', 
                   'packets_sent', 'packets_received', 'energy_per_packet_uJ', 'cluster_tx_power'])
        for node in sim.nodes:
            if hasattr(node, 'energy_model') and node.energy_model:
                em = node.energy_model
                epp = node.get_energy_per_packet() * 1e6 if hasattr(node, 'get_energy_per_packet') else 0
                w.writerow([
                    node.id,
                    node.role.name if hasattr(node.role, 'name') else str(node.role),
                    f"{em.energy:.6f}",
                    f"{em.get_remaining_percent():.2f}",
                    f"{em.total_tx_energy:.6f}",
                    f"{em.total_rx_energy:.6f}",
                    em.packets_sent,
                    em.packets_received,
                    f"{epp:.4f}",
                    getattr(node, 'cluster_tx_power', config.TX_POWER_DEFAULT)
                ])


def export_recovery_stats(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}recovery_stats.csv"
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'orphan_time', 'recovery_time', 'time_to_recover'])
        for node_id, data in RECOVERY_TRACKER.items():
            w.writerow([
                node_id,
                f"{data['orphan_time']:.4f}",
                f"{data['recovery_time']:.4f}" if data['recovery_time'] else 'N/A',
                f"{data['recovery_time'] - data['orphan_time']:.4f}" if data['recovery_time'] else 'N/A'
            ])


def export_cluster_stats(path=None):
    path = path or f"{config.METRICS_EXPORT_PATH}cluster_stats.csv"
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['cluster_head_id', 'member_count', 'child_networks_count', 'tx_power', 'avg_member_energy'])
        for node in sim.nodes:
            if hasattr(node, 'role') and node.role == Roles.CLUSTER_HEAD:
                member_energies = []
                for m_id in node.members_table:
                    if m_id < len(sim.nodes) and sim.nodes[m_id].energy_model:
                        member_energies.append(sim.nodes[m_id].energy_model.energy)
                avg_energy = sum(member_energies) / len(member_energies) if member_energies else 0
                w.writerow([
                    node.id,
                    len(node.members_table),
                    len(node.child_networks_table),
                    node.cluster_tx_power,
                    f"{avg_energy:.4f}"
                ])


# ============== NETWORK CREATION ==============

# Place ROOT at center of grid for better coverage
ROOT_ID = config.SIM_NODE_COUNT // 2


def create_network(node_class, number_of_nodes=100):
    edge = math.ceil(math.sqrt(number_of_nodes))
    for i in range(number_of_nodes):
        x = i / edge
        y = i % edge
        px = 300 + config.SCALE * x * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-config.SIM_NODE_PLACING_CELL_SIZE / 3, config.SIM_NODE_PLACING_CELL_SIZE / 3)
        py = 200 + config.SCALE * y * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-config.SIM_NODE_PLACING_CELL_SIZE / 3, config.SIM_NODE_PLACING_CELL_SIZE / 3)
        
        node = sim.add_node(node_class, (px, py))
        NODE_POS[node.id] = (px, py)
        ALL_NODES.append(node)
        
        if config.TX_POWER_PER_CLUSTER:
            node.tx_range = random.uniform(config.TX_POWER_MIN, config.TX_POWER_MAX) * config.SCALE
        else:
            node.tx_range = config.TX_POWER_DEFAULT * config.SCALE
        
        node.logging = False
        node.arrival = random.uniform(0, config.NODE_ARRIVAL_MAX)
        
        if node.id == ROOT_ID:
            node.arrival = 0.1


# ============== MAIN SIMULATION ==============

sim = wsn.Simulator(
    duration=config.SIM_DURATION,
    timescale=config.SIM_TIME_SCALE,
    visual=config.SIM_VISUALIZATION,
    terrain_size=config.SIM_TERRAIN_SIZE,
    title=config.SIM_TITLE
)

create_network(SensorNode, config.SIM_NODE_COUNT)

print(f"Created {config.SIM_NODE_COUNT} nodes")
print(f"Root node: {ROOT_ID}")
print(f"Energy model: {'Enabled' if config.ENERGY_MODEL_ENABLED else 'Disabled'}")
print(f"Packet loss rate: {config.PACKET_LOSS_RATE * 100}%")
print(f"Max cluster members: {config.MAX_CLUSTER_MEMBERS}")
print(f"Minimal cluster overlap: {'Enabled' if config.MINIMAL_CLUSTER_OVERLAP else 'Disabled'}")
print(f"Random failures: {'Enabled' if config.ENABLE_RANDOM_FAILURES else 'Disabled'}")
print("Starting simulation...")

sim.run()

print("\n=== Simulation Finished ===")
export_all_metrics()
print(f"Metrics exported to {config.METRICS_EXPORT_PATH}")
print(f"Total events logged: {len(EVENT_LOG)}")
print(f"Packets: sent={METRICS['packets_sent']}, received={METRICS['packets_received']}, dropped={METRICS['packets_dropped']}")
print(f"Cluster heads: {len([n for n in sim.nodes if n.role == Roles.CLUSTER_HEAD])}")
print(f"Routers: {len([n for n in sim.nodes if n.role == Roles.ROUTER])}")
print(f"Cluster merges: {METRICS['cluster_merges']}, CH handoffs: {METRICS['ch_handoffs']}")
