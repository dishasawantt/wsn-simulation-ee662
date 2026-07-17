"""
Part 3: Hybrid Mesh-Tree Routing with Metrics

Builds on Part 2, adding:
1. Hybrid Routing:
   - Mesh: If destination in 1-hop or 2-hop neighbor table, forward directly
   - Tree: Otherwise, forward to CH who routes via Members/Child Net Tables

2. Metrics:
   - Join time tracking (time from PROBE to REGISTERED)
   - Packet delay tracking (creation time to delivery)
   - Packet path tracing (record each hop)

Per Assignment1_instructions.txt Section 4.8:
- Mesh Routing: Forward directly if destination accessible via neighbor
- Tree Routing: Forward upward to CH, who routes using tables
"""

import random
from enum import Enum
import sys
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
import math
from source import config_p3 as config
from collections import Counter
import csv
import uuid

NODE_POS = {}
ALL_NODES = {}
ROLE_COUNTS = Counter()

# ============== GLOBAL METRICS ==============
JOIN_TIMES = {}  # {node_id: {'start': time, 'end': time, 'duration': time}}
PACKET_DELAYS = []  # [{'packet_id': id, 'source': id, 'dest': id, 'created': time, 'delivered': time, 'delay': time}]
PACKET_TRACES = []  # [{'packet_id': id, 'source': id, 'dest': id, 'path': [node_ids], 'routing_type': 'MESH'/'TREE'}]
ROUTING_STATS = {'mesh_success': 0, 'tree_success': 0, 'mesh_attempts': 0, 'tree_attempts': 0}

Roles = Enum('Roles', 'UNDISCOVERED UNREGISTERED ROOT REGISTERED CLUSTER_HEAD')


# ============== APPENDIX B: NEIGHBOR TABLE ENTRY ==============

class NeighborEntry:
    def __init__(self, gui, pck, distance, timestamp):
        self.gui = gui
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        self.net_addr = pck['ch_addr'].net_addr if pck.get('ch_addr') else None
        
        self.distance = distance
        self.lqi = self._calculate_lqi(distance)
        self.rssi = self._calculate_rssi(distance)
        self.last_heard = timestamp
        self.hello_interval = config.HEARTH_BEAT_TIME_INTERVAL
        self.capabilities = 0x01
        self.cost = 1
        self.state = 'ACTIVE'
    
    def _calculate_lqi(self, distance):
        if distance >= config.NODE_TX_RANGE:
            return 0
        return int((1 - distance / config.NODE_TX_RANGE) * 255)
    
    def _calculate_rssi(self, distance, tx_power=0):
        if distance <= 0:
            return tx_power
        return int(tx_power - 20 * math.log10(max(distance, 1)))
    
    def update(self, pck, distance, timestamp):
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        self.net_addr = pck['ch_addr'].net_addr if pck.get('ch_addr') else None
        self.distance = distance
        self.lqi = self._calculate_lqi(distance)
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


###########################################################
class SensorNode(wsn.Node):
    """Enhanced SensorNode with Hybrid Mesh-Tree Routing."""

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
        self.neighbors_table = {}  # {gui: NeighborEntry}
        self.two_hop_neighbors = {}  # {gui: TwoHopNeighborEntry}
        self.candidate_parents_table = []
        self.child_networks_table = {}  # {dest_network: ChildNetEntry}
        self.members_table = {}  # {gui: MemberEntry}
        self.received_JR_guis = []
        self.join_attempts = 0  # Counter for exponential backoff
        self.last_heartbeat_response = 0  # Rate limiting for PROBE responses
        
        # Metrics tracking
        self.join_start_time = None
        self.data_packets_sent = 0
        
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
            }
            if new_role in colors:
                self.scene.nodecolor(self.id, *colors[new_role])
            if new_role == Roles.CLUSTER_HEAD:
                self.draw_tx_range()

    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)

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
        self.join_attempts = 0  # Reset join attempts counter
        self.last_heartbeat_response = 0  # Rate limiting for PROBE responses
        
        # Track join start time
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
        
        if gui in self.neighbors_table:
            self.neighbors_table[gui].update(pck, distance, self.now)
        else:
            self.neighbors_table[gui] = NeighborEntry(gui, pck, distance, self.now)
        
        # Update 2-hop neighbors from shared info
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
        # Increment join attempts counter for exponential backoff
        self.join_attempts = getattr(self, 'join_attempts', 0) + 1
        
        # Exponential backoff after too many attempts
        if self.join_attempts > 10:
            self.log("Max join attempts reached, backing off...")
            self.kill_timer('TIMER_JOIN_REQUEST')
            self.set_timer('TIMER_JOIN_REQUEST', 30)  # Longer backoff
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
            if selected_addr is not None:  # Null check
                self.send_join_request(selected_addr)
            else:
                self.log(f"Warning: No valid source address for neighbor {min_hop_gui}")
        
        # Kill existing timer before setting new one to prevent accumulation
        self.kill_timer('TIMER_JOIN_REQUEST')
        self.set_timer('TIMER_JOIN_REQUEST', 5)

    # ============== HYBRID MESH-TREE ROUTING ==============

    def route_packet(self, pck):
        """
        Hybrid routing per Assignment1_instructions Section 4.8:
        1. MESH: If destination in 1-hop or 2-hop neighbor table, forward directly
        2. TREE: Otherwise, forward to parent CH who routes via tables
        """
        dest = pck.get('dest')
        dest_gui = pck.get('dest_gui')
        
        # Initialize path tracking
        if 'path' not in pck:
            pck['path'] = [self.id]
            pck['routing_types'] = []
        else:
            if self.id not in pck['path']:
                pck['path'].append(self.id)
        
        # Check if we are the destination
        is_my_addr = (self.addr is not None and dest is not None and dest == self.addr)
        is_my_ch = (self.ch_addr is not None and dest is not None and dest == self.ch_addr)
        is_my_gui = (dest_gui == self.id)
        if is_my_addr or is_my_ch or is_my_gui:
            self._handle_packet_arrival(pck)
            return
        
        # === MESH ROUTING: Check 1-hop neighbors ===
        if config.ENABLE_MESH_ROUTING:
            # Check if destination node is a direct neighbor
            for gui, entry in self.neighbors_table.items():
                if entry.address == dest or gui == dest_gui:
                    pck['next_hop'] = entry.address if entry.address else entry.source
                    pck['routing_types'].append('MESH_1HOP')
                    ROUTING_STATS['mesh_attempts'] += 1
                    self.send(pck)
                    return
            
            # Check if destination's network is accessible via 1-hop neighbor
            if hasattr(dest, 'net_addr'):
                for gui, entry in self.neighbors_table.items():
                    if entry.net_addr == dest.net_addr:
                        pck['next_hop'] = entry.source
                        pck['routing_types'].append('MESH_1HOP_NET')
                        ROUTING_STATS['mesh_attempts'] += 1
                        self.send(pck)
                        return
            
            # === MESH ROUTING: Check 2-hop neighbors ===
            if config.USE_TWO_HOP_MESH and dest_gui in self.two_hop_neighbors:
                via = self.two_hop_neighbors[dest_gui].via_neighbor
                if via in self.neighbors_table:
                    pck['next_hop'] = self.neighbors_table[via].source
                    pck['routing_types'].append('MESH_2HOP')
                    ROUTING_STATS['mesh_attempts'] += 1
                    self.send(pck)
                    return
        
        # === TREE ROUTING ===
        ROUTING_STATS['tree_attempts'] += 1
        pck['routing_types'].append('TREE')
        
        if self.role == Roles.ROOT:
            # ROOT: Check child networks table for downward routing
            if hasattr(dest, 'net_addr'):
                for net_id, entry in self.child_networks_table.items():
                    if dest.net_addr == net_id:
                        if entry.next_hop_gui in self.neighbors_table:
                            next_hop_addr = self.neighbors_table[entry.next_hop_gui].address
                            if next_hop_addr is not None:
                                pck['next_hop'] = next_hop_addr
                                self.send(pck)
                                return
            # Check members table
            if dest_gui in self.members_table:
                if dest is not None:
                    pck['next_hop'] = dest
                    self.send(pck)
                    return
        
        elif self.role == Roles.CLUSTER_HEAD:
            # CH: Check if destination is in our cluster (members table)
            if dest_gui in self.members_table:
                pck['next_hop'] = dest
                self.send(pck)
                return
            
            # Check child networks for downward routing
            if hasattr(dest, 'net_addr'):
                for net_id, entry in self.child_networks_table.items():
                    if dest.net_addr == net_id:
                        if entry.next_hop_gui in self.neighbors_table:
                            next_hop_addr = self.neighbors_table[entry.next_hop_gui].address
                            if next_hop_addr is not None:
                                pck['next_hop'] = next_hop_addr
                                self.send(pck)
                                return
            
            # Forward upward to parent
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
                if parent_ch is not None:
                    pck['next_hop'] = parent_ch
                    self.send(pck)
                    return
        
        else:
            # REGISTERED: Forward to parent CH
            if self.parent_gui in self.neighbors_table:
                parent_entry = self.neighbors_table[self.parent_gui]
                parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
                if parent_ch is not None:
                    pck['next_hop'] = parent_ch
                    self.send(pck)
                    return
        
        # Fallback: try parent if no route found
        if self.parent_gui in self.neighbors_table:
            parent_entry = self.neighbors_table[self.parent_gui]
            parent_ch = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
            if parent_ch is not None:
                pck['next_hop'] = parent_ch
                self.send(pck)

    def _handle_packet_arrival(self, pck):
        """Handle packet that has arrived at destination."""
        if pck.get('type') == 'DATA':
            # Record delay
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
            
            # Record trace
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
        """Send a data packet to another node."""
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
            'packet_id': packet_id,
            'created_at': self.now,
            'payload': f"Data from {self.id} to {dest_gui}",
        }
        
        self.log(f"Sending DATA {packet_id} to node {dest_gui}")
        self.route_packet(pck)
        self.data_packets_sent += 1

    # ============== PROTOCOL MESSAGES ==============

    def send_probe(self):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE', 'gui': self.id, 'created_at': self.now})

    def send_heart_beat(self):
        one_hop_list = list(self.neighbors_table.keys())
        self.send({'dest': wsn.BROADCAST_ADDR,
                   'type': 'HEART_BEAT',
                   'source': self.ch_addr if self.ch_addr else self.addr,
                   'gui': self.id,
                   'role': self.role,
                   'addr': self.addr,
                   'ch_addr': self.ch_addr,
                   'hop_count': self.hop_count,
                   'eui64': self.eui64,
                   'one_hop_neighbors': one_hop_list,
                   'created_at': self.now})

    def send_join_request(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_REQUEST', 'gui': self.id, 'eui64': self.eui64, 'created_at': self.now})

    def send_join_reply(self, gui, addr, requester_eui64):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REPLY', 'source': self.ch_addr,
                   'gui': self.id, 'dest_gui': gui, 'addr': addr, 'root_addr': self.root_addr,
                   'hop_count': self.hop_count + 1, 'created_at': self.now})
        
        self.members_table[gui] = MemberEntry(requester_eui64, addr, self.ch_addr, self.now, 0, 0x01)

    def send_join_ack(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id, 'created_at': self.now})

    def send_network_request(self):
        pck = {'dest': self.root_addr, 'type': 'NETWORK_REQUEST', 'source': self.addr, 
               'gui': self.id, 'eui64': self.eui64, 'created_at': self.now}
        self.route_packet(pck)

    def send_network_reply(self, dest, addr, dest_gui):
        pck = {'dest': dest, 'type': 'NETWORK_REPLY', 'source': self.addr, 'addr': addr, 'created_at': self.now}
        self.route_packet(pck)
        self.child_networks_table[addr.net_addr] = ChildNetEntry(addr.net_addr, addr, dest_gui, 1, self.now)

    def send_network_update(self):
        if self.ch_addr is None:
            return  # Can't send update without our own CH address
            
        child_networks = [self.ch_addr.net_addr]
        for net_id in self.child_networks_table.keys():
            child_networks.append(net_id)
        
        if self.parent_gui in self.neighbors_table:
            parent_entry = self.neighbors_table[self.parent_gui]
            dest = parent_entry.ch_addr if parent_entry.ch_addr is not None else parent_entry.address
            if dest is not None:
                self.send({'dest': dest, 'type': 'NETWORK_UPDATE', 'source': self.addr,
                           'gui': self.id, 'child_networks': child_networks, 'ch_addr': self.ch_addr, 'created_at': self.now})

    # ============== PACKET HANDLERS ==============

    def on_receive(self, pck):
        pck_type = pck.get('type')
        
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
                # Rate limit PROBE responses to avoid message storms
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:  # Max 1 response per second
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            elif pck_type == 'JOIN_REQUEST':
                eui64 = pck.get('eui64', generate_eui64(pck['gui']))
                self.delayed_exec(random.uniform(0.1, 0.3), self.send_join_reply,
                                 pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']), eui64)
            elif pck_type == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    new_addr = wsn.Addr(pck['source'].node_addr, 254)
                    self.send_network_reply(pck['source'], new_addr, pck.get('gui'))
            elif pck_type == 'NETWORK_UPDATE':
                # Track if anything changed to avoid unnecessary forwarding
                sender_gui = pck['gui']
                sender_ch_addr = pck.get('ch_addr')
                changed = False
                
                for net_id in pck['child_networks']:
                    if net_id != self.ch_addr.net_addr:
                        if net_id in self.child_networks_table:
                            self.child_networks_table[net_id].update(sender_ch_addr, sender_gui, 1, self.now)
                        else:
                            # New network - this is a change
                            changed = True
                            self.child_networks_table[net_id] = ChildNetEntry(net_id, sender_ch_addr, sender_gui, 1, self.now)
                
                # Only forward if we're not ROOT and something actually changed
                if self.role != Roles.ROOT and changed:
                    self.send_network_update()
            elif pck_type == 'DATA':
                self._handle_packet_arrival(pck)

        elif self.role == Roles.REGISTERED:
            if pck_type == 'HEART_BEAT':
                self.update_neighbor(pck)
            elif pck_type == 'PROBE':
                # Rate limit PROBE responses to avoid message storms
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:  # Max 1 response per second
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            elif pck_type == 'JOIN_REQUEST':
                if pck['gui'] not in self.received_JR_guis:
                    self.received_JR_guis.append(pck['gui'])
                    self.send_network_request()
            elif pck_type == 'NETWORK_REPLY':
                self.set_role(Roles.CLUSTER_HEAD)
                self.ch_addr = pck['addr']
                self.send_network_update()
                self.send_heart_beat()
                for gui in self.received_JR_guis:
                    eui64 = generate_eui64(gui)
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_join_reply,
                                     gui, wsn.Addr(self.ch_addr.net_addr, gui), eui64)
            elif pck_type == 'DATA':
                self._handle_packet_arrival(pck)

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
                    self.draw_parent()
                    self.kill_timer('TIMER_JOIN_REQUEST')
                    self.join_attempts = 0  # Reset join attempts on successful join
                    self.send_heart_beat()
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    self.send_join_ack(pck['source'])
                    
                    # Track join time
                    if config.ENABLE_JOIN_TIME_TRACKING and self.id in JOIN_TIMES:
                        JOIN_TIMES[self.id]['end'] = self.now
                        JOIN_TIMES[self.id]['duration'] = self.now - JOIN_TIMES[self.id]['start']
                    
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)
                        # Start data packet timer
                        if config.ENABLE_DATA_PACKETS:
                            self.set_timer('TIMER_DATA_PACKET', config.DATA_PACKET_START_TIME + random.uniform(0, 50))

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
                # Pick a random destination
                registered_nodes = [n for n in ALL_NODES.values() 
                                   if n.role in [Roles.REGISTERED, Roles.CLUSTER_HEAD, Roles.ROOT] 
                                   and n.id != self.id and n.addr is not None]
                if registered_nodes:
                    dest = random.choice(registered_nodes)
                    self.send_data_packet(dest.id)
                self.set_timer('TIMER_DATA_PACKET', config.DATA_PACKET_INTERVAL)


# ============== EXPORT FUNCTIONS ==============

def export_join_times(path="join_times_p3.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'join_start', 'join_end', 'join_duration'])
        for node_id, times in JOIN_TIMES.items():
            w.writerow([node_id, times['start'], times['end'], times['duration']])


def export_packet_delays(path="packet_delays_p3.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'created', 'delivered', 'delay', 'hops'])
        for p in PACKET_DELAYS:
            w.writerow([p['packet_id'], p['source'], p['dest'], p['created'], p['delivered'], p['delay'], p['hops']])


def export_packet_traces(path="packet_traces_p3.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['packet_id', 'source', 'dest', 'path', 'routing_types', 'primary_routing', 'delay'])
        for t in PACKET_TRACES:
            w.writerow([t['packet_id'], t['source'], t['dest'], 
                       '->'.join(map(str, t['path'])), 
                       ','.join(t['routing_types']), t['primary_routing'], t['delay']])


def print_metrics():
    print("\n" + "="*70)
    print("ROUTING & TIMING METRICS")
    print("="*70)
    
    # Join times
    valid_joins = [t['duration'] for t in JOIN_TIMES.values() if t['duration'] is not None]
    if valid_joins:
        print(f"\nJoin Time Statistics:")
        print(f"  Nodes joined: {len(valid_joins)}")
        print(f"  Average join time: {sum(valid_joins)/len(valid_joins):.2f}s")
        print(f"  Min join time: {min(valid_joins):.2f}s")
        print(f"  Max join time: {max(valid_joins):.2f}s")
    
    # Packet delays
    if PACKET_DELAYS:
        delays = [p['delay'] for p in PACKET_DELAYS]
        hops = [p['hops'] for p in PACKET_DELAYS]
        print(f"\nPacket Delay Statistics:")
        print(f"  Packets delivered: {len(PACKET_DELAYS)}")
        print(f"  Average delay: {sum(delays)/len(delays):.4f}s")
        print(f"  Min delay: {min(delays):.4f}s")
        print(f"  Max delay: {max(delays):.4f}s")
        print(f"  Average hops: {sum(hops)/len(hops):.2f}")
    
    # Routing stats
    print(f"\nRouting Statistics:")
    print(f"  Mesh attempts: {ROUTING_STATS['mesh_attempts']}")
    print(f"  Tree attempts: {ROUTING_STATS['tree_attempts']}")
    print(f"  Mesh successes: {ROUTING_STATS['mesh_success']}")
    print(f"  Tree successes: {ROUTING_STATS['tree_success']}")
    
    # Packet traces summary
    if PACKET_TRACES:
        mesh_count = sum(1 for t in PACKET_TRACES if t['primary_routing'] == 'MESH')
        tree_count = sum(1 for t in PACKET_TRACES if t['primary_routing'] == 'TREE')
        print(f"\nPacket Traces:")
        print(f"  Total traced: {len(PACKET_TRACES)}")
        print(f"  Via MESH: {mesh_count}")
        print(f"  Via TREE: {tree_count}")
    
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

print(f"Part 3: Hybrid Mesh-Tree Routing")
print(f"Nodes: {config.SIM_NODE_COUNT}, ROOT: Node {ROOT_ID}")
print(f"Mesh routing: {config.ENABLE_MESH_ROUTING}, 2-hop mesh: {config.USE_TWO_HOP_MESH}")
print("Starting simulation...")

sim.run()

print("\n=== Simulation Finished ===")
print_summary(sim.nodes)
print_metrics()

if config.EXPORT_METRICS:
    export_join_times()
    export_packet_delays()
    export_packet_traces()
    print(f"\nExported metrics:")
    print("  - join_times_p3.csv")
    print("  - packet_delays_p3.csv")
    print("  - packet_traces_p3.csv")

