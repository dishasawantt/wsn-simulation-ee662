"""
Part 2: Enhanced Cluster-Tree Network with Appendix B & C Table Structures

This builds on top of data_collection_tree.py protocol, enhancing:
- neighbors_table: Now includes all Appendix B fields (LQI, RSSI, capabilities, etc.)
- members_table: Changed from list to dict with Appendix C MemberEntry structure
- child_networks_table: Enhanced with Appendix C ChildNetEntry structure

The cluster formation protocol (PROBE, HEART_BEAT, JOIN_REQUEST, JOIN_REPLY, 
NETWORK_REQUEST, NETWORK_REPLY, NETWORK_UPDATE) remains the same.
"""

import random
from enum import Enum
import sys
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
import math
from source import config_p2 as config
from collections import Counter
import csv

NODE_POS = {}
ALL_NODES = []
CLUSTER_HEADS = []
ROLE_COUNTS = Counter()

Roles = Enum('Roles', 'UNDISCOVERED UNREGISTERED ROOT REGISTERED CLUSTER_HEAD')


# ============== APPENDIX B: NEIGHBOR TABLE ENTRY ==============

class NeighborEntry:
    """Enhanced neighbor table entry per Appendix B."""
    def __init__(self, gui, pck, distance, timestamp):
        self.gui = gui  # Global Unique ID (node_id)
        self.address = pck.get('addr')  # Short address
        self.ch_addr = pck.get('ch_addr')  # Cluster head address
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
        
        # Appendix B fields
        self.distance = distance
        self.lqi = self._calculate_lqi(distance)  # Link Quality Indicator (0-255)
        self.rssi = self._calculate_rssi(distance)  # Received Signal Strength (dBm)
        self.last_heard = timestamp
        self.hello_interval = config.HEARTH_BEAT_TIME_INTERVAL
        self.capabilities = 0x01  # 8-bit flags
        self.cost = 1  # Hop cost metric
        self.state = 'ACTIVE'  # ACTIVE, STALE, INVALID
    
    def _calculate_lqi(self, distance):
        if distance >= config.NODE_TX_RANGE:
            return 0
        ratio = 1 - (distance / config.NODE_TX_RANGE)
        return int(ratio * 255)
    
    def _calculate_rssi(self, distance, tx_power=0):
        if distance <= 0:
            return tx_power
        path_loss = 20 * math.log10(max(distance, 1))
        return int(tx_power - path_loss)
    
    def update(self, pck, distance, timestamp):
        self.address = pck.get('addr')
        self.ch_addr = pck.get('ch_addr')
        self.role = pck.get('role')
        self.hop_count = pck.get('hop_count', 99999)
        self.source = pck.get('source')
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
    
    def to_dict(self):
        return {
            'gui': self.gui,
            'address': str(self.address) if self.address else None,
            'ch_addr': str(self.ch_addr) if self.ch_addr else None,
            'role': self.role.name if hasattr(self.role, 'name') else str(self.role),
            'hop_count': self.hop_count,
            'distance': f"{self.distance:.2f}",
            'lqi': self.lqi,
            'rssi': self.rssi,
            'last_heard': f"{self.last_heard:.2f}",
            'hello_interval': self.hello_interval,
            'capabilities': hex(self.capabilities),
            'cost': self.cost,
            'state': self.state
        }


# ============== APPENDIX C: MEMBERS TABLE ENTRY ==============

class MemberEntry:
    """Members Table entry per Appendix C - stored by Cluster Heads."""
    def __init__(self, eui64, short_addr, parent_addr, join_time, device_type=0, capabilities=0x01):
        self.eui64 = eui64  # Extended Address (simulated as hex string)
        self.short_addr = short_addr  # Dynamic short address (wsn.Addr)
        self.parent_addr = parent_addr  # CH's own short address
        self.join_time = join_time  # Timestamp when node joined
        self.device_type = device_type  # 0=End Device, 1=Router, 2=CH
        self.capabilities = capabilities  # 8-bit flags
        self.state = 'ACTIVE'  # ACTIVE, ORPHANED, PENDING_REJOIN
        self.last_heard = join_time
    
    def update_last_heard(self, timestamp):
        self.last_heard = timestamp
        self.state = 'ACTIVE'
    
    def is_valid(self, current_time):
        timeout = config.HEARTH_BEAT_TIME_INTERVAL * config.MEMBER_TIMEOUT_FACTOR
        if current_time - self.last_heard > timeout:
            self.state = 'ORPHANED'
            return False
        self.state = 'ACTIVE'
        return True
    
    def to_dict(self):
        device_names = {0: 'End Device', 1: 'Router', 2: 'Cluster Head'}
        return {
            'eui64': self.eui64,
            'short_addr': str(self.short_addr) if self.short_addr else None,
            'parent_addr': str(self.parent_addr) if self.parent_addr else None,
            'join_time': f"{self.join_time:.2f}",
            'device_type': device_names.get(self.device_type, 'Unknown'),
            'capabilities': hex(self.capabilities),
            'state': self.state,
            'last_heard': f"{self.last_heard:.2f}"
        }


# ============== APPENDIX C: CHILD NET TABLE ENTRY ==============

class ChildNetEntry:
    """Child Net (Networks) Table entry per Appendix C - stored by Cluster Heads."""
    def __init__(self, dest_network, ch_addr, next_hop_gui, hop_distance, last_update):
        self.dest_network = dest_network  # Network ID of destination cluster
        self.ch_addr = ch_addr  # Address of the CH for that network
        self.next_hop_gui = next_hop_gui  # GUI of immediate neighbor to forward to
        self.hop_distance = hop_distance  # Hops to destination CH
        self.last_update = last_update  # Timestamp for freshness
        self.state = 'ACTIVE'  # ACTIVE, STALE, INVALID
    
    def update(self, ch_addr, next_hop_gui, hop_distance, timestamp):
        self.ch_addr = ch_addr
        self.next_hop_gui = next_hop_gui
        self.hop_distance = hop_distance
        self.last_update = timestamp
        self.state = 'ACTIVE'
    
    def is_valid(self, current_time):
        timeout = config.HEARTH_BEAT_TIME_INTERVAL * config.CHILD_NET_TIMEOUT_FACTOR
        if current_time - self.last_update > timeout:
            self.state = 'STALE'
            return False
        self.state = 'ACTIVE'
        return True
    
    def to_dict(self):
        return {
            'dest_network': self.dest_network,
            'ch_addr': str(self.ch_addr) if self.ch_addr else None,
            'next_hop_gui': self.next_hop_gui,
            'hop_distance': self.hop_distance,
            'last_update': f"{self.last_update:.2f}",
            'state': self.state
        }


def generate_eui64(node_id):
    return f"0x00124B0001ABCD{node_id:02X}"


###########################################################
class SensorNode(wsn.Node):
    """Enhanced SensorNode with Appendix B & C table structures."""

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
        
        # EUI-64 for this node
        self.eui64 = generate_eui64(self.id)
        
        # ENHANCED: neighbors_table now stores NeighborEntry objects
        self.neighbors_table = {}  # {gui: NeighborEntry}
        
        self.candidate_parents_table = []
        
        # ENHANCED: child_networks_table now stores ChildNetEntry objects  
        self.child_networks_table = {}  # {dest_network: ChildNetEntry}
        
        # ENHANCED: members_table now stores MemberEntry objects
        self.members_table = {}  # {gui: MemberEntry}
        
        self.received_JR_guis = []
        self.join_attempts = 0  # Counter for exponential backoff
        self.last_heartbeat_response = 0  # Rate limiting for PROBE responses

    def set_role(self, new_role, *, recolor=True):
        old_role = getattr(self, "role", None)
        if old_role is not None:
            ROLE_COUNTS[old_role] -= 1
            if ROLE_COUNTS[old_role] <= 0:
                ROLE_COUNTS.pop(old_role, None)
        ROLE_COUNTS[new_role] += 1
        self.role = new_role

        if recolor:
            if new_role == Roles.UNDISCOVERED:
                self.scene.nodecolor(self.id, 1, 1, 1)
            elif new_role == Roles.UNREGISTERED:
                self.scene.nodecolor(self.id, 1, 1, 0)
            elif new_role == Roles.REGISTERED:
                self.scene.nodecolor(self.id, 0, 1, 0)
            elif new_role == Roles.CLUSTER_HEAD:
                self.scene.nodecolor(self.id, 0, 0, 1)
                self.draw_tx_range()
            elif new_role == Roles.ROOT:
                self.scene.nodecolor(self.id, 0, 0, 0)

    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)

    def become_unregistered(self):
        if self.role != Roles.UNDISCOVERED:
            self.kill_all_timers()
            self.log('I became UNREGISTERED')
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
        self.candidate_parents_table = []
        self.child_networks_table = {}
        self.members_table = {}
        self.received_JR_guis = []
        self.join_attempts = 0  # Reset join attempts counter
        self.last_heartbeat_response = 0  # Rate limiting for PROBE responses
        self.send_probe()
        self.set_timer('TIMER_JOIN_REQUEST', 20)

    def update_neighbor(self, pck):
        """ENHANCED: Update neighbor with full Appendix B entry."""
        gui = pck['gui']
        
        # Calculate distance
        distance = 0
        if gui in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[gui]
            distance = math.hypot(x1 - x2, y1 - y2)
        
        if gui in self.neighbors_table:
            # Update existing entry
            self.neighbors_table[gui].update(pck, distance, self.now)
        else:
            # Create new entry
            self.neighbors_table[gui] = NeighborEntry(gui, pck, distance, self.now)
        
        # Update candidate parents (exclude children and members)
        is_child = gui in self.child_networks_table
        is_member = gui in self.members_table
        if not is_child and not is_member:
            if gui not in self.candidate_parents_table:
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

    def send_probe(self):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE', 'gui': self.id})

    def send_heart_beat(self):
        self.send({'dest': wsn.BROADCAST_ADDR,
                   'type': 'HEART_BEAT',
                   'source': self.ch_addr if self.ch_addr is not None else self.addr,
                   'gui': self.id,
                   'role': self.role,
                   'addr': self.addr,
                   'ch_addr': self.ch_addr,
                   'hop_count': self.hop_count,
                   'eui64': self.eui64})

    def send_join_request(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_REQUEST', 'gui': self.id, 'eui64': self.eui64})

    def send_join_reply(self, gui, addr, requester_eui64):
        """ENHANCED: Also add to members_table with Appendix C structure."""
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REPLY', 'source': self.ch_addr,
                   'gui': self.id, 'dest_gui': gui, 'addr': addr, 'root_addr': self.root_addr,
                   'hop_count': self.hop_count + 1})
        
        # Add to enhanced members_table
        member = MemberEntry(
            eui64=requester_eui64,
            short_addr=addr,
            parent_addr=self.ch_addr,
            join_time=self.now,
            device_type=0,  # End Device
            capabilities=0x01
        )
        self.members_table[gui] = member
        self.log(f"Added member: gui={gui}, addr={addr}")

    def send_join_ack(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id})

    def route_and_forward_package(self, pck):
        if self.role != Roles.ROOT and self.parent_gui in self.neighbors_table:
            parent_entry = self.neighbors_table[self.parent_gui]
            # Use ch_addr if available, otherwise fall back to address
            if parent_entry.ch_addr is not None:
                pck['next_hop'] = parent_entry.ch_addr
            elif parent_entry.address is not None:
                pck['next_hop'] = parent_entry.address
            else:
                self.log(f"Warning: No valid address for parent {self.parent_gui}")
        
        if self.ch_addr is not None and pck.get('dest') is not None:
            if pck['dest'].net_addr == self.ch_addr.net_addr:
                pck['next_hop'] = pck['dest']
            else:
                for net_id, entry in self.child_networks_table.items():
                    if pck['dest'].net_addr == net_id:
                        if entry.next_hop_gui in self.neighbors_table:
                            next_hop_addr = self.neighbors_table[entry.next_hop_gui].address
                            if next_hop_addr is not None:
                                pck['next_hop'] = next_hop_addr
                        break
        self.send(pck)
    
    def _get_all_child_networks(self, via_net_id):
        """Get all networks reachable through a specific child network.
        
        This method returns networks that are accessible by routing through
        the CH of via_net_id (i.e., networks whose next_hop leads to via_net_id).
        """
        networks = [via_net_id]
        # Find networks that route through the same next_hop as via_net_id
        if via_net_id in self.child_networks_table:
            target_next_hop = self.child_networks_table[via_net_id].next_hop_gui
            for net_id, entry in self.child_networks_table.items():
                if net_id != via_net_id and entry.next_hop_gui == target_next_hop:
                    networks.append(net_id)
        return networks

    def send_network_request(self):
        self.route_and_forward_package({'dest': self.root_addr, 'type': 'NETWORK_REQUEST', 
                                        'source': self.addr, 'gui': self.id, 'eui64': self.eui64})

    def send_network_reply(self, dest, addr, dest_gui):
        """ENHANCED: Also add to child_networks_table with Appendix C structure."""
        self.route_and_forward_package({'dest': dest, 'type': 'NETWORK_REPLY', 
                                        'source': self.addr, 'addr': addr})
        
        # Add initial child network entry
        child_entry = ChildNetEntry(
            dest_network=addr.net_addr,
            ch_addr=addr,
            next_hop_gui=dest_gui,
            hop_distance=1,
            last_update=self.now
        )
        self.child_networks_table[addr.net_addr] = child_entry
        self.log(f"Added child network: net={addr.net_addr}")

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
                           'gui': self.id, 'child_networks': child_networks, 'ch_addr': self.ch_addr})

    def on_receive(self, pck):
        if self.role == Roles.ROOT or self.role == Roles.CLUSTER_HEAD:
            if 'next_hop' in pck.keys() and pck['dest'] != self.addr and pck['dest'] != self.ch_addr:
                self.route_and_forward_package(pck)
                return
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
                # Update member's last_heard if they're in our members_table
                if pck['gui'] in self.members_table:
                    self.members_table[pck['gui']].update_last_heard(self.now)
            if pck['type'] == 'PROBE':
                # Rate limit PROBE responses to avoid message storms
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:  # Max 1 response per second
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            if pck['type'] == 'JOIN_REQUEST':
                eui64 = pck.get('eui64', generate_eui64(pck['gui']))
                self.delayed_exec(random.uniform(0.1, 0.3), self.send_join_reply, 
                                 pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']), eui64)
            if pck['type'] == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    new_addr = wsn.Addr(pck['source'].node_addr, 254)
                    self.send_network_reply(pck['source'], new_addr, pck.get('gui'))
            if pck['type'] == 'JOIN_ACK':
                # Already added in send_join_reply
                pass
            if pck['type'] == 'NETWORK_UPDATE':
                # ENHANCED: Update child_networks_table with ChildNetEntry
                # Track if anything changed to avoid unnecessary forwarding
                sender_gui = pck['gui']
                sender_ch_addr = pck.get('ch_addr')
                changed = False
                
                for net_id in pck['child_networks']:
                    if net_id == self.ch_addr.net_addr:
                        continue
                    if net_id in self.child_networks_table:
                        # Update existing entry
                        self.child_networks_table[net_id].update(sender_ch_addr, sender_gui, 1, self.now)
                    else:
                        # New network - this is a change
                        changed = True
                        self.child_networks_table[net_id] = ChildNetEntry(
                            dest_network=net_id,
                            ch_addr=sender_ch_addr,
                            next_hop_gui=sender_gui,
                            hop_distance=1,
                            last_update=self.now
                        )
                
                # Only forward if we're not ROOT and something actually changed
                if self.role != Roles.ROOT and changed:
                    self.send_network_update()

        elif self.role == Roles.REGISTERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            if pck['type'] == 'PROBE':
                # Rate limit PROBE responses to avoid message storms
                last_response = getattr(self, 'last_heartbeat_response', 0)
                if self.now - last_response >= 1.0:  # Max 1 response per second
                    self.last_heartbeat_response = self.now
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_heart_beat)
            if pck['type'] == 'JOIN_REQUEST':
                if pck['gui'] not in self.received_JR_guis:
                    self.received_JR_guis.append(pck['gui'])
                    self.send_network_request()
            if pck['type'] == 'NETWORK_REPLY':
                self.set_role(Roles.CLUSTER_HEAD)
                self.ch_addr = pck['addr']
                self.send_network_update()
                self.send_heart_beat()
                for gui in self.received_JR_guis:
                    eui64 = generate_eui64(gui)
                    self.delayed_exec(random.uniform(0.1, 0.5), self.send_join_reply,
                                     gui, wsn.Addr(self.ch_addr.net_addr, gui), eui64)

        elif self.role == Roles.UNDISCOVERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
                self.kill_timer('TIMER_PROBE')
                self.become_unregistered()

        if self.role == Roles.UNREGISTERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            if pck['type'] == 'JOIN_REPLY':
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
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)

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
                    self.scene.nodecolor(self.id, 0, 0, 0)
                    self.addr = wsn.Addr(self.id, 254)
                    self.ch_addr = wsn.Addr(self.id, 254)
                    self.root_addr = self.addr
                    self.hop_count = 0
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
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


# ============== EXPORT FUNCTIONS ==============

def export_neighbor_tables(nodes, path="neighbor_tables_p2.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['node_id', 'neighbor_gui', 'address', 'ch_addr', 'role', 'hop_count',
                   'distance', 'lqi', 'rssi', 'last_heard', 'hello_interval', 
                   'capabilities', 'cost', 'state'])
        
        for node in nodes:
            for gui, entry in node.neighbors_table.items():
                d = entry.to_dict()
                w.writerow([
                    node.id, d['gui'], d['address'], d['ch_addr'], d['role'], d['hop_count'],
                    d['distance'], d['lqi'], d['rssi'], d['last_heard'], d['hello_interval'],
                    d['capabilities'], d['cost'], d['state']
                ])


def export_members_tables(nodes, path="members_table_p2.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['ch_node_id', 'ch_net_addr', 'member_gui', 'member_eui64', 'short_addr',
                   'parent_addr', 'join_time', 'device_type', 'capabilities', 'state', 'last_heard'])
        
        for node in nodes:
            if node.role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
                net_addr = node.ch_addr.net_addr if node.ch_addr else None
                for gui, entry in node.members_table.items():
                    d = entry.to_dict()
                    w.writerow([
                        node.id, net_addr, gui, d['eui64'], d['short_addr'], d['parent_addr'],
                        d['join_time'], d['device_type'], d['capabilities'], d['state'], d['last_heard']
                    ])


def export_child_net_tables(nodes, path="child_net_table_p2.csv"):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['ch_node_id', 'ch_net_addr', 'dest_network', 'dest_ch_addr',
                   'next_hop_gui', 'hop_distance', 'last_update', 'state'])
        
        for node in nodes:
            if node.role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
                net_addr = node.ch_addr.net_addr if node.ch_addr else None
                for net_id, entry in node.child_networks_table.items():
                    d = entry.to_dict()
                    w.writerow([
                        node.id, net_addr, d['dest_network'], d['ch_addr'],
                        d['next_hop_gui'], d['hop_distance'], d['last_update'], d['state']
                    ])


def print_summary(nodes):
    print("\n" + "="*70)
    print("CLUSTER-TREE NETWORK SUMMARY (Enhanced with Appendix B & C Tables)")
    print("="*70)
    
    states = {}
    for node in nodes:
        states[node.role] = states.get(node.role, 0) + 1
    
    print(f"\nNode States:")
    for role, count in states.items():
        print(f"  {role.name}: {count}")
    
    print(f"\nCluster Heads & ROOT:")
    for node in nodes:
        if node.role in [Roles.CLUSTER_HEAD, Roles.ROOT]:
            print(f"\n  Node {node.id} ({node.role.name}):")
            print(f"    Net Addr: {node.ch_addr.net_addr if node.ch_addr else 'N/A'}")
            print(f"    CH Addr: {node.ch_addr}")
            print(f"    Members: {len(node.members_table)}")
            if node.members_table:
                for gui, m in node.members_table.items():
                    print(f"      - GUI {gui}: {m.short_addr} ({m.state})")
            print(f"    Child Networks: {len(node.child_networks_table)}")
            if node.child_networks_table:
                for net_id, c in node.child_networks_table.items():
                    print(f"      - Net {net_id}: via GUI {c.next_hop_gui} ({c.state})")
    
    print("\n" + "="*70)


###########################################################

ROOT_ID = config.SIM_NODE_COUNT // 2  # Central node as root

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

print(f"Part 2: Cluster-Tree Network with Enhanced Tables")
print(f"Nodes: {config.SIM_NODE_COUNT}, ROOT: Node {ROOT_ID}")
print("Starting simulation...")

sim.run()

print("\n=== Simulation Finished ===")
print_summary(sim.nodes)

if config.EXPORT_TABLES:
    export_neighbor_tables(sim.nodes)
    export_members_tables(sim.nodes)
    export_child_net_tables(sim.nodes)
    print(f"\nExported:")
    print("  - neighbor_tables_p2.csv (Appendix B)")
    print("  - members_table_p2.csv (Appendix C)")
    print("  - child_net_table_p2.csv (Appendix C)")
