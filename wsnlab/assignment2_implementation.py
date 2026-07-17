import random
from enum import Enum
import sys
import math
import time
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
from source import config

Roles = Enum('Roles', 'UNDISCOVERED UNREGISTERED ROOT REGISTERED CLUSTER_HEAD')

class PacketLogger:
    def __init__(self):
        self.packets, self.join_times, self.orphan_events, self.role_changes, self.energy_log = [], [], [], [], []
        self.paths = {}
        
    def log_packet(self, pck, event_type, node_id, time):
        self.packets.append({'packet': pck, 'event': event_type, 'node': node_id, 'time': time})
        
    def log_join(self, node_id, join_time):
        self.join_times.append({'node': node_id, 'time': join_time})
        
    def log_path(self, packet_id, node_id, time):
        self.paths.setdefault(packet_id, []).append({'node': node_id, 'time': time})
        
    def log_orphan(self, node_id, time, reason):
        self.orphan_events.append({'node': node_id, 'time': time, 'reason': reason})
        
    def log_role_change(self, node_id, old_role, new_role, time):
        self.role_changes.append({'node': node_id, 'old': old_role, 'new': new_role, 'time': time})
        
    def log_energy(self, node_id, energy, time):
        self.energy_log.append({'node': node_id, 'energy': energy, 'time': time})
        
    def get_average_join_time(self):
        return sum(j['time'] for j in self.join_times) / len(self.join_times) if self.join_times else 0
        
    def print_summary(self):
        print("\n" + "="*80)
        print("SIMULATION SUMMARY")
        print("="*80)
        print(f"Total packets: {len(self.packets)}")
        print(f"Average join time: {self.get_average_join_time():.2f}s")
        print(f"Total join events: {len(self.join_times)}")
        print(f"Orphan events: {len(self.orphan_events)}")
        print(f"Role changes: {len(self.role_changes)}")
        print(f"\nPath traces recorded: {len(self.paths)}")
        for pid, path in list(self.paths.items())[:5]:
            print(f"  Packet {pid}: {' -> '.join(str(p['node']) for p in path)}")
        print("="*80)

logger = PacketLogger()

class SensorNode(wsn.Node):
    packet_id_counter = 0
    
    def init(self):
        self.scene.nodecolor(self.id, 1, 1, 1)
        self.sleep()
        self.addr = self.ch_addr = self.parent_gui = self.root_addr = None
        self.role = Roles.UNDISCOVERED
        self.is_root_eligible = self.id == ROOT_ID
        self.c_probe, self.th_probe, self.hop_count, self.child_count = 0, 10, 99999, 0
        self.neighbors_table, self.multihop_neighbors_table, self.mesh_routing_table, self.child_networks_table = {}, {}, {}, {}
        self.candidate_parents_table, self.members_table, self.received_JR_guis = [], [], []
        self.join_start_time = None
        self.tx_power = config.NODE_TX_POWER_DEFAULT
        if config.ENABLE_ENERGY_MODEL:
            self.energy, self.is_dead = config.INITIAL_ENERGY, False
        
    def run(self):
        self.set_timer('TIMER_ARRIVAL', self.arrival)
        if config.ENABLE_ENERGY_MODEL:
            self.set_timer('TIMER_ENERGY_CHECK', 10)
        if config.ENABLE_CLUSTER_OPTIMIZATION:
            self.set_timer('TIMER_OPTIMIZE', config.OPTIMIZATION_INTERVAL)
            
    def consume_energy(self, bits, tx=True):
        if not config.ENABLE_ENERGY_MODEL or self.is_dead:
            return
        if tx:
            energy_consumed = bits * config.TX_ENERGY_PER_BIT
        else:
            energy_consumed = bits * config.RX_ENERGY_PER_BIT
        self.energy -= energy_consumed
        if self.energy <= config.MIN_ENERGY_THRESHOLD:
            self.die_from_energy_depletion()
            
    def die_from_energy_depletion(self):
        if self.is_dead:
            return
        self.is_dead = True
        self.log(f'ENERGY DEPLETED - Node shutting down')
        logger.log_energy(self.id, 0, self.now)
        self.scene.nodecolor(self.id, 0.5, 0.5, 0.5)
        self.sleep()
        self.erase_parent()
        self.kill_all_timers()
        self.send_i_am_orphan()
        
    def set_role(self, new_role):
        if self.role != new_role:
            logger.log_role_change(self.id, self.role, new_role, self.now)
            self.role = new_role
            
    def become_unregistered(self):
        if self.role != Roles.UNDISCOVERED:
            self.kill_all_timers()
            self.log('I became UNREGISTERED')
            logger.log_orphan(self.id, self.now, 'Becoming unregistered')
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
        self.members_table = []
        self.received_JR_guis = []
        self.child_count = 0
        self.send_probe()
        self.set_timer('TIMER_JOIN_REQUEST', 20)
        self.join_start_time = self.now
        
    def update_neighbor(self, pck):
        pck['arrival_time'] = self.now
        self.neighbors_table[pck['gui']] = pck
        
        if 'multihop_neighbors' in pck:
            for nh_gui, nh_data in pck['multihop_neighbors'].items():
                if nh_gui != self.id and nh_gui not in self.neighbors_table:
                    self.multihop_neighbors_table[nh_gui] = nh_data
                    
        if self.role in [Roles.ROOT, Roles.CLUSTER_HEAD]:
            if pck['gui'] not in self.members_table and pck['gui'] not in self.child_networks_table.keys():
                if self.child_count < config.MAX_CHILD_NODES_PER_CLUSTER:
                    if pck['gui'] not in self.candidate_parents_table:
                        self.candidate_parents_table.append(pck['gui'])
        else:
            if pck['gui'] not in self.child_networks_table.keys() and pck['gui'] not in self.members_table:
                if pck['gui'] not in self.candidate_parents_table:
                    self.candidate_parents_table.append(pck['gui'])
                    
        if pck['gui'] == self.parent_gui and self.hop_count != pck['hop_count'] + 1:
            self.hop_count = pck['hop_count'] + 1
            self.send_heart_beat()
            
    def check_neighbors(self):
        childs_updated = False
        parent_dead = False
        will_be_removed = []
        for gui, pck in self.neighbors_table.items():
            if self.now - pck['arrival_time'] > 3 * config.HEARTH_BEAT_TIME_INTERVAL:
                will_be_removed.append(gui)
                if gui == self.parent_gui:
                    parent_dead = True
                    logger.log_orphan(self.id, self.now, f'Parent {gui} died')
                if gui in self.child_networks_table.keys():
                    del self.child_networks_table[gui]
                    childs_updated = True
                if gui in self.candidate_parents_table:
                    self.candidate_parents_table.remove(gui)
                if gui in self.members_table:
                    self.members_table.remove(gui)
                    self.child_count -= 1
        for gui in will_be_removed:
            del self.neighbors_table[gui]
        if self.role != Roles.UNREGISTERED:
            if parent_dead:
                self.repair()
            else:
                self.send_heart_beat()
                self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                if childs_updated:
                    if self.role != Roles.ROOT:
                        self.send_network_update()
                        
    def select_and_join(self):
        min_hop = 99999
        min_hop_gui = 99999
        for gui in self.candidate_parents_table:
            if gui in self.neighbors_table:
                if self.neighbors_table[gui]['hop_count'] < min_hop or (self.neighbors_table[gui]['hop_count'] == min_hop and gui < min_hop_gui):
                    min_hop = self.neighbors_table[gui]['hop_count']
                    min_hop_gui = gui
        if min_hop_gui != 99999:
            selected_addr = self.neighbors_table[min_hop_gui]['source']
            self.send_join_request(selected_addr)
            self.set_timer('TIMER_JOIN_REQUEST', 5)
            
    def repair(self):
        if self.role == Roles.REGISTERED:
            self.become_unregistered()
        else:
            if config.REPAIRING_METHOD == 'ALL_ORPHAN':
                self.repair_all_orphan()
            elif config.REPAIRING_METHOD == 'FIND_ANOTHER_PARENT':
                self.repair_find_another_parent()
                
    def repair_all_orphan(self):
        self.send_i_am_orphan()
        self.become_unregistered()
        
    def repair_find_another_parent(self):
        if self.parent_gui in self.candidate_parents_table:
            self.candidate_parents_table.remove(self.parent_gui)
        self.neighbors_table.pop(self.parent_gui, None)
        if self.candidate_parents_table:
            self.kill_all_timers()
            self.erase_parent()
            self.set_role(Roles.UNREGISTERED)
            self.select_and_join()
        else:
            self.send_i_am_orphan()
            self.become_unregistered()
            
    def find_mesh_route(self, dest_addr):
        if not config.USE_MESH_ROUTING:
            return None
        for gui, neighbor in self.neighbors_table.items():
            if neighbor.get('addr') == dest_addr:
                return dest_addr
            if 'multihop_neighbors' in neighbor:
                for mh_gui, mh_data in neighbor['multihop_neighbors'].items():
                    if mh_data.get('addr') == dest_addr:
                        return neighbor['source']
        return None
        
    def route_and_forward_package(self, pck):
        if 'packet_id' in pck:
            logger.log_path(pck['packet_id'], self.id, self.now)
            
        mesh_next_hop = self.find_mesh_route(pck['dest'])
        if mesh_next_hop:
            pck['next_hop'] = mesh_next_hop
            pck['routing_type'] = 'mesh'
        else:
            if self.role != Roles.ROOT:
                if self.parent_gui in self.neighbors_table:
                    pck['next_hop'] = self.neighbors_table[self.parent_gui]['ch_addr']
                    pck['routing_type'] = 'tree'
            if self.ch_addr is not None:
                if pck['dest'].net_addr == self.ch_addr.net_addr:
                    pck['next_hop'] = pck['dest']
                else:
                    for child_gui, child_networks in self.child_networks_table.items():
                        if pck['dest'].net_addr in child_networks:
                            if child_gui in self.neighbors_table:
                                pck['next_hop'] = self.neighbors_table[child_gui]['addr']
                            break
        self.send(pck)
        
    def send_i_am_orphan(self):
        if self.ch_addr:
            self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'I_AM_ORPHAN', 'source': self.ch_addr, 'timestamp': self.now})
            
    def send_probe(self):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'PROBE', 'source': self.addr, 'timestamp': self.now})
        
    def send_heart_beat(self):
        multihop_nh = {}
        for gui, nh in list(self.neighbors_table.items())[:5]:
            multihop_nh[gui] = {
                'addr': nh.get('addr'),
                'hop_count': nh.get('hop_count', 99999)
            }
        self.send({'dest': wsn.BROADCAST_ADDR,
                   'type': 'HEART_BEAT',
                   'source': self.ch_addr if self.ch_addr is not None else self.addr,
                   'gui': self.id,
                   'role': self.role,
                   'addr': self.addr,
                   'ch_addr': self.ch_addr,
                   'hop_count': self.hop_count,
                   'multihop_neighbors': multihop_nh,
                   'timestamp': self.now})
                   
    def send_join_request(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_REQUEST', 'source': self.addr, 'gui': self.id, 'timestamp': self.now})
        
    def send_join_reply(self, gui, addr):
        self.send({'dest': wsn.BROADCAST_ADDR, 'type': 'JOIN_REPLY', 'source': self.ch_addr,
                   'gui': self.id, 'dest_gui': gui, 'addr': addr, 'root_addr': self.root_addr,
                   'hop_count': self.hop_count+1, 'timestamp': self.now})
                   
    def send_join_ack(self, dest):
        self.send({'dest': dest, 'type': 'JOIN_ACK', 'source': self.addr, 'gui': self.id, 'timestamp': self.now})
        
    def send_network_request(self):
        self.route_and_forward_package({'dest': self.root_addr, 'type': 'NETWORK_REQUEST', 'source': self.addr, 'timestamp': self.now})
        
    def send_network_reply(self, dest, addr):
        self.route_and_forward_package({'dest': dest, 'type': 'NETWORK_REPLY', 'source': self.addr, 'addr': addr, 'timestamp': self.now})
        
    def send_network_update(self):
        child_networks = [self.ch_addr.net_addr]
        for networks in self.child_networks_table.values():
            child_networks.extend(networks)
        if self.parent_gui in self.neighbors_table:
            self.send({'dest': self.neighbors_table[self.parent_gui]['ch_addr'], 'type': 'NETWORK_UPDATE',
                       'source': self.addr, 'gui': self.id, 'child_networks': child_networks, 'timestamp': self.now})
                       
    def send(self, pck):
        if config.ENABLE_ENERGY_MODEL and self.is_dead:
            return
        if random.random() < config.PACKET_LOSS_RATIO:
            logger.log_packet(pck, 'dropped', self.id, self.now)
            return
        if 'packet_id' not in pck:
            SensorNode.packet_id_counter += 1
            pck['packet_id'] = SensorNode.packet_id_counter
        logger.log_packet(pck, 'sent', self.id, self.now)
        self.consume_energy(1000, tx=True)
        super().send(pck)
        
    def on_receive(self, pck):
        if config.ENABLE_ENERGY_MODEL and self.is_dead:
            return
        logger.log_packet(pck, 'received', self.id, self.now)
        self.consume_energy(1000, tx=False)
        
        if 'packet_id' in pck:
            logger.log_path(pck['packet_id'], self.id, self.now)
            
        if self.role == Roles.ROOT or self.role == Roles.CLUSTER_HEAD:
            if 'next_hop' in pck.keys() and pck['dest'] != self.addr and pck['dest'] != self.ch_addr:
                self.route_and_forward_package(pck)
                return
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            if pck['type'] == 'PROBE':
                self.send_heart_beat()
            if pck['type'] == 'JOIN_REQUEST':
                if self.child_count < config.MAX_CHILD_NODES_PER_CLUSTER:
                    self.send_join_reply(pck['gui'], wsn.Addr(self.ch_addr.net_addr, pck['gui']))
            if pck['type'] == 'NETWORK_REQUEST':
                if self.role == Roles.ROOT:
                    new_addr = wsn.Addr(pck['source'].node_addr, 254)
                    self.send_network_reply(pck['source'], new_addr)
            if pck['type'] == 'JOIN_ACK':
                if pck['gui'] not in self.members_table:
                    self.members_table.append(pck['gui'])
                    self.child_count += 1
            if pck['type'] == 'NETWORK_UPDATE':
                self.child_networks_table[pck['gui']] = pck['child_networks']
                if self.role != Roles.ROOT:
                    self.send_network_update()
            if pck['type'] == 'I_AM_ORPHAN':
                if self.parent_gui in self.neighbors_table:
                    if pck['source'] == self.neighbors_table[self.parent_gui]['ch_addr']:
                        self.repair()
                        
        elif self.role == Roles.REGISTERED:
            if pck['type'] == 'HEART_BEAT':
                self.update_neighbor(pck)
            if pck['type'] == 'PROBE':
                self.send_heart_beat()
            if pck['type'] == 'JOIN_REQUEST':
                self.received_JR_guis.append(pck['gui'])
                self.send_network_request()
            if pck['type'] == 'NETWORK_REPLY':
                self.set_role(Roles.CLUSTER_HEAD)
                self.scene.nodecolor(self.id, 0, 0, 1)
                self.ch_addr = pck['addr']
                self.send_network_update()
                self.send_heart_beat()
                for gui in self.received_JR_guis:
                    if self.child_count < config.MAX_CHILD_NODES_PER_CLUSTER:
                        self.send_join_reply(gui, wsn.Addr(self.ch_addr.net_addr, gui))
            if pck['type'] == 'I_AM_ORPHAN':
                if self.parent_gui in self.neighbors_table:
                    if pck['source'] == self.neighbors_table[self.parent_gui]['ch_addr']:
                        self.repair()
                        
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
                    if self.join_start_time:
                        join_time = self.now - self.join_start_time
                        logger.log_join(self.id, join_time)
                        self.join_start_time = None
                    self.send_heart_beat()
                    self.set_timer('TIMER_HEART_BEAT', config.HEARTH_BEAT_TIME_INTERVAL)
                    self.send_join_ack(pck['source'])
                    if self.ch_addr is not None:
                        self.set_role(Roles.CLUSTER_HEAD)
                        self.send_network_update()
                    else:
                        self.set_role(Roles.REGISTERED)
                        self.scene.nodecolor(self.id, 0, 1, 0)
                        
    def optimize_cluster(self):
        if not config.ENABLE_CLUSTER_OPTIMIZATION:
            return
        if self.role not in [Roles.ROOT, Roles.CLUSTER_HEAD]:
            return
        if config.OPTIMIZATION_TARGET == 'ENERGY':
            if len(self.members_table) < 3 and self.role == Roles.CLUSTER_HEAD:
                self.log('Optimizing: Too few members, considering demotion')
        elif config.OPTIMIZATION_TARGET == 'CLUSTERS':
            if self.child_count > config.MAX_CHILD_NODES_PER_CLUSTER * 0.8:
                self.log('Optimizing: Near capacity, should promote child to CH')
                
    def on_timer_fired(self, name, *args, **kwargs):
        if config.ENABLE_ENERGY_MODEL and self.is_dead:
            return
            
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
            self.check_neighbors()
            
        elif name == 'TIMER_JOIN_REQUEST':
            self.check_neighbors()
            if len(self.candidate_parents_table) == 0:
                if self.ch_addr is not None:
                    self.send_i_am_orphan()
                self.become_unregistered()
            else:
                self.select_and_join()
                
        elif name == 'TIMER_ENERGY_CHECK':
            if config.ENABLE_ENERGY_MODEL:
                self.energy -= config.IDLE_ENERGY_PER_SEC * 10
                if self.energy <= config.MIN_ENERGY_THRESHOLD and not self.is_dead:
                    self.die_from_energy_depletion()
                else:
                    self.set_timer('TIMER_ENERGY_CHECK', 10)
                    
        elif name == 'TIMER_OPTIMIZE':
            self.optimize_cluster()
            self.set_timer('TIMER_OPTIMIZE', config.OPTIMIZATION_INTERVAL)

ROOT_ID = random.randint(0, config.SIM_NODE_COUNT - 1)

def create_network(node_class, number_of_nodes=100):
    edge = math.ceil(math.sqrt(number_of_nodes))
    for i in range(number_of_nodes):
        x = i // edge
        y = i % edge
        px = 50 + x * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-1 * config.SIM_NODE_PLACING_CELL_SIZE / 3, config.SIM_NODE_PLACING_CELL_SIZE / 3)
        py = 50 + y * config.SIM_NODE_PLACING_CELL_SIZE + random.uniform(-1 * config.SIM_NODE_PLACING_CELL_SIZE / 3, config.SIM_NODE_PLACING_CELL_SIZE / 3)
        node = sim.add_node(node_class, (px, py))
        node.tx_range = config.NODE_TX_RANGE
        node.logging = False
        node.arrival = random.uniform(0, config.NODE_ARRIVAL_MAX)
        if node.id == ROOT_ID:
            node.arrival = 0.1

sim = wsn.Simulator(
    duration=config.SIM_DURATION,
    timescale=config.SIM_TIME_SCALE,
    visual=config.SIM_VISUALIZATION,
    terrain_size=config.SIM_TERRAIN_SIZE,
    title=config.SIM_TITLE)

create_network(SensorNode, config.SIM_NODE_COUNT)

print("\n" + "="*80)
print("STARTING SIMULATION")
print("="*80)
print(f"Nodes: {config.SIM_NODE_COUNT}")
print(f"Root Node: {ROOT_ID}")
print(f"Duration: {config.SIM_DURATION}s")
print(f"Max children per cluster: {config.MAX_CHILD_NODES_PER_CLUSTER}")
print(f"Packet loss ratio: {config.PACKET_LOSS_RATIO}")
print(f"Mesh routing: {config.USE_MESH_ROUTING}")
print(f"Energy model: {config.ENABLE_ENERGY_MODEL}")
print(f"Cluster optimization: {config.ENABLE_CLUSTER_OPTIMIZATION}")
print("="*80 + "\n")

sim.run()

logger.print_summary()

