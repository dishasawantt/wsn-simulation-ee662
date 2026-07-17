"""
Part 1: Neighbor Discovery Protocol Implementation

Implements:
- One-hop neighbor table populated via HELLO messages
- Multi-hop neighbor table populated via neighbor table sharing
- Toggle between single-hop and multi-hop modes via config flag

Neighbor Table Fields (per Appendix B):
- Neighbor Address: Node ID
- LQI: Link Quality Indicator (simulated via distance)
- RSSI: Received Signal Strength (simulated)
- Last Heard Timestamp: For neighbor aging
- Capabilities/Flags: Node type info
- Cost/Metric: Hop count
- State/Validity: Active, stale, invalid
"""

import random
import math
import csv
import sys
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
from source import config_p1 as config

NODE_POS = {}


class NeighborEntry:
    """One-hop neighbor table entry per Appendix B specification."""
    def __init__(self, node_id, distance, lqi, rssi, timestamp, hop_count=1, capabilities=0):
        self.node_id = node_id
        self.distance = distance
        self.lqi = lqi  # Link Quality Indicator (0-255)
        self.rssi = rssi  # Received Signal Strength in dBm
        self.last_heard = timestamp
        self.hello_interval = config.HELLO_INTERVAL
        self.capabilities = capabilities  # 8-bit flags
        self.cost = hop_count  # Hop cost metric
        self.state = 'ACTIVE'  # ACTIVE, STALE, INVALID
        self.hop_count = hop_count
    
    def is_valid(self, current_time):
        timeout = config.HELLO_INTERVAL * config.NEIGHBOR_TIMEOUT_FACTOR
        if current_time - self.last_heard > timeout:
            self.state = 'STALE'
            return False
        self.state = 'ACTIVE'
        return True
    
    def to_dict(self):
        return {
            'node_id': self.node_id,
            'distance': f"{self.distance:.2f}",
            'lqi': self.lqi,
            'rssi': self.rssi,
            'last_heard': f"{self.last_heard:.2f}",
            'hop_count': self.hop_count,
            'cost': self.cost,
            'state': self.state,
            'capabilities': self.capabilities
        }


class TwoHopNeighborEntry:
    """Two-hop neighbor table entry per Appendix B specification."""
    def __init__(self, node_id, via_neighbor, path_cost, timestamp, hop_count=2, 
                 parent_seqno=0, capabilities=0):
        self.node_id = node_id
        self.via_neighbor = via_neighbor  # 1-hop neighbor through which this is reachable
        self.path_cost = path_cost  # Combined cost via parent
        self.last_heard = timestamp
        self.parent_seqno = parent_seqno  # Sequence number from parent's HELLO
        self.hop_count = hop_count
        self.capabilities = capabilities  # 8-bit flags (reachable, battery-powered, etc.)
        self.state = 'ACTIVE'
    
    def is_valid(self, current_time):
        timeout = config.HELLO_INTERVAL * config.NEIGHBOR_TIMEOUT_FACTOR * 2
        if current_time - self.last_heard > timeout:
            self.state = 'STALE'
            return False
        self.state = 'ACTIVE'
        return True
    
    def to_dict(self):
        return {
            'node_id': self.node_id,
            'via_neighbor': self.via_neighbor,
            'path_cost': self.path_cost,
            'last_heard': f"{self.last_heard:.2f}",
            'parent_seqno': self.parent_seqno,
            'hop_count': self.hop_count,
            'capabilities': self.capabilities,
            'state': self.state
        }


def calculate_lqi(distance, tx_range):
    """Simulate LQI based on distance (0-255, higher is better)."""
    if distance >= tx_range:
        return 0
    ratio = 1 - (distance / tx_range)
    return int(ratio * 255)


def calculate_rssi(distance, tx_power=0):
    """Simulate RSSI in dBm based on distance (path loss model)."""
    if distance <= 0:
        return tx_power
    # Simple path loss: RSSI = TxPower - 10 * n * log10(d) where n=2
    path_loss = 20 * math.log10(max(distance, 1))
    return int(tx_power - path_loss)


class SensorNode(wsn.Node):
    """Sensor node implementing neighbor discovery protocol."""
    
    def init(self):
        self.scene.nodecolor(self.id, 1, 1, 1)  # White initially
        self.sleep()
        
        # One-hop neighbor table: {node_id: NeighborEntry}
        self.one_hop_neighbors = {}
        
        # Two-hop neighbor table: {node_id: TwoHopNeighborEntry}
        self.two_hop_neighbors = {}
        
        # Node capabilities (8-bit flags)
        self.capabilities = 0x01  # Bit 0: Active
        
        self._hello_count = 0
        self._seqno = 0  # Sequence number for HELLO messages

    def run(self):
        arrival_time = random.uniform(0, config.NODE_ARRIVAL_MAX)
        self.set_timer('TIMER_ARRIVAL', arrival_time)

    def send_hello(self):
        """Send HELLO message with neighbor table for sharing."""
        self._seqno += 1
        hello_pck = {
            'dest': wsn.BROADCAST_ADDR,
            'type': 'HELLO',
            'sender_id': self.id,
            'seqno': self._seqno,
            'capabilities': self.capabilities,
            'timestamp': self.now
        }
        
        # Include 1-hop neighbor list for multi-hop table population
        if config.MULTI_HOP_NEIGHBOR_TABLE:
            hello_pck['one_hop_neighbor_ids'] = list(self.one_hop_neighbors.keys())
            hello_pck['one_hop_neighbor_caps'] = {
                nid: entry.capabilities for nid, entry in self.one_hop_neighbors.items()
            }
        
        self.send(hello_pck)
        self._hello_count += 1

    def receive_hello(self, pck):
        """Process received HELLO message and update neighbor tables."""
        sender_id = pck['sender_id']
        
        if sender_id == self.id:
            return
        
        # Calculate distance and signal metrics
        if sender_id in NODE_POS and self.id in NODE_POS:
            x1, y1 = NODE_POS[self.id]
            x2, y2 = NODE_POS[sender_id]
            distance = math.hypot(x1 - x2, y1 - y2)
        else:
            distance = 0
        
        lqi = calculate_lqi(distance, self.tx_range)
        rssi = calculate_rssi(distance)
        
        # Update or create 1-hop neighbor entry
        self.one_hop_neighbors[sender_id] = NeighborEntry(
            node_id=sender_id,
            distance=distance,
            lqi=lqi,
            rssi=rssi,
            timestamp=self.now,
            hop_count=1,
            capabilities=pck.get('capabilities', 0)
        )
        
        # Multi-hop neighbor table population via neighbor sharing
        if config.MULTI_HOP_NEIGHBOR_TABLE:
            shared_neighbors = pck.get('one_hop_neighbor_ids', [])
            shared_caps = pck.get('one_hop_neighbor_caps', {})
            parent_seqno = pck.get('seqno', 0)
            
            for two_hop_id in shared_neighbors:
                # Skip if it's us or already a 1-hop neighbor
                if two_hop_id == self.id:
                    continue
                if two_hop_id in self.one_hop_neighbors:
                    continue
                
                # Calculate path cost (sum of hop costs)
                sender_entry = self.one_hop_neighbors.get(sender_id)
                path_cost = (sender_entry.cost if sender_entry else 1) + 1
                
                # Get capabilities of the 2-hop neighbor from shared info
                two_hop_caps = shared_caps.get(two_hop_id, 0)
                
                # Update or create 2-hop neighbor entry
                existing = self.two_hop_neighbors.get(two_hop_id)
                if existing is None or path_cost < existing.path_cost:
                    self.two_hop_neighbors[two_hop_id] = TwoHopNeighborEntry(
                        node_id=two_hop_id,
                        via_neighbor=sender_id,
                        path_cost=path_cost,
                        timestamp=self.now,
                        hop_count=2,
                        parent_seqno=parent_seqno,
                        capabilities=two_hop_caps
                    )
        
        if config.ENABLE_LOGGING:
            mode = "MULTI-HOP" if config.MULTI_HOP_NEIGHBOR_TABLE else "SINGLE-HOP"
            self.log(f"[{mode}] Neighbor {sender_id} added/updated (LQI:{lqi}, RSSI:{rssi}dBm)")

    def age_neighbors(self):
        """Remove stale neighbors from tables."""
        # Age 1-hop neighbors
        stale_one_hop = []
        for node_id, entry in self.one_hop_neighbors.items():
            if not entry.is_valid(self.now):
                stale_one_hop.append(node_id)
        
        for node_id in stale_one_hop:
            if config.ENABLE_LOGGING:
                self.log(f"1-hop neighbor {node_id} aged out (STALE)")
            del self.one_hop_neighbors[node_id]
        
        # Age 2-hop neighbors
        if config.MULTI_HOP_NEIGHBOR_TABLE:
            stale_two_hop = []
            for node_id, entry in self.two_hop_neighbors.items():
                # Also remove if via_neighbor is no longer valid
                if entry.via_neighbor not in self.one_hop_neighbors:
                    stale_two_hop.append(node_id)
                elif not entry.is_valid(self.now):
                    stale_two_hop.append(node_id)
            
            for node_id in stale_two_hop:
                del self.two_hop_neighbors[node_id]

    def on_receive(self, pck):
        if pck.get('type') == 'HELLO':
            self.receive_hello(pck)

    def on_timer_fired(self, name, *args, **kwargs):
        if name == 'TIMER_ARRIVAL':
            self.scene.nodecolor(self.id, 0, 1, 0)  # Green when active
            self.wake_up()
            self.send_hello()
            self.set_timer('TIMER_HELLO', config.HELLO_INTERVAL)
        
        elif name == 'TIMER_HELLO':
            self.age_neighbors()
            self.send_hello()
            self.set_timer('TIMER_HELLO', config.HELLO_INTERVAL)

    def get_neighbor_summary(self):
        """Get summary of neighbor tables for export."""
        return {
            'node_id': self.id,
            'one_hop_count': len(self.one_hop_neighbors),
            'two_hop_count': len(self.two_hop_neighbors) if config.MULTI_HOP_NEIGHBOR_TABLE else 0,
            'one_hop_neighbors': [e.to_dict() for e in self.one_hop_neighbors.values()],
            'two_hop_neighbors': [e.to_dict() for e in self.two_hop_neighbors.values()] if config.MULTI_HOP_NEIGHBOR_TABLE else []
        }


def export_neighbor_tables(nodes, path=None):
    """Export all neighbor tables to CSV."""
    path = path or f"{config.METRICS_EXPORT_PATH}neighbor_tables_p1.csv"
    
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        
        # Header with all Appendix B fields
        w.writerow(['node_id', 'neighbor_type', 'neighbor_id', 'via_neighbor', 
                   'distance', 'lqi', 'rssi', 'hop_count', 'cost', 
                   'hello_interval', 'capabilities', 'parent_seqno', 'state', 'last_heard'])
        
        for node in nodes:
            # 1-hop neighbors
            for entry in node.one_hop_neighbors.values():
                w.writerow([
                    node.id, '1-HOP', entry.node_id, '-',
                    f"{entry.distance:.2f}", entry.lqi, entry.rssi,
                    entry.hop_count, entry.cost, 
                    entry.hello_interval, entry.capabilities, '-',
                    entry.state, f"{entry.last_heard:.2f}"
                ])
            
            # 2-hop neighbors (if multi-hop enabled)
            if config.MULTI_HOP_NEIGHBOR_TABLE:
                for entry in node.two_hop_neighbors.values():
                    w.writerow([
                        node.id, '2-HOP', entry.node_id, entry.via_neighbor,
                        '-', '-', '-',
                        entry.hop_count, entry.path_cost, 
                        '-', entry.capabilities, entry.parent_seqno,
                        entry.state, f"{entry.last_heard:.2f}"
                    ])


def print_neighbor_summary(nodes):
    """Print summary of all neighbor tables."""
    print("\n" + "="*60)
    print("NEIGHBOR TABLE SUMMARY")
    print(f"Mode: {'MULTI-HOP' if config.MULTI_HOP_NEIGHBOR_TABLE else 'SINGLE-HOP'}")
    print("="*60)
    
    total_1hop = 0
    total_2hop = 0
    
    for node in nodes:
        n1 = len(node.one_hop_neighbors)
        n2 = len(node.two_hop_neighbors) if config.MULTI_HOP_NEIGHBOR_TABLE else 0
        total_1hop += n1
        total_2hop += n2
        
        print(f"\nNode {node.id}:")
        print(f"  1-hop neighbors ({n1}): {list(node.one_hop_neighbors.keys())}")
        if config.MULTI_HOP_NEIGHBOR_TABLE:
            two_hop_via = {e.node_id: e.via_neighbor for e in node.two_hop_neighbors.values()}
            print(f"  2-hop neighbors ({n2}): {two_hop_via}")
    
    print("\n" + "-"*60)
    print(f"Average 1-hop neighbors per node: {total_1hop / len(nodes):.1f}")
    if config.MULTI_HOP_NEIGHBOR_TABLE:
        print(f"Average 2-hop neighbors per node: {total_2hop / len(nodes):.1f}")
    print("="*60)


def create_network(node_class, number_of_nodes):
    """Create network with nodes in grid layout."""
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


# ============== MAIN ==============

sim = wsn.Simulator(
    duration=config.SIM_DURATION,
    timescale=config.SIM_TIME_SCALE,
    visual=config.SIM_VISUALIZATION,
    terrain_size=config.SIM_TERRAIN_SIZE,
    title=config.SIM_TITLE
)

create_network(SensorNode, config.SIM_NODE_COUNT)

print(f"Part 1: Neighbor Discovery Protocol")
print(f"Nodes: {config.SIM_NODE_COUNT}")
print(f"Mode: {'MULTI-HOP' if config.MULTI_HOP_NEIGHBOR_TABLE else 'SINGLE-HOP'}")
print(f"Max hops tracked: {config.MAX_NEIGHBOR_HOPS if config.MULTI_HOP_NEIGHBOR_TABLE else 1}")
print("Starting simulation...")

sim.run()

print("\n=== Simulation Finished ===")
print_neighbor_summary(sim.nodes)

if config.EXPORT_NEIGHBOR_TABLE:
    export_neighbor_tables(sim.nodes)
    print(f"\nNeighbor tables exported to: {config.METRICS_EXPORT_PATH}neighbor_tables_p1.csv")

