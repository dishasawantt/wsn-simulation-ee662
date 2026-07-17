"""
Metrics Analysis Tool for Assignment 2
Analyzes simulation logs and generates detailed reports
"""

import json
import statistics

def analyze_metrics(logger):
    """Analyze and generate comprehensive metrics report"""
    
    print("\n" + "="*80)
    print("DETAILED METRICS ANALYSIS")
    print("="*80)
    
    # 1. Join Time Analysis
    print("\n1. JOIN TIME METRICS")
    print("-" * 40)
    if logger.join_times:
        join_times = [j['time'] for j in logger.join_times]
        print(f"   Total nodes joined: {len(join_times)}")
        print(f"   Average join time: {statistics.mean(join_times):.2f}s")
        print(f"   Median join time: {statistics.median(join_times):.2f}s")
        print(f"   Min join time: {min(join_times):.2f}s")
        print(f"   Max join time: {max(join_times):.2f}s")
        if len(join_times) > 1:
            print(f"   Std deviation: {statistics.stdev(join_times):.2f}s")
    else:
        print("   No join events recorded")
    
    # 2. Packet Statistics
    print("\n2. PACKET STATISTICS")
    print("-" * 40)
    packets_sent = sum(1 for p in logger.packets if p['event'] == 'sent')
    packets_received = sum(1 for p in logger.packets if p['event'] == 'received')
    packets_dropped = sum(1 for p in logger.packets if p['event'] == 'dropped')
    total_packets = packets_sent + packets_dropped
    
    print(f"   Packets sent: {packets_sent}")
    print(f"   Packets received: {packets_received}")
    print(f"   Packets dropped: {packets_dropped}")
    if total_packets > 0:
        print(f"   Delivery ratio: {packets_received/total_packets*100:.1f}%")
        print(f"   Loss ratio: {packets_dropped/total_packets*100:.1f}%")
    
    # Packet types analysis
    packet_types = {}
    for p in logger.packets:
        if p['event'] == 'sent':
            ptype = p['packet'].get('type', 'UNKNOWN')
            packet_types[ptype] = packet_types.get(ptype, 0) + 1
    
    print("\n   Packet types sent:")
    for ptype, count in sorted(packet_types.items(), key=lambda x: x[1], reverse=True):
        print(f"     {ptype:20s}: {count:5d}")
    
    # 3. Path Trace Analysis
    print("\n3. PATH TRACE ANALYSIS")
    print("-" * 40)
    print(f"   Total paths traced: {len(logger.paths)}")
    
    if logger.paths:
        path_lengths = []
        mesh_routes = 0
        tree_routes = 0
        
        for packet_id, path in logger.paths.items():
            path_lengths.append(len(path))
            # Check if packet used mesh or tree routing
            for p in logger.packets:
                if p['packet'].get('packet_id') == packet_id:
                    routing_type = p['packet'].get('routing_type', 'unknown')
                    if routing_type == 'mesh':
                        mesh_routes += 1
                    elif routing_type == 'tree':
                        tree_routes += 1
                    break
        
        if path_lengths:
            print(f"   Average path length: {statistics.mean(path_lengths):.2f} hops")
            print(f"   Min path length: {min(path_lengths)} hops")
            print(f"   Max path length: {max(path_lengths)} hops")
        
        print(f"   Mesh routes: {mesh_routes}")
        print(f"   Tree routes: {tree_routes}")
        
        # Show sample paths
        print("\n   Sample packet paths (first 10):")
        for i, (packet_id, path) in enumerate(list(logger.paths.items())[:10]):
            path_str = " -> ".join(str(p['node']) for p in path)
            print(f"     Packet {packet_id:3d}: {path_str}")
    
    # 4. Failure Recovery Analysis
    print("\n4. FAILURE RECOVERY ANALYSIS")
    print("-" * 40)
    print(f"   Total orphan events: {len(logger.orphan_events)}")
    
    if logger.orphan_events:
        print("\n   Orphan events:")
        for event in logger.orphan_events[:10]:
            print(f"     Node {event['node']:3d} at {event['time']:8.2f}s - {event['reason']}")
        if len(logger.orphan_events) > 10:
            print(f"     ... and {len(logger.orphan_events) - 10} more")
    
    # Recovery time analysis
    recovery_times = []
    orphan_dict = {}
    for event in logger.orphan_events:
        orphan_dict[event['node']] = event['time']
    
    for join in logger.join_times:
        if join['node'] in orphan_dict:
            recovery_time = join['time'] - orphan_dict[join['node']]
            if recovery_time > 0:
                recovery_times.append(recovery_time)
    
    if recovery_times:
        print(f"\n   Recovery time statistics:")
        print(f"     Average recovery time: {statistics.mean(recovery_times):.2f}s")
        print(f"     Min recovery time: {min(recovery_times):.2f}s")
        print(f"     Max recovery time: {max(recovery_times):.2f}s")
    
    # 5. Role Change Analysis
    print("\n5. ROLE CHANGE ANALYSIS")
    print("-" * 40)
    print(f"   Total role changes: {len(logger.role_changes)}")
    
    role_transitions = {}
    for change in logger.role_changes:
        transition = f"{change['old'].name} -> {change['new'].name}"
        role_transitions[transition] = role_transitions.get(transition, 0) + 1
    
    if role_transitions:
        print("\n   Role transitions:")
        for transition, count in sorted(role_transitions.items(), key=lambda x: x[1], reverse=True):
            print(f"     {transition:40s}: {count:3d}")
    
    # Sample role changes
    if logger.role_changes:
        print("\n   Sample role changes (first 10):")
        for change in logger.role_changes[:10]:
            print(f"     Node {change['node']:3d} at {change['time']:8.2f}s: {change['old'].name} -> {change['new'].name}")
    
    # 6. Energy Analysis
    print("\n6. ENERGY ANALYSIS")
    print("-" * 40)
    print(f"   Energy log entries: {len(logger.energy_log)}")
    
    if logger.energy_log:
        nodes_died = set(e['node'] for e in logger.energy_log if e['energy'] == 0)
        print(f"   Nodes died from energy depletion: {len(nodes_died)}")
        
        if nodes_died:
            death_times = []
            for node in nodes_died:
                for event in logger.energy_log:
                    if event['node'] == node and event['energy'] == 0:
                        death_times.append(event['time'])
                        break
            
            if death_times:
                print(f"   First node death: {min(death_times):.2f}s")
                print(f"   Last node death: {max(death_times):.2f}s")
                print(f"   Network lifetime (first critical death): {min(death_times):.2f}s")
        
        print("\n   Node deaths:")
        for event in logger.energy_log:
            if event['energy'] == 0:
                print(f"     Node {event['node']:3d} at {event['time']:8.2f}s")
    
    # 7. Network Topology Metrics
    print("\n7. NETWORK TOPOLOGY METRICS")
    print("-" * 40)
    
    # Count nodes in each role at the end
    role_counts = {}
    for change in logger.role_changes:
        role_counts[change['node']] = change['new']
    
    role_distribution = {}
    for role in role_counts.values():
        role_distribution[role.name] = role_distribution.get(role.name, 0) + 1
    
    if role_distribution:
        print("   Final role distribution:")
        for role, count in sorted(role_distribution.items()):
            print(f"     {role:20s}: {count:3d}")
    
    # Cluster head count
    cluster_heads = sum(1 for role in role_counts.values() if role.name == 'CLUSTER_HEAD')
    registered = sum(1 for role in role_counts.values() if role.name == 'REGISTERED')
    
    if cluster_heads > 0:
        print(f"\n   Total clusters: {cluster_heads + 1}")  # +1 for root
        if registered > 0:
            print(f"   Average cluster size: {registered/cluster_heads:.1f} nodes")
    
    print("\n" + "="*80)
    print("END OF ANALYSIS")
    print("="*80)

def export_to_json(logger, filename='simulation_results.json'):
    """Export logger data to JSON file"""
    data = {
        'packets': logger.packets,
        'join_times': logger.join_times,
        'paths': {str(k): v for k, v in logger.paths.items()},
        'orphan_events': logger.orphan_events,
        'role_changes': [(r['node'], r['old'].name, r['new'].name, r['time']) 
                         for r in logger.role_changes],
        'energy_log': logger.energy_log
    }
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"\nData exported to {filename}")

def export_to_csv(logger, base_filename='simulation'):
    """Export logger data to CSV files"""
    import csv
    
    # Join times
    with open(f'{base_filename}_join_times.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Node', 'Join Time (s)'])
        for j in logger.join_times:
            writer.writerow([j['node'], j['time']])
    
    # Orphan events
    with open(f'{base_filename}_orphan_events.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Node', 'Time (s)', 'Reason'])
        for e in logger.orphan_events:
            writer.writerow([e['node'], e['time'], e['reason']])
    
    # Role changes
    with open(f'{base_filename}_role_changes.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Node', 'Old Role', 'New Role', 'Time (s)'])
        for r in logger.role_changes:
            writer.writerow([r['node'], r['old'].name, r['new'].name, r['time']])
    
    # Energy log
    with open(f'{base_filename}_energy.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Node', 'Energy (mJ)', 'Time (s)'])
        for e in logger.energy_log:
            writer.writerow([e['node'], e['energy'], e['time']])
    
    print(f"\nData exported to {base_filename}_*.csv files")

# This can be imported and used after simulation
if __name__ == '__main__':
    print("This module provides analysis tools for simulation logs")
    print("Import and use after running a simulation:")
    print("  from metrics_analysis import analyze_metrics, export_to_json, export_to_csv")
    print("  analyze_metrics(logger)")
    print("  export_to_json(logger)")
    print("  export_to_csv(logger)")

