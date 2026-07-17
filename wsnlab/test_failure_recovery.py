import random
from enum import Enum
import sys
import math
sys.path.insert(1, '.')
from source import wsnlab_vis as wsn
from source import config

config.SIM_NODE_COUNT = 30
config.SIM_DURATION = 600
config.SIM_VISUALIZATION = True
config.ENABLE_ENERGY_MODEL = False

exec(open('assignment2_implementation.py').read())

print("\n" + "="*80)
print("FAILURE RECOVERY TEST")
print("="*80)
print("This test will randomly kill a node at t=200s and revive it at t=400s")
print("="*80 + "\n")

def kill_random_node():
    alive_nodes = [n for n in sim.nodes if not n.is_sleep and n.id != ROOT_ID]
    if alive_nodes:
        victim = random.choice(alive_nodes)
        victim.log(f'*** NODE KILLED FOR TESTING ***')
        logger.log_orphan(victim.id, victim.now, 'Killed for testing')
        victim.scene.nodecolor(victim.id, 0.3, 0.3, 0.3)
        victim.sleep()
        victim.erase_parent()
        victim.kill_all_timers()
        if victim.ch_addr:
            victim.send({'dest': wsn.BROADCAST_ADDR, 'type': 'I_AM_ORPHAN', 'source': victim.ch_addr, 'timestamp': victim.now})
        return victim
    return None

def revive_node(node):
    if node:
        node.log(f'*** NODE REVIVED ***')
        node.wake_up()
        node.scene.nodecolor(node.id, 1, 0, 0)
        node.become_unregistered()

killed_node = None

def test_failure():
    global killed_node
    yield sim.timeout(200)
    killed_node = kill_random_node()
    yield sim.timeout(200)
    revive_node(killed_node)

sim.env.process(test_failure())

