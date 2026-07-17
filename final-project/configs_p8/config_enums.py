from enum import Enum

role_types = ['UNDISCOVERED', 'UNREGISTERED', 'ROOT', 'REGISTERED', 'CLUSTER_HEAD', 'ROUTER', 'DEAD']
Roles = Enum('Roles', role_types)

packet_types = ['PROBE', 'HEART_BEAT', 'JOIN_REQUEST', 'JOIN_REPLY', 'JOIN_ACK',
                'NETWORK_REQUEST', 'NETWORK_REPLY', 'NETWORK_UPDATE', 'DATA', 'ACK',
                'CH_HANDOFF_REQUEST', 'CH_HANDOFF_ACCEPT', 'CH_HANDOFF_COMPLETE',
                'CLUSTER_MERGE_REQUEST', 'CLUSTER_MERGE_ACCEPT', 'CLUSTER_MERGE_NOTIFY',
                'MEMBER_TRANSFER_REQUEST', 'I_AM_ORPHAN', 'ROUTER_REGISTER',
                'BECOME_ROUTER', 'BECOME_CH']
PacketType = Enum('PacketType', packet_types)

timer_types = ['ARRIVAL_TIMER', 'PROBE_TIMER', 'HEART_BEAT_TIMER', 'JOIN_REQUEST_TIMER',
               'DATA_PACKET_TIMER', 'RETRANSMIT_TIMER', 'HANDOFF_TIMEOUT_TIMER',
               'RECOVER_TIMER', 'ORPHAN_CHECK_TIMER', 'OPTIMIZATION_TIMER', 'MERGE_TIMEOUT_TIMER']
TimerType = Enum('TimerType', timer_types)

