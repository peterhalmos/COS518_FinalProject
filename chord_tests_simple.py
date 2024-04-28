import simgrid
import VanillaChordIntegrated as VCI
import random
import time
import os

node_ids = []

e = simgrid.Engine(["chord_tests_platform.xml", "chord_tests_deployment.xml"])

NUM_NODES = 1000
NUM_CLIENTS = 100
platform_desc = \
"""<?xml version='1.0'?>
<!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">
<platform version="4.1">
    <zone id="zone0" routing="Floyd">
        <!-- Central node for routing -->
        <host id="central_node" speed="100000Gf"/>\n\n\t""" + \
        "\n\t".join([f'<host id="host{i}" speed="1Gf" />' for i in range(NUM_NODES)]) + "\n\t" + \
        "\n\t".join([f'<host id="clienthost{i}" speed="1Gf" />' for i in range(NUM_CLIENTS)]) + "\n\t" + \
        "\n\t".join([f'<link id="link{i}" bandwidth="10Gbps" latency="10ms"/>' for i in range(NUM_NODES)]) + \
        "\n\t".join([f'<link id="clientlink{i}" bandwidth="10Gbps" latency="10ms"/>' for i in range(NUM_CLIENTS)]) + \
        "\n\t".join([f'<route src="host{i}" dst="central_node"> <link_ctn id="link{i}"/> </route>' for i in range(NUM_NODES)]) + \
        "\n\t".join([f'<route src="clienthost{i}" dst="central_node"> <link_ctn id="link{i}"/> </route>' for i in range(NUM_CLIENTS)]) + \
        """
    </zone>
</platform>
"""
TMP_PLATFORM_FILE = "tmp_platform_desc.txt"
with open(TMP_PLATFORM_FILE, "w") as f:
    f.write(platform_desc)
e.load_platform(TMP_PLATFORM_FILE)
#os.remove(TMP_PLATFORM_FILE)


actor_ids = []
client_ids = []

actor_ids.append(random.randrange(0, VCI.Q))
simgrid.Actor.create(f"0", simgrid.Host.by_name(f"host0"), VCI.Chord_Node(ID=actor_ids[-1], join_target=None))
for i in range(1, NUM_NODES):
    actor_ids.append(random.randrange(0, VCI.Q))
    simgrid.Actor.create(f"{i}", simgrid.Host.by_name(f"host{i}"), VCI.Chord_Node(ID=actor_ids[-1], join_target=actor_ids[-2]))
for i in range(NUM_CLIENTS):
    client_ids.append(random.randrange(0, VCI.Q))
    simgrid.Actor.create(f"client_{i}", simgrid.Host.by_name(f"clienthost{i}"), VCI.Client(ID=client_ids[-1], target=actor_ids[i % len(actor_ids)]))
e.run()