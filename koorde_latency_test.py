import simgrid
import VanillaKoordeIntegrated as VKI
import random
import sys, os

def average(l):
    return sum(l) / len(l)

if __name__ == "__main__":
    NUM_NODES = int(sys.argv[1])
    NUM_CLIENTS = 1
    
    platform_desc = \
    """<?xml version='1.0'?>
    <!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">
    <platform version="4.1">
        <zone id="zone0" routing="Floyd">
            <!-- Central node for routing -->
            <host id="central_node" speed="1000000000Tf"/>\n\n\t""" + \
            "\n\t".join([f'<host id="host{i}" speed="100Tf" />' for i in range(NUM_NODES)]) + "\n\t" + \
            "\n\t".join([f'<host id="clienthost{i}" speed="100Tf" />' for i in range(NUM_CLIENTS)]) + "\n\t" + \
            "\n\t".join([f'<link id="link{i}" bandwidth="10Gbps" latency="1ms"/>' for i in range(NUM_NODES)]) + \
            "\n\t".join([f'<link id="clientlink{i}" bandwidth="10Gbps" latency="1ms"/>' for i in range(NUM_CLIENTS)]) + \
            "\n\t".join([f'<route src="host{i}" dst="central_node"> <link_ctn id="link{i}"/> </route>' for i in range(NUM_NODES)]) + \
            "\n\t".join([f'<route src="clienthost{i}" dst="central_node"> <link_ctn id="clientlink{i}"/> </route>' for i in range(NUM_CLIENTS)]) + \
            """
        </zone>
    </platform>
    """
    TMP_PLATFORM_FILE = "tmp_platform_desc.txt"
    with open(TMP_PLATFORM_FILE, "w") as f:
        f.write(platform_desc)
    e = simgrid.Engine(["chord_tests_platform.xml", "chord_tests_deployment.xml"])
    e.load_platform(TMP_PLATFORM_FILE)
    os.remove(TMP_PLATFORM_FILE)

    server_ids = []
    client_ids = []

    server_ids.append(random.randrange(0, VKI.Q))
    simgrid.Actor.create(f"0", simgrid.Host.by_name(f"host0"), VKI.Koorde_Node(ID=server_ids[-1], join_target=None))
    for i in range(1, NUM_NODES):
        server_ids.append(random.randrange(0, VKI.Q))
        simgrid.Actor.create(f"{i}", simgrid.Host.by_name(f"host{i}"), VKI.Koorde_Node(ID=server_ids[-1], join_target=server_ids[-2]))

    #actor_data = []
    monitor_object = VKI.Monitor(num_clients=NUM_CLIENTS, server_ids=server_ids)
    monitor = simgrid.Actor.create("monitor", simgrid.Host.by_name("central_node"), monitor_object)


    for i in range(NUM_CLIENTS):
        client_ids.append(random.randrange(0, VKI.Q))
        simgrid.Actor.create(f"client_{i}", simgrid.Host.by_name(f"clienthost{i}"), VKI.Client(ID=client_ids[-1], target=server_ids[i % len(server_ids)]))
    e.run()
    flat_read_latencies = [x for y in monitor_object.info_dicts["read_latencies"] for x in y]
    #sys.stdout.write(str((sum(flat_read_latencies) / len(flat_read_latencies)))
    sys.stdout.write(str(average(flat_read_latencies)))
    sys.stdout.flush()
    sys.exit(0)