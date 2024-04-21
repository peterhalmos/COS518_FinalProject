import simgrid
import Chord
import random
import time

node_ids = []

e = simgrid.Engine(["chord_tests_platform.xml", "chord_tests_deployment.xml"])
e.register_actor("node", Chord.Node)


def master_fn():
    init_mailbox = simgrid.Mailbox.by_name("init")
    ids = [random.randrange(0, 1 << Chord.KEY_SIZE) for _ in range(5)]
    init_mailbox.put((-1, Chord.START, -1, ids[0], None), 0)  # Start one of the processes.
    for i in range(1, 5):
        init_mailbox.put((-1, Chord.START, -1, ids[i], ids[i-1]), 0)  # Start one of the processes.
    time.sleep(3)
    for i in range(5):
        simgrid.Mailbox.by_name(str(ids[i])).put((-1, Chord.PING, -1), 0)
    


e.register_actor("master", master_fn)




e.load_platform("chord_tests_platform.xml")
e.load_deployment("chord_tests_deployment.xml")
print("Loaded. Running...")
e.run()