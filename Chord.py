from time import sleep
import random
import simgrid
import math

"""
All messages are tuples, where:
-the first argument is the message id (except in the case of RESP, where it is the id of the original)
-the second is the message type
-the third is the id of the initial querier (i.e. the return address in most cases)
-the subsequent are any other necessary data.
"""

KEY_SIZE = 40  # Bitlength of keys
NUM_SUCCESSORS = 10

# Message types
START = 0  # Presumed to be the first message that a node receives. Useful exclusively as a result of the weird simulation environment -- this lets us assign node IDs and join_targets.
KILL = 1
RESP = 2
PING = 3
FIND_SUCC = 4
FIND_PRED = 5
STORE = 6  # Store this key-value pair, and tell your successors to do the same
STORE_NOBROADCAST = 7  # Store this key-value pair, and do *not* tell your successors to do the same.
GET_FINGER_TABLE = 8  # Get the target's finger table
ADD_ME = 9  # Add the included id to your predecessor, successor lists, finger tables, etc. (if you should)





def dist_ahead(start, end):
    """
    For cyclic distance
    """
    return (end - start) % (1 << KEY_SIZE)


class Node:
    """
    We'll end up passing this entire thing as an actor (on its own host).

    Doing all of our calls as recursive (as opposed to bounceback) means that it'll be much easier to run in parallel (we won't be able to detect when calls fail except at source, but that's ok -- assuming our dropout rate stays significantly below 1/lg n, calls should mostly succeed.
    """
    def __init__(self):
        _, _, _, self.id, join_target = simgrid.Mailbox.by_name("init").get()  # Wait until we capture a "turn on" message to actually start up


        self.message_ct = 0

        self.mailbox = simgrid.Mailbox.by_name(str(self.id))
        simgrid.Mailbox.set_receiver(self.mailbox, simgrid.Actor.self())  # Should allow us to do blocking puts.
        self.message_queue = []


        self.finger_table = []
        self.key_value_store = {}
        self.predecessor = None
        self.successors = []
        print("Starting join...")
        self.join(join_target)
        print("Finished join! ID:", self.id)

        self.outstanding_calls = []  # We need to run all of our network calls in a nonblocking manner.

        done = False
        while not done:
            if len(self.message_queue) > 0:
                message = self.message_queue.pop(0)
            else:
                message = self.mailbox.get()
            message_id = message[0]
            message_type = message[1]
            queryer = message[2]
            print(self.id, "received message. ID:", message_id, "type:", message_type, "From ", queryer)
            if message_type == KILL:
                done = True
            elif message_type == RESP:
                print("No general RESP handling for nodes!")
            elif message_type == PING:
                self.send_message(queryer, (message_id, RESP, self.id))
                print("Received ping. Predecessor:", self.predecessor, "Successors:", self.successors, "Finger table:", self.finger_table)
            elif message_type == FIND_SUCC or message_type == FIND_PRED:
                target = message[3]
                print("id, pred:", self.id, self.predecessor)
                if self.id == target:
                    self.send_message(queryer, (message_id, RESP, self.id, self.id))
                if self.predecessor is None or dist_ahead(self.id, target) > dist_ahead(self.predecessor, target):
                    self.send_message(queryer, (message_id, RESP, self.id, self.id if (message_type == FIND_SUCC or self.predecessor is None) else self.predecessor))  # None-catch here because the first node to join will be asked for its predecessor by the second, and it won't have one.
                else:
                    print("DA:", dist_ahead(self.id, target), self.id, target)
                    i = math.floor(math.log2(dist_ahead(self.id, target)))
                    if self.finger_table[i] != self.id:
                        self.send_message(self.finger_table[i], message)
                    else:
                        self.send_message(queryer, (message_id, RESP, self.id, self.id if (message_type == FIND_SUCC or self.predecessor is None) else self.predecessor))
                        
            elif message_type == GET_FINGER_TABLE:
                print("Received finger table query with finger table:", self.finger_table)
                self.send_message(queryer, (message_id, RESP, self.id, tuple(self.finger_table)))
    
            elif message_type == ADD_ME:
                if queryer == self.id:  # If it's been passed all the way around in a loop, ignore it.
                    continue
                changed = False
                if self.predecessor is None or dist_ahead(self.predecessor, self.id) > dist_ahead(queryer, self.id):
                    self.predecessor = queryer
                    changed = True
                dist_to_queryer = dist_ahead(self.id, queryer)
                if len(self.successors) == 0 or (dist_to_queryer <= dist_ahead(self.id, self.successors[-1]) and queryer not in self.successors):  # This won't work if we let self.successors loop or have duplicates
                    self.successors = sorted(list(set(self.successors + [queryer])), key=lambda x: dist_ahead(self.id, x))[:NUM_SUCCESSORS]
                    changed = True
                for i in range(KEY_SIZE):
                    if dist_ahead(self.id, self.finger_table[i]) > dist_to_queryer and dist_to_queryer >= 1<<i:
                        self.finger_table[i] = queryer
                        changed = True
                if changed:
                    self.send_message(self.predecessor, message)  # Pass it back -- important for both successor updating and finger table updating (more for the former).
            else:
                print("Message type", message_type, "unrecognized!")

            """
            We'll want this looping forever -- it'll be constantly querying for messages and responding to them.
            """



    def query(self, k):
        return

    def find_succ(self, k):
        return
    def find_pred(self, k):
        return
    def closest_preceeding_finger(self):
        return

    def join(self, join_target):
        if join_target is None:
            self.finger_table = [self.id] * KEY_SIZE
        else:
            self.init_finger_table(join_target)
            print("Finished init finger table.")
            self.successors = [self.finger_table[0]]
            while len(self.successors) < NUM_SUCCESSORS:
                self.successors.append(self.send_message_await_resp(self.successors[-1] if len(self.successors) > 0 else join_target, (self.new_mid(), FIND_SUCC, self.id, self.successors[-1]+1))[3])
            for target in [self.predecessor] + self.successors[:1] + [self.send_message_await_resp(join_target, (self.new_mid(), FIND_PRED, self.id, self.id - (1<<i)))[3] for i in range(KEY_SIZE)]:
                self.send_message(target, (self.new_mid(), ADD_ME, self.id))
                print("Noted self to others.")
        return

    def init_finger_table(self, join_target):
        self.predecessor = self.send_message_await_resp(join_target, (self.new_mid(), FIND_PRED, self.id, self.id))[3]
        pred_finger_table = list(self.send_message_await_resp(self.predecessor, (self.new_mid(), GET_FINGER_TABLE, self.id))[3])
        print("Got pred finger table:", pred_finger_table)
        self.finger_table = []
        for i in range(KEY_SIZE):
            self.finger_table.append(self.send_message_await_resp(pred_finger_table[i], (self.new_mid(), FIND_SUCC, self.id, self.id + (1 << i)))[3])

        print("Finger Table Update Completed")
        return

    def update_finger_table(self):
        return
    def update_others(self):
        return
    def stabilize(self):
        return
    def notify(self):
        return
    def fix_fingers(self):
        return

    def new_mid(self):
        """
        Generates a globally unique message ID
        """
        self.message_ct += 1
        return (self.message_ct << KEY_SIZE) + self.id

    def send_message(self, target_id, message):
        """
        Given a message, sends it. Note that most of the time, we want to generate a new id, but sometimes, we need to take one as given (for RESP)
        """
        target_mailbox = simgrid.Mailbox.by_name(str(target_id))
        target_mailbox.put_async(message, 0)

    def await_resp(self, message_id):
        """
        Awaits message response, returning the eventual response
        """
        print("Awaiting message id:", message_id)
        while 1:
            message = self.mailbox.get()
            print("Received message id", message[0])
            if message[0] == message_id:
                return message
            else:
                self.message_queue.append(message)

    def send_message_await_resp(self, target_id, message):
        """
        Combines the above two functions.
        """
        print("Sending message:", message)
        target_mailbox = simgrid.Mailbox.by_name(str(target_id))
        target_mailbox.put_async(message, 0)
        return self.await_resp(message[0])
