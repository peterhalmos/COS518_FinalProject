'''
A Vanilla Implementation of Chord.

We extend this code for use on simgrid and realistic emulators.
'''
import simgrid
from random import sample, randrange

global M, Q
global KILL, RESP, RPC, RPC_NORESP, PING, PING_NORESP, FINGER_PRINT

M = 50
Q = 2 ** M

# Idea: when parent needs to do something blocking, it spawns a child, does what it needs to do, passes any updates to the child, and dies.
# Messages are structured as ((msg_id, return_mailbox_id), msg_type, payload). Typically, resps (which are to blocking queries) will want to go to a separate inbox
# Types:
KILL = 0
RESP = 1  # These share the msg_id of the message that prompted them, and have no return_mailbox_id
RPC = 2
RPC_NORESP = -2
PING = 3
PING_NORESP = -3
FINGER_PRINT = 4

# NOTE: Leon points out that generators can take inputs as a response to yield with .continue, 
# and that this is the perfect mechanism for running async functions with many blocking calls. May be dramatically easier than debugging the fork-and-die scheme.

class Chord_Node(): # will super() this once Koorde is done..
    def __init__(self, ID, FingerTable=None, predecessor=None, num_sent=-1, storage={}, join_target=None): 
        # Add SHA-hashing specific functions later;
        # for now assume that the ID is given

        # Set the identifer to be a number modulo 2^m (keyspace's modular interval)
        self.ID = ID % Q
        self.mailbox = simgrid.Mailbox.by_name(str(self.ID))
        self.mailbox.set_receiver(simgrid.this_actor.get_host())
        self.FingerTable = FingerTable
        self.predecessor = predecessor
        self.num_sent = num_sent + 100 * M  # Leave some room for the parent to send messages without treading on our ID.
        self.storage = storage
        self.join_target = join_target
    def __call__(self):
        if self.FingerTable is None: # If we're joining for the first time (not a child)
            # Initialize Finger Table
            start, end = self.ID+1,self.ID+2
            # Leave 'finger' entry unfilled until init_finger_table call
            self.FingerTable = {'start':[start], 'interval':[(start,end)], 'finger':[None]*M}
            # Initialize predecessor to NIL
            self.predecessor = None
            self.num_sent = 0

            for i in range(1, M, 1):
                # To be consistent with the paper,
                # we keep both start and interval: (start,end) entries
                start = self.FingerTable['interval'][i-1][-1]
                self.FingerTable['start'].append(start)
                end = (start + 2**i)%Q
                self.FingerTable['interval'].append((start,end))
            self.storage = {}
            self.join(self.join_target)
        self.print("Finished setup")

        last_stab_time = 0

        old_network_fixer_actors = []
        while 1:
            msg = self.mailbox.get()
            msg_id, sender_id = msg[0]
            msg_type = msg[1]
            payload = msg[2]
            if msg_type == KILL:
                return
            elif msg_type == RESP:
                self.print("Received response in processing loop! Exiting.")
                print(msg)
                return
            elif msg_type == PING:
                self.send_resp(msg_id, sender_id, None)
            elif msg_type == PING_NORESP:  # This is just supposed to break us out of the loop -- that's it. Allows us to stabilize the network by periodic pings.
                pass
            elif msg_type == FINGER_PRINT:
                self.print_fingers()
            elif msg_type == RPC or msg_type == RPC_NORESP:
                """
                We have two types of procedures -- those that modify internal state, and those that call externally. Except for stabilizing (which we'll treat separately), none do both. 
                """
                if payload[0] in ["blocking_request", "blocking_rpc", "find_succ", "find_pred"]:
                    simgrid.Actor.create(f"responder_{randrange(0, 1<<64)}", simgrid.this_actor.get_host(), self.call_and_resp, msg_id, sender_id, payload)
                else:  # Otherwise, we might modify state (and we won't block), so we do it in thread.
                    if msg_type == RPC:
                        self.call_and_resp(msg_id, sender_id, payload)
                    else:
                        self.call(payload[0], payload[1])


            else:
                self.print("Unknown message type! Exiting.")
                print(msg)
                return
            if simgrid.Engine.clock > last_stab_time + 100:
                old_network_fixer_actors.append(simgrid.Actor.create(f"stabilizer_{self.ID}", simgrid.this_actor.get_host(), self.stabilize))
                old_network_fixer_actors.append(simgrid.Actor.create(f"finger_fixer_{self.ID}", simgrid.this_actor.get_host(), self.fix_fingers))
                last_stab_time = simgrid.Engine.clock
            
    def print(self, s):
        print(f"[{simgrid.Engine.clock}] {self.ID}: " + s)

    def call(self, fname, fargs):
        res = getattr(self, fname)(*fargs)
        return res

    def var_val(self, var_name):
        return getattr(self, var_name)
    
    def set_var(self, var_name, val):
        return setattr(self, var_name, val)

    def new_msg_id(self):
        # return randrange(0, 1<<64)
        self.num_sent += 1
        return self.num_sent * Q + self.ID

    def request(self, target_ID, msg_type, payload):
        mid = self.new_msg_id()
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        if target_ID is None:
            raise ArithmeticError
        target_mailbox.put(((mid, mid), msg_type, payload), 0)
        if msg_type in [RPC, PING]:  # Message types that obtain a response:
            blocking_mailbox = simgrid.Mailbox.by_name(str(mid))  # A mailbox unique to this message.
            resp = blocking_mailbox.get()
            return resp[2]

    def blocking_rpc(self, target_ID, func, args):
        if self.ID == target_ID:  # So that we don't block on RPCs to self.
            return self.call(func, args)
        else:
            return self.request(target_ID, RPC, (func, args))
        
    def noreturn_rpc(self, target_ID, func, args):  # Not blocking, will not get a response.
        if self.ID == target_ID:
            self.call(func, args)
        else:
            self.request(target_ID, RPC_NORESP, (func, args))
        return

    def send_resp(self, msg_id, target, payload):
        target_mailbox = simgrid.Mailbox.by_name(str(target))
        target_mailbox.put(((msg_id, None), RESP, payload), 0)
        return

    
    def call_and_resp(self, msg_id, target, payload):  # Async wrapper
        r = self.send_resp(msg_id, target, self.call(payload[0], payload[1]))
        return 

    def spawn_child(self):
        """
        Only use if we're about to finish up and die -- we can't process any more messages on the main mailbox after this point. 
        We determine this at the top level, in our control loop.
        """
        simgrid.Actor.create(simgrid.this_actor.get_host(), self.__class__(ID=self.ID, FingerTable=self.FingerTable, predecessor=self.predecessor, num_sent=self.num_sent, storage=self.storage))

    def check_mod_interval(self, x, init, end, left_open=True, right_open=True):
        # Check the modular rotation for each case
        '''Check the modular rotation for each case
         1. [init, end), 2. (init, end], 3. [init, end], 4. (init, end)
        '''
        # Edge case: init==end is True except case where interval open, and x = init = end
        if init == end:
            if (left_open and right_open and x == init):
                return False
            else:
                return True
        # Otherwise, check normally: init =/= end
        if right_open and not left_open:
            return (x - init)%Q < (end - init)%Q
        elif left_open and not right_open:
            return (end - x)%Q < (end - init)%Q
        elif not left_open and not right_open:
            return (x - init)%Q <= (end - init)%Q
        else:
            b = (x - end)%Q <= (init - end)%Q
            return not b
    
    def successor(self):
        # return successor from finger table
        return self.FingerTable['finger'][0]
    
    def find_succ(self, ID):
        if ID == self.ID:
            # Return self's successor if ID equal
            return self.successor()
        elif self.check_mod_interval(ID, self.predecessor,\
                                     self.ID, \
                                     left_open=True, right_open=False):
            # Return self if in interval (self.pred.ID, self.ID]
            return self.ID
        else:
            # Otherwise, invoke find_predecessor to recursively search for ID's pred and find its successor pointer
            return self.blocking_rpc(self.find_pred(ID), "successor", tuple())
    
    def find_pred(self, ID):
        if ID == self.ID:
            return self.predecessor
        n_prime = self.ID
        n = None
        while n != n_prime:
            # Keep searching
            n = n_prime
            n_prime = self.blocking_rpc(n_prime, "closest_preceeding_finger", (ID,))

        return n_prime
    
    def closest_preceeding_finger(self, ID):
        '''
        Find closest finger with identifier preceding ID
        '''
        for i in range(M-1, -1, -1):
            if self.check_mod_interval(self.FingerTable['finger'][i],\
                                  self.ID, ID, \
                                  left_open=True, right_open=True):
                return self.FingerTable['finger'][i]
        return self.ID  # Moved this by 1 -- pretty sure it was an indent error.
    
    def join(self, n_prime): 
        '''
        Called on join. n_prime is our target for finger table initialization
        '''
        if n_prime is not None:
            # Initialize our finger table from that of n_prime
            self.init_finger_table(n_prime)
            # Signal other nodes
            self.update_others()
        else:
            # Init case
            for i in range(M):
                self.FingerTable['finger'][i] = self.ID
            self.predecessor = self.ID
            
        return
        
    def update_pred(self, ID):
        self.predecessor = ID
    
    def init_finger_table(self, n_prime):      # THIS AS WELL -- SEEMS TO BE A TON OF RPCS IN HERE, AND I THINK IT INTERACTS POORLY WITH THE ABOVE. CONFUSED AS TO WHO'S CALLING WHAT.
        # Resetting successor and predecessor pointers as needed
        # 1. Use n_prime's existing Finger Table to set the successor node for self
        self.FingerTable['finger'][0] = n_succ = self.blocking_rpc(n_prime, "find_succ", (self.FingerTable['start'][0],))
        # 2. Set the node previous to the successor as our current predecessor
        self.predecessor = self.blocking_rpc(n_succ, "var_val", ("predecessor",))
        
        for i in range(M-1):
            if self.check_mod_interval(self.FingerTable['start'][i+1], \
                              self.ID, self.FingerTable['finger'][i], \
                             left_open=False, right_open=True \
                             ):
                self.FingerTable['finger'][i+1] = self.FingerTable['finger'][i]
            else:
                self.FingerTable['finger'][i+1] = self.blocking_rpc(n_prime, "find_succ", (self.FingerTable['start'][i+1],))
                if self.check_mod_interval(self.ID, self.FingerTable['start'][i+1], self.FingerTable['finger'][i+1]):
                    self.FingerTable['finger'][i+1] = self.ID
        return
    
    def update_finger_table(self, z):
        # Need to handle trivial case
        if z == self.ID:
            return
        else:
            changed = False
            for i in range(M):
                if self.check_mod_interval(z, self.FingerTable['start'][i], self.FingerTable['finger'][i],\
                                     left_open=True, right_open=True):
                    changed = True
                    self.FingerTable['finger'][i] = z
            if changed:
                self.remote_update_finger_table(self.predecessor, z)
            return
    
    def remote_update_finger_table(self, target, z):  # A nonblocking way to do an update to another player.
        simgrid.Actor.create(f"uftp_{target}_{z}_{randrange(0, 1<<64)}", simgrid.this_actor.get_host(), self.noreturn_rpc, self.predecessor, "update_finger_table", (z,))
    
    def notify(self, n_prime):  # This seems to be *receiving* notification? I see -- we do this as a RPC (to notify someone, we call their "notify" function).
        if self.predecessor is None or self.check_mod_interval(n_prime, self.predecessor, self.ID, \
                                     left_open=True, right_open=True):
            self.predecessor = n_prime
        return
    
    def stabilize(self):
        succ = self.blocking_rpc(self.successor(), "successor", tuple())
        x = self.blocking_rpc(succ, "var_val", ("predecessor",))
        if self.check_mod_interval(x, self.ID, succ, \
                                     left_open=True, right_open=True):
            self.update_finger_table(x)
        self.blocking_rpc(succ, "notify", (self.ID,))  # This doesn't actually need to be blocking.
        return
    
    def update_others(self):
        self.noreturn_rpc(self.successor(), "update_pred", (self.ID,))
        self.noreturn_rpc(self.predecessor, "update_finger_table", (self.ID,))
        p = None
        for i in range(1, M):
            a,b = self.ID,2**(i-1)
            idx = (a-b)%Q
            p_prime = self.find_pred(idx)
            if p_prime == p:
                continue
            else:
                p = p_prime
                if self.ID != p:
                    self.remote_update_finger_table(p, self.ID)
        return
    
    def fix_fingers(self):   # This is blocking, and will be run as a subprocess
        i = sample(range(M), 1)[0]
        self.blocking_rpc(self.ID, "update_finger_table", (self.blocking_rpc(self.successor(), "find_succ", (self.FingerTable['start'][i],)),))
        return

    def print_fingers(self):
        self.print("Fingers:")
        self.print("Pred: " + str(self.predecessor))
        for i in range(len(self.FingerTable['start'])):
            self.print(str(self.FingerTable['start'][i]) + ' | ' + str(self.FingerTable['interval'][i]) + ' | ' + str(self.FingerTable['finger'][i]))
        return

    def store(self, key, val):
        self.storage[key] = val
    
    def query_key(self, key):
        if key in self.storage.keys():
            return self.storage[key]
        else:
            return None


class Client:
    def __init__(self, ID, target):
        self.ID = "client_" + str(ID)
        self.target = target
        self.target_mailbox = simgrid.Mailbox.by_name(str(target))
        self.num_sent = 0
        self.store_latencies = []
        self.read_latencies = []

    def __call__(self):
        simgrid.this_actor.sleep_for(10000)  # Wait for the network to get set up and stable

        for i in range(1):
            self.store_val(randrange(0, 100), randrange(0, 10000000000))
            self.get_val(randrange(0, 100))
        simgrid.Mailbox.by_name(MONITOR_MAILBOX).put({"store_latencies":tuple(self.store_latencies),
                                                      "read_latencies":tuple(self.read_latencies)}, 0)
        return
    
    def blocking_request(self, target_ID, msg_type, payload):
        mid = self.new_msg_id()
        blocking_mailbox = simgrid.Mailbox.by_name(str(mid))  # A mailbox unique to this message.
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        target_mailbox.put(((mid, mid), msg_type, payload), 0)
        resp = blocking_mailbox.get()
        return resp[2]

    def blocking_rpc(self, target_ID, func, args):
        return self.blocking_request(target_ID, RPC, (func, args))

    def store_val(self, key, val):
        t = simgrid.Engine.clock
        storing_node = self.blocking_rpc(self.target, "find_succ", (self.protocol_hash(key) % Q,))
        self.blocking_rpc(storing_node, "store", (self.protocol_hash(key) % Q, val))
        self.store_latencies.append(simgrid.Engine.clock - t)
    
    def get_val(self, key):
        t = simgrid.Engine.clock
        storing_node = self.blocking_rpc(self.target, "find_succ", (self.protocol_hash(key) % Q,))
        val = self.blocking_rpc(storing_node, "query_key", (self.protocol_hash(key),))
        self.read_latencies.append(simgrid.Engine.clock - t)
        return val

    def print(self, s):
        print(f"[{simgrid.Engine.clock}] {self.ID}: " + s)

    def new_msg_id(self):
        # return randrange(0, 1<<64)
        self.num_sent += 1
        return self.num_sent * Q + self.protocol_hash(self.ID)
    
    def protocol_hash(self, x):
        return hash(x) % Q


MONITOR_MAILBOX = "monitor_mailbox"
class Monitor:
    def __init__(self, num_clients, server_ids):
        self.num_clients_alive = num_clients
        self.server_ids = server_ids
        self.info_dicts = {}
        self.mailbox = simgrid.Mailbox.by_name(MONITOR_MAILBOX)

    def __call__(self):
        for _ in range(10):
            simgrid.this_actor.sleep_for(101)
            for server_id in self.server_ids:
                target_mailbox = simgrid.Mailbox.by_name(str(server_id))
                target_mailbox.put(((-1, -1), PING_NORESP, None), 0)

        while self.num_clients_alive > 0:
            client_return = self.mailbox.get()
            self.num_clients_alive -= 1
            for (k, v) in client_return.items():
                if k not in self.info_dicts.keys():
                    self.info_dicts[k] = []
                self.info_dicts[k].append(v)
        # for server_id in self.server_ids:
        #     target_mailbox = simgrid.Mailbox.by_name(str(server_id))
        #     target_mailbox.put(((-1, -1), FINGER_PRINT, None), 0)

        simgrid.Actor.kill_all()

        return
        
    
    def kill(self, target_ID):
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        target_mailbox.put(((-1, -1), KILL, None), 0)

    def print(self, s):
        print(f"[{simgrid.Engine.clock}] Monitor: " + s)