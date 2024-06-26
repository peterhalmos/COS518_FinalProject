'''
Koorde implementation in the Simgrid emulator
'''
import simgrid
from random import sample, randrange

global M, Q
global KILL, RESP, RPC, RPC_NORESP, PING, PING_NORESP, FINGER_PRINT

M = 8
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

class Koorde_Node(): # will super() this once Koorde is done..
    def __init__(self, ID, predecessor=None, successor=None, next=None, num_sent=-1, storage={}, join_target=None): 
        # Add SHA-hashing specific functions later;
        # for now assume that the ID is given
        
        # Set the identifer to be a number modulo 2^m (keyspace's modular interval)
        self.ID = ID % Q
        self.successor = successor
        self.predecessor = predecessor
        self.next = next
        
        self.mailbox = simgrid.Mailbox.by_name(str(self.ID))
        self.mailbox.set_receiver(simgrid.this_actor.get_host())
        self.num_sent = num_sent + 100 * M  # Leave some room for the parent to send messages without treading on our ID.
        self.storage = storage
        self.join_target = join_target

        
        
    def __call__(self):

        self.join(self.join_target)
        
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
                self.print_info()
            elif msg_type == RPC or msg_type == RPC_NORESP:
                """
                We have two types of procedures -- those that modify internal state, 
                and those that call externally. Except for stabilizing (which we'll treat separately), none do both. 
                """
                #self.print("Got RPC: " + str(msg))
                if payload[0] in ["blocking_request", "blocking_rpc", "find_succ", "find_pred", "lookup"]:
                    simgrid.Actor.create(f"responder_{randrange(0, 1<<64)}", simgrid.this_actor.get_host(), self.call_and_resp, msg_id, sender_id, payload)
                else:  # Otherwise, we might modify state (and we won't block), so we do it in thread.
                    if msg_type == RPC:
                        self.call_and_resp(msg_id, sender_id, payload)
                    else:
                        self.call(payload[0], payload[1])
                #self.print("Finished rpc")
            else:
                #self.print("Unknown message type! Exiting.")
                print(msg)
                return
            if simgrid.Engine.clock > last_stab_time + 100:
                #old_network_fixer_actors.append(simgrid.Actor.create(f"stabilizer_{self.ID}", simgrid.this_actor.get_host(), self.stabilize, self.join_target))
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
        #self.print("Start request")
        #self.print(str(msg_type) + " " + str(payload))
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        #self.print("Target mailbox:" + str(target_mailbox))
        if target_ID is None:
            raise ArithmeticError
        target_mailbox.put(((mid, mid), msg_type, payload), 0)
        #self.print("Await resp")
        if msg_type in [RPC, PING]:  # Message types that obtain a response:
            blocking_mailbox = simgrid.Mailbox.by_name(str(mid))  # A mailbox unique to this message.
            resp = blocking_mailbox.get()
            return resp[2]

    def blocking_rpc(self, target_ID, func, args):
        if self.ID == target_ID:  # So that we don't block on RPCs to self.
            self.print("Rec")
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
        #self.print("Send_resp target: " + str(target))
        target_mailbox = simgrid.Mailbox.by_name(str(target))
        target_mailbox.put(((msg_id, None), RESP, payload), 0)
        #self.print("Finished send_resp to " + str(target))
        return
    
    def call_and_resp(self, msg_id, target, payload):  # Async wrapper
        #self.print("call_and_resp_target:" + str(target))
        r = self.send_resp(msg_id, target, self.call(payload[0], payload[1]))
        #self.print("Finished c&r at " + str(target))
        return 

    def spawn_child(self):
        """
        Only use if we're about to finish up and die -- we can't process any more messages on the main mailbox after this point. 
        We determine this at the top level, in our control loop.
        """
        simgrid.Actor.create(simgrid.this_actor.get_host(),\
                             self.__class__(ID=self.ID, FingerTable=self.FingerTable, \
                                            predecessor=self.predecessor, num_sent=self.num_sent, \
                                            storage=self.storage))
        
    def find_succ(self, val):
        imaginary_id = self.best_move(val)
        z = self.lookup(val, val, imaginary_id)
        # if z == self.predecessor:
        #     return self.ID
        # else:
        #     
        return self.blocking_rpc(z, "var_val", ("successor",))

    def join(self, n_prime):
        '''
        Adding a new Node to the DHT by reference to n_prime
        '''
        if n_prime is not None:
            self.stabilize(n_prime)
        else: # if new node is the same as self, initialize everything to self.
            self.successor = self.ID
            self.predecessor = self.ID
            self.next = self.ID
        return

    def top_bit(self, val):
      p = 2 ** (M - 1)
      if val / p >= 1:
        return 1
      else:
        return 0

    def lookup(self, key, key_shift, i):
        #self.print("Looking up...")
        # * key is the node identifier that we're looking for, key_shift is the shifted version of key based
        # on the lookup path so far.
        # * i is the imaginary de Bruijn node, which does not necessarily exist in the graph.
        if self.check_interval(self.ID, key, self.successor):
            return self.successor
        elif self.check_interval(self.ID, i, self.successor):
            #self.print("Getting next")
            return self.blocking_rpc(self.next, "lookup", (key, (key_shift << 1) % Q, (i << 1) % Q + self.top_bit(key_shift)) )
        else:
            #self.print("Getting successor")
            return self.blocking_rpc(self.successor, "lookup", (key, key_shift, i) )

    def best_move(self, key):
        current_id = self.ID
        # path will contain the search path in de Bruijn graph
        path = []
        for i in range(M-1):
            next_id = (current_id << 1) % Q + self.top_bit(key)
            path.append(next_id)
            key = (key << 1) % Q
            current_id = next_id
        path.reverse()
        j = 0
        while not self.check_interval(self.ID, path[j], self.successor) and j > 0:
          j = j+1
        return path[j]

    def check_interval(self, begin, middle, end):
      if middle == end:
        return True
      else:
        if begin == end:
          return True
        elif begin > end:
          diff = Q - begin
          middle = (middle + diff)%Q
          begin = 0
          end = (end + diff)%Q
        return (begin < middle < end)

    def update_pred(self, n):
        self.predecessor = n

    def update_succ(self, n):
        self.successor = n
    
    def stabilize(self, n_prime):
        if n_prime is None:  # Case for first node.
            return
        # set successor and predecessor for newly-entered node.
        self.successor = self.blocking_rpc(n_prime, "find_succ", (self.ID,))
        self.print("Found successor: " + str(self.successor))
        self.predecessor = self.blocking_rpc(self.successor, "var_val", ("predecessor",))

        #self.try_update_next(self.ID)

        self.next = self.blocking_rpc(self.successor, "find_succ", ((self.ID * 2) % Q, ))

        self.update_others()
        self.blocking_rpc(self.predecessor, "update_succ", (self.ID,))
        self.blocking_rpc(self.successor, "update_pred", (self.ID,))

        self.print("Begin deadlock")

        
        self.try_update_next(self.ID)

        self.print("Intermediate deadlock")
        
        #self.next = self.blocking_rpc(self.successor, "find_succ", ((self.ID * 2) % Q , ))

        self.print("End deadlock")

        
        # set de Bruijn graph pointer "next" and update other node
        #self.update_others() # update the "next" pointer for the node before'''

        return

    
    
        
    def update_others(self):
        # potential_successor_targets = [(self.ID >> 1)-1, (self.ID >> 1)-1 + Q//2, self.ID-1]
        # potential_nodes = set()
        # for ID in potential_successor_targets:
        #     self.print("Start update...")
        #     potential_nodes.add(self.blocking_rpc(self.blocking_rpc(self.successor, "find_succ", (ID,)), "var_val", ("predecessor",)))
        # for n in potential_nodes:
        #     if n != self.ID:
        #         self.blocking_rpc(n, "try_update_next", (self.ID,))
        #     self.print("Finish update")
        key = ((self.predecessor + 1) >> 1) % Q
        self.print("Key: " + str(key))
        if (key << 1)% Q == self.predecessor + 1:
            self.print("Active!")
            other_node = self.find_succ(key)
            self.print("Other node: " + str(other_node))
            self.blocking_rpc(other_node, "set_next", (self.ID,))
            self.print("To confirm: " + str(self.blocking_rpc(other_node, "var_val", ("next",))))
        return
    
    def set_next(self, val):
        self.next = val

    def try_update_next(self, ID):
        self.print("Trying to update next. Current: " + str(self.ID) + " and new: " + str(ID))
        if self.check_interval((2 * self.ID) % Q, ID, self.next):
            self.print("Succeeded!")
            self.next = ID


    
    def print_info(node):
        print(f'Finger table of {node.ID}:')
        print("Predecessor: " + str(node.predecessor))
        print("Successor: " + str(node.successor))
        print("Next (predecessor of 2*ID): " + str(node.next))
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

        for i in range(10):
            self.store_val(str(randrange(0, 100)), randrange(0, 10000000000))
            self.get_val(randrange(0, 100))
        simgrid.Mailbox.by_name(MONITOR_MAILBOX).put({"store_latencies":tuple(self.store_latencies),
                                                      "read_latencies":tuple(self.read_latencies)}, 0)
        return
    
    def blocking_request(self, target_ID, msg_type, payload):
        mid = self.new_msg_id()
        blocking_mailbox = simgrid.Mailbox.by_name(str(mid))  # A mailbox unique to this message.
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        #self.print("Blocking mailbox: " + str(blocking_mailbox))
        target_mailbox.put(((mid, mid), msg_type, payload), 0)
        resp = blocking_mailbox.get()
        return resp[2]

    def blocking_rpc(self, target_ID, func, args):
        return self.blocking_request(target_ID, RPC, (func, args))

    def store_val(self, key, val):
        #self.print("Storing val!")
        t = simgrid.Engine.clock
        storing_node = self.blocking_rpc(self.target, "find_succ", (self.protocol_hash(key),))
        self.print("Found storing node for " + str(self.protocol_hash(key)) + ": " + str(storing_node))
        self.blocking_rpc(storing_node, "store", (self.protocol_hash(key), val))
        self.store_latencies.append(simgrid.Engine.clock - t)
        #self.print("Finished store!")
    
    def get_val(self, key):
        #self.print("Getting val")
        t = simgrid.Engine.clock
        storing_node = self.blocking_rpc(self.target, "find_succ", (self.protocol_hash(key),))
        #self.print("Found storing node")
        val = self.blocking_rpc(storing_node, "query_key", (self.protocol_hash(key),))
        self.read_latencies.append(simgrid.Engine.clock - t)
        #self.print("Got val")
        return val

    def print(self, s):
        print(f"[{simgrid.Engine.clock}] {self.ID}: " + s)

    def new_msg_id(self):
        return randrange(0, 1<<64)
        #self.num_sent += 1
        #return self.num_sent * Q + self.protocol_hash(self.ID)
    
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
        #self.print("Server IDs: " + str(self.server_ids))

        # for _ in range(10):
        #     simgrid.this_actor.sleep_for(101)
        #     for server_id in self.server_ids:
        #         target_mailbox = simgrid.Mailbox.by_name(str(server_id))
        #         target_mailbox.put(((-1, -1), PING_NORESP, None), 0)
        simgrid.this_actor.sleep_for(1000)
        for server_id in self.server_ids:
            target_mailbox = simgrid.Mailbox.by_name(str(server_id))
            target_mailbox.put(((-1, -1), FINGER_PRINT, None), 0)

        while self.num_clients_alive > 0:
            client_return = self.mailbox.get()
            self.num_clients_alive -= 1
            for (k, v) in client_return.items():
                if k not in self.info_dicts.keys():
                    self.info_dicts[k] = []
                self.info_dicts[k].append(v)
        simgrid.Actor.kill_all()
        return
        
    def kill(self, target_ID):
        target_mailbox = simgrid.Mailbox.by_name(str(target_ID))
        target_mailbox.put(((-1, -1), KILL, None), 0)

    def print(self, s):
        print(f"[{simgrid.Engine.clock}] Monitor: " + s)