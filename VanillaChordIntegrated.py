'''
A Vanilla Implementation of Chord.

We extend this code for use on simgrid and realistic emulators.
'''
import simgrid



M = 50
Q = 2 ** M


# Idea: when parent needs to do something blocking, it spawns a child, does what it needs to do, passes any updates to the child, and dies.
# Messages are structured as ((msg_id, return_mailbox_id), msg_type, payload). Typically, resps (which are to blocking queries) will want to go to a separate inbox
# Types:
KILL = 0
RESP = 1  # These share the msg_id of the message that prompted them, and have no return_mailbox_id
RPC = 2
PING = 3








class Chord_Node(): # will super() this once Koorde is done..
    
    def __init__(self, ID, start=None, end=None, FingerTable=None, predecessor=None, num_sent=None, storage={}, join_target=None): 
        # Add SHA-hashing specific functions later;
        # for now assume that the ID is given

        # Set the identifer to be a number modulo 2^m (keyspace's modular interval)
        self.ID = ID % Q 
        self.mailbox = simgrid.mailbox.by_name(str(self.ID))

        if start is None:  # If we'
            # Initialize Finger Table
            start, end = self.ID+1,self.ID+2
            # Leave 'finger' entry unfilled until init_finger_table call
            self.FingerTable = {'start':[start], 'interval':[(start,end)], 'finger':[None]*m}
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

            self.join(join_target)


        else:
            self.start = start
            self.end = end
            self.FingerTable = FingerTable
            self.predecessor = predecessor
            self.num_sent = num_sent + 100 * M  # Leave some room for the parent to send messages without treading on our ID.
            self.storage = storage


        while 1:
            msg = self.mailbox.get()
            msg_id, sender_id = msg[0]
            msg_type = msg[1]
            payload = msg[2]
            match msg_type:
                case KILL:
                    return
                case RESP:
                    print("Received response in processing loop! Exiting.")
                    return
                case PING:
                    send_resp(msg_id, sender_id, None)
                case RPC:
                    """
                    We'll need to fork and die if this will require us to make a blocking call (i.e. a RPC to another node)
                    """
                    blocking = payload[0] in ["blocking_request", "blocking_rpc", "find_succ", "find_pred", "stabilize"]
                    if blocking:  # If blocking, fork a child to replace us...
                        self.spawn_child()

                    send_resp(msg_id, sender_id, self.call(payload[0], payload[1]))

                    if blocking:  # ...and quit
                        return

        
            
    

    def call(self, fname, fargs):
        return getattr(self, fname)(*fargs)

    def var_val(self, var_name):
        return getattr(self, var_name)
    
    def set_var(self, var_name, val):
        return setattr(self, var_name, val)

    def new_msg_id(self):
        self.num_sent += 1
        return self.num_sent * Q + self.ID

    def blocking_request(self, target_ID, msg_type, payload):
        mid = self.new_msg_id()
        blocking_mailbox = simgrid.mailbox.by_name(str(mid))  # A mailbox unique to this message. Inefficient (dep. on impl. on simgrid side) but makes things easy.
        target_mailbox = simgrid.mailbox.by_name(str(target_ID))
        target_mailbox.put((mid, mid), msg_type, payload)
        resp = blocking_mailbox.get()
        return resp[2]

    def blocking_rpc(self, target_ID, func, args):
        return blocking_request(target_ID, RPC, (func, args))

    def send_resp(self, msg_id, target, payload):
        target_mailbox = simgrid.mailbox.by_name(str(target))
        target_mailbox.put_async((msg_id, None), RESP, payload)
        return

    def spawn_child(self)
        """
        Only use if we're about to finish up and die -- we can't process any more messages on the main mailbox after this point. 
        We determine this at the top level, in our control loop.
        """
        simgrid.Actor.create(host=simgrid.this_actor.get_host(), self.__class__, self.start, self.end, self.FingerTable, self.predecessor, self.num_sent, self.storage)




    def check_mod_interval(self, x, init, end, left_open=True, right_open=True):
        # Check the modular rotation for each case
        if right_open and not left_open:
            # 1 if in the interval [init, end) and 0 otherwise
            return (x - init)%Q < (end - init)%Q
        elif left_open and not right_open:
            # 1 if in the interval (init, end] and 0 otherwise
            return (end - x)%Q < (end - init)%Q
        elif not left_open and not right_open:
            # 1 if in the interval [init, end] and 0 otherwise
            return (x - init)%Q <= (end - init)%Q
        else:
            # 1 if in the interval (init, end) and 0 otherwise
            b = (x - end)%Q <= (init - end)%Q
            return not b
    
    def successor(self):
        # return successor from finger table
        return self.FingerTable['finger'][0]
    
    def find_succ(self, ID):
        # invoke find_predecessor and find its successor pointer
        return self.find_pred(ID).successor()
    
    def find_pred(self, ID):       # ISSUE FUNCTION
        n_prime = self.ID
        while not self.check_mod_interval( ID, n_prime,\
                                     self.blocking_rpc(n_prime, "successor", tuple()), \
                                     left_open=True, right_open=False ):
            # Keep searching
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
        if n_prime != self:
            self.init_finger_table(n_prime)
            
            # This needs to be implemented for the code to work properly...
            self.update_others()
        else:
            for i in range(M):
                self.FingerTable['finger'][i] = self
            self.predecessor = self
            
        return


    def update_pred(self, ID):
        n_succ.predecessor = n_succ.FingerTable['finger'][0] = ID
    
    def init_finger_table(self, n_prime):      # THIS AS WELL -- SEEMS TO BE A TON OF RPCS IN HERE, AND I THINK IT INTERACTS POORLY WITH THE ABOVE. CONFUSED AS TO WHO'S CALLING WHAT.
        # Resetting successor and predecessor pointers as needed
        self.FingerTable['finger'][0] = n_succ = self.blocking_rpc(n_prime, "find_succ", (self.FingerTable['start'][0],))
        self.predecessor = self.blocking_rpc(n_succ, "var_val", (predecessor,))
        self.blocking_rpc(n_succ, "update_pred", (self.ID,))
        
        for i in range(M-1):
            
            if check_interval(self.FingerTable['start'][i+1], \
                              self.ID, self.FingerTable['finger'][i], \
                             left_open=False, right_open=True \
                             ):
                self.FingerTable['finger'][i+1] = self.FingerTable['finger'][i]
                
            else:
                self.FingerTable['finger'][i+1] = n_prime.find_succ(self.FingerTable['start'][i+1])
        return
    
    def update_finger_table(self, z, i):
        if self.check_mod_interval(z, self.ID, self.FingerTable['finger'][i],\
                                     left_open=True, right_open=False):
            self.FingerTable['finger'][i] = z
            p = self.predecessor
            p.update_finger_table(z, i)
        return
    
    def notify(self, n_prime):  # This seems to be *receiving* notification? I see -- we do this as a RPC (to notify someone, we call their "notify" function).
        pID = self.predecessor
        if self.predecessor is None or self.check_mod_interval(n_prime, self.predecessor, self.ID, \
                                     left_open=True, right_open=True):
            self.predecessor = n_prime
        return
    
    def stabilize(self):
        succ = self.blocking_rpc(self.successor, "successor", tuple())
        x = self.blocking_rpc(succ, "var_val", ("predecessor",))
        if self.check_mod_interval(x, self.ID, succ, \
                                     left_open=True, right_open=True):
            self.FingerTable['finger'][0] = x
        self.blocking_rpc(succ, "notify", (self.id,))  # This doesn't actually need to be blocking.
        return
    
    def update_others(self):
        for i in range(M):
            # will fill this out later
            pass
        return
    
    def fix_fingers(self):
        # ditto, will finish this
        pass
        return