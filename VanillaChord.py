'''
A Vanilla Implementation of Chord.

We extend this code for use on simgrid and realistic emulators.
'''
from random import sample

class Chord_Node(): # will super() this once Koorde is done..
    
    def __init__(self, ID, m):
        
        # Add SHA-hashing specific functions later;
        # for now assume that the ID is given
        
        # bits to represent key-space
        self.m = m 
        # size of key-space
        self.q = 2**self.m 
        # Set the identifer to be a number modulo 2^m (keyspace's modular interval)
        self.ID = ID % 2**self.m
        
        # Initialize Finger Table
        start, end = self.ID+1,self.ID+2
        # Leave 'finger' entry unfilled until init_finger_table call
        self.FingerTable = {'start':[start], 'interval':[(start,end)], 'finger':[None]*m}
        # Initialize predecessor to NIL
        self.predecessor = None
        for i in range(1, self.m, 1):
            # To be consistent with the paper,
            # we keep both start and interval: (start,end) entries
            start = self.FingerTable['interval'][i-1][-1]
            self.FingerTable['start'].append(start)
            end = (start + 2**i)%self.q
            self.FingerTable['interval'].append((start,end))
        return
    
    def check_mod_interval(self, x, init, end, left_open=True, right_open=True):
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
            return (x - init)%self.q < (end - init)%self.q
        elif left_open and not right_open:
            return (end - x)%self.q < (end - init)%self.q
        elif not left_open and not right_open:
            return (x - init)%self.q <= (end - init)%self.q
        else:
            b = (x - end)%self.q <= (init - end)%self.q
            return not b
    
    def successor(self):
        # return successor from finger table
        return self.FingerTable['finger'][0]
    
    def find_succ(self, ID):
        if ID == self.ID:
            # Return self's successor if ID equal
            return self.successor()
        elif self.check_mod_interval(ID, self.predecessor.ID,\
                                     self.ID, \
                                     left_open=True, right_open=False):
            # Return self if in interval (self.pred.ID, self.ID]
            return self
        else:
            # Otherwise, invoke find_predecessor to recursively search for ID's pred and find its successor pointer
            return self.find_pred(ID).successor()
    
    def find_pred(self, ID, count_hops=False):
        num_hops = 0
        n_prime = self
        if ID == n_prime.ID:
            ret_node = n_prime.predecessor
        else:
            while not self.check_mod_interval( ID, n_prime.ID,\
                                         n_prime.successor().ID, \
                                         left_open=True, right_open=False ):
                # Keep searching
                n_prime = n_prime.closest_preceeding_finger(ID)
                num_hops += 1
            ret_node = n_prime
            
        if count_hops is False:
            return n_prime
        else:
            return num_hops+1
    
    def closest_preceeding_finger(self, ID):
        '''
        Find closest finger with identifier preceding ID
        '''
        for i in range(self.m-1, -1, -1):
            if self.check_mod_interval(self.FingerTable['finger'][i].ID,\
                                  self.ID, ID, \
                                  left_open=True, right_open=True):
                return self.FingerTable['finger'][i]
        return self
    
    def join(self, n_prime):
        '''
        Adding a new Node to the DHT by reference to n_prime
        '''
        if n_prime != self:
            # Initialize self's finger table to reflect n_prime's
            self.init_finger_table(n_prime)
            # Update the finger tables of the other nodes as self has joined
            self.update_others()
        else:
            # Initialization case
            # self == n_prime is only node in the network
            for i in range(self.m):
                self.FingerTable['finger'][i] = self
            self.predecessor = self
        return
    
    def init_finger_table(self, n_prime):
        # Resetting successor and predecessor pointers as needed
        # 1. Use n_prime's existing Finger Table to set the successor node for self
        self.FingerTable['finger'][0] = n_succ = n_prime.find_succ(self.FingerTable['start'][0])
        
        # 2. Set the node previous to the successor as our current predecessor
        self.predecessor = n_succ.predecessor
        
        # 3. Reset the successor's predecessor to self (the node joining the network)
        n_succ.predecessor.FingerTable['finger'][0] = n_succ.predecessor = self
        
        for i in range(self.m-1):
            if self.check_mod_interval(self.FingerTable['start'][i+1], \
                              self.ID, self.FingerTable['finger'][i].ID, \
                             left_open=False, right_open=True \
                             ):
                self.FingerTable['finger'][i+1] = self.FingerTable['finger'][i]
            else:
                self.FingerTable['finger'][i+1] = n_prime.find_succ(self.FingerTable['start'][i+1])
        return
    
    def update_finger_table(self, z, i):  # Appears unused?
        #print(f'{z.ID} in [{self.ID},{self.FingerTable['finger'][i].ID})?')
        # Need to handle trivial case
        if z.ID == self.ID:
            return
        elif self.check_mod_interval(z.ID, self.ID, self.FingerTable['finger'][i].ID,\
                                     left_open=False, right_open=True):
            self.FingerTable['finger'][i] = z
            p = self.predecessor
            p.update_finger_table(z, i)
        
        return
    
    def notify(self, n_prime):
        pID = self.predecessor.ID
        if self.predecessor is None or self.check_mod_interval(n_prime.ID, pID, self.ID, \
                                     left_open=True, right_open=True):
            self.predecessor = n_prime
        return
    
    def stabilize(self):
        x = self.successor().predecessor
        if self.check_mod_interval(x.ID, self.ID, self.successor().ID, \
                                     left_open=True, right_open=True):
            self.FingerTable['finger'][0] = x
        self.successor().notify(self)
        return
        
    def update_others(self):
        # Update the other nodes in the finger table to reflect all updates
        for i in range(1, self.m+1, 1):
            a,b = self.ID,2**(i-1)
            idx = (a-b)%self.q
            p = self.find_pred(idx)
            p.update_finger_table(self, i-1)
        return
    
    def fix_fingers(self):
        # Randomly sample Finger Table entries and update to correct successor
        i = sample(range(self.m), 1)[0]
        self.FingerTable['finger'][i] = self.find_succ(self.FingerTable['start'][i])
        return

def print_fingers(node, m=8):
    # For printing the full finger table data for a given node
    print(f'Finger table of {node.ID}')
    print('start | interval | finger')
    for i in range(m):
        print(str(node.FingerTable['start'][i]) + ' | ' + str(node.FingerTable['interval'][i]) + ' | ' + str(node.FingerTable['finger'][i].ID))
    return
