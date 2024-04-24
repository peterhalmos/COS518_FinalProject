'''
A Vanilla Implementation of Chord.

We extend this code for use on simgrid and realistic emulators.
'''

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
        # Check the modular rotation for each case
        if right_open and not left_open:
            # 1 if in the interval [init, end) and 0 otherwise
            return (x - init)%self.q < (end - init)%self.q
        elif left_open and not right_open:
            # 1 if in the interval (init, end] and 0 otherwise
            return (end - x)%self.q < (end - init)%self.q
        elif not left_open and not right_open:
            # 1 if in the interval [init, end] and 0 otherwise
            return (x - init)%self.q <= (end - init)%self.q
        else:
            # 1 if in the interval (init, end) and 0 otherwise
            b = (x - end)%self.q <= (init - end)%self.q
            return not b
    
    def successor(self):
        # return successor from finger table
        return self.FingerTable['finger'][0]
    
    def find_succ(self, ID):
        # invoke find_predecessor and find its successor pointer
        return self.find_pred(ID).successor()
    
    def find_pred(self, ID):
        n_prime = self
        if ID == n_prime.ID:
            return n_prime.predecessor
        while not self.check_mod_interval( ID, n_prime.ID,\
                                     n_prime.successor().ID, \
                                     left_open=True, right_open=False ):
            # Keep searching
            n_prime = n_prime.closest_preceeding_finger(ID)
        return n_prime
    
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
            self.init_finger_table(n_prime)
            
            # This needs to be implemented for the code to work properly...
            self.update_others()
        else:
            for i in range(self.m):
                self.FingerTable['finger'][i] = self
            self.predecessor = self
            
        return
    
    def init_finger_table(self, n_prime):
        # Resetting successor and predecessor pointers as needed
        print('a')
        print(self.FingerTable['start'][0])
        a = n_prime.find_succ(self.FingerTable['start'][0])
        print('b')
        self.FingerTable['finger'][0] = n_succ = n_prime.find_succ(self.FingerTable['start'][0])
        self.predecessor = n_succ.predecessor
        n_succ.predecessor = n_succ.FingerTable['finger'][0] = self
        
        for i in range(self.m-1):
            
            if check_mod_interval(self.FingerTable['start'][i+1], \
                              self.ID, self.FingerTable['finger'][i].ID, \
                             left_open=False, right_open=True \
                             ):
                self.FingerTable['finger'][i+1] = self.FingerTable['finger'][i]
                
            else:
                self.FingerTable['finger'][i+1] = n_prime.find_succ(self.FingerTable['start'][i+1])
        
        return
    
    def update_finger_table(self, z, i):
        if self.check_mod_interval(z.ID, self.ID, self.FingerTable['finger'][i].ID,\
                                     left_open=True, right_open=False):
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
        for i in range(self.m):
            # will fill this out later
            pass
        return
    
    def fix_fingers(self):
        # ditto, will finish this
        pass
        return