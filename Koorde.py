'''
An Implementation of Koorde: note that this implementation is for degree-2 de Bruijn graphs (as a start).

We extend this code for use on simgrid and realistic emulators.
'''


class Koorde_Node():

    def __init__(self, ID, m):
        # Add SHA-hashing specific functions later;
        # for now assume that the ID is given

        # bits to represent key-space
        self.m = m
        # size of key-space
        self.q = 2 ** self.m
        # Set the identifier to be a number modulo 2^d (keyspace's modular interval)
        self.ID = ID % self.q
        # base used for bit-representation of ID
        # self.k = k

        # As described in Koorde paper, we maintain (1) the successor of our node (ID) and (2) the node that
        # preceeds the node with identifier 2*(ID) (mod 2^d).
        self.successor = None
        self.predecessor = None
        self.next = None

        return

    def find_succ(self, key):
        i = self.best_move(self, key)
        return self.lookup(key, key, i)


    def join(self, n_prime):
        '''
        Adding a new Node to the DHT by reference to n_prime
        '''
        if n_prime != self:
            # set successor for newly-entered node.
            old_succ = n_prime.find_succ(self.ID)
            self.successor = old_succ
            # set predecessor for newly-entered node + update pointers.
            old_pred = old_succ.predecessor
            self.predecessor = old_succ.predecessor
            old_pred.successor = self
            # set de Bruijn graph pointer "next"
            self.next = old_succ.find_succ((self.ID * 2) % self.q)
            self.update_others()
        else:
            self.ID = self
            self.successor = self
            self.predecessor = self
            self.next = self
        return

    def top_bit(self, val):
        p = 2 ** (self.m - 1)
        if val / p >= 1:
            return 1
        else:
            return 0

    def lookup(self, key, key_shift, i):
        # * key is the node identifier that we're looking for, key_shift is the shifted version of key based
        # on the lookup path so far.
        # * i is the imaginary de Bruijn node, which does not necessarily exist in the graph.
        if self.ID < key <= self.successor:
            return self.successor
        elif self.ID < i <= self.successor:
            return self.next.lookup(key, (key_shift << 1) % self.q, (i << 1) % self.q + self.top_bit(key_shift))
        else:
            return self.successor.lookup(key, key_shift, i)

    def best_move(self, key):
        # fill in: returns best move in the complete de Bruijn graph (check how paper selects this i)
        return

    # adjust the following functions (stabilization/joining should be the same as chord)
    def update_finger_table(self, z, i):
        if self.check_mod_interval(z.ID, self.ID, self.FingerTable['finger'][i].ID, \
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

    # need to implement for join().
    def update_others(self):
        for i in range(self.m):
            # will fill this out later
            pass
        return

    def fix_fingers(self):
        # ditto, will finish this
        pass
        return
