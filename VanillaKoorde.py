
class Koorde_Node():
    def __init__(self, ID, m):
        # bits to represent key-space
        self.m = m
        # size of key-space
        self.q = 2 ** self.m
        # Set the identifier to be a number modulo 2^d (keyspace's modular interval)
        self.ID = ID % self.q
        # As described in Koorde paper, we maintain (1) the successor of our node (ID) and (2) the node that
        # preceeds the node with identifier 2*(ID) (mod 2^d).
        return

    def find_succ(self, val, count_hops=False):
        imaginary_id = self.best_move(val)
        if count_hops:
            n = self.lookup(val, val, imaginary_id, counter=0)
            return n
        else:
            return self.lookup(val, val, imaginary_id)
    
    def lookup(self, key, key_shift, i, counter=None):
        # * key is the node identifier that we're looking for, key_shift is the shifted version of key based
        # on the lookup path so far.
        # * i is the imaginary de Bruijn node, which does not necessarily exist in the graph.
        if counter is not None:
            # performing a count_hops trace
            counter = counter+1
        if self.check_interval(self.ID, key, self.successor.ID):
            if counter is not None:
                return counter
            else:
                return self.successor
        elif self.check_interval(self.ID, i, self.successor.ID):
            return self.next.lookup(key, (key_shift << 1) % self.q, (i << 1) % self.q + self.top_bit(key_shift), \
                                    counter=counter)
        else:
            return self.successor.lookup(key, key_shift, i, counter=counter)
        
    def join(self, n_prime):
        '''
        Adding a new Node to the DHT by reference to n_prime
        '''
        if n_prime != self:
            self.stabilize(n_prime)
        else: # if new node is the same as self, initialize everything to self.
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

    def best_move(self, key):
        current_id = self.ID
        # path will contain the search path in de Bruijn graph
        path = []
        for i in range(self.m-1):
            next_id = (current_id << 1) % self.q + self.top_bit(key)
            path.append(next_id)
            key = (key << 1) % self.q
            current_id = next_id
        path.reverse()
        j = 0
        while not self.check_interval(self.ID, path[j], self.successor.ID) and j > 0:
          j = j+1
        return path[j]

    def check_interval(self, begin, middle, end):
      if middle == end:
        return True
      else:
        if begin == end:
          return True
        elif begin > end:
          diff = self.q - begin
          middle = (middle + diff)%self.q
          begin = 0
          end = (end + diff)%self.q

        return (begin < middle < end)

    def notify(self, n_prime):  # adjust for koorde + add simgrid
        pID = self.predecessor.ID
        if self.predecessor is None or self.check_mod_interval(n_prime.ID, pID, self.ID, \
                                                               left_open=True, right_open=True):
            self.predecessor = n_prime
        return

    def stabilize(self, n_prime):
        # set successor and predecessir for newly-entered node.
        old_successor = n_prime.find_succ(self.ID)
        self.successor = old_successor
        self.predecessor = old_successor.predecessor
        # update pointers!
        old_predecessor = old_successor.predecessor
        old_predecessor.successor = self
        old_successor.predecessor = self
        # set de Bruijn graph pointer "next" and update other node
        self.update_next(old_successor)
        self.update_others() # update the "next" pointer for the node before
        return

    def update_next(self, old_succ):
        self.next = old_succ.find_succ((self.ID * 2) % self.q)
        return

    def update_others(self):
        '''
        # Another way to do update_others():
        
        key = ((self.ID + 1) >> 1) % self.q
        #if (key << 1)% self.q == self.predecessor.ID + 1:
        other_node = self.find_succ(key)
        other_node.next = self
        '''
        temp = self.successor
        while temp != self:
          if (temp.ID << 1)% self.q > self.ID and self.compare_mod(self.ID, temp.next.ID):
            temp.next = self
          temp = temp.successor
        return

    def compare_mod(self,x,y):
      if x == y:
        return True
      else:
        diff = self.q - x
        x = x + diff
        y = y + diff
        return x > y

def print_info(node):
    print(f'Finger table of {node.ID}:')

    print("Predecessor: " + str(node.predecessor.ID))
    print("Successor: " + str(node.successor.ID))
    print("Next (predecessor of 2*ID): " + str(node.next.ID))

    return
