#!/usr/bin/env python3

import sys
import time
import random

sys.path.append("..")
from HashTableClient import *

def main():
    if len(sys.argv) != 2:
        print('Wrong number of command line arguments')
        return
    nameserver = sys.argv[1]
    
    # create client object
    htc = HashTableClient(nameserver)

    keys = [1000000,'keyagain',700,'anotherkey',345,789,1,999,256,600]

    print('INSERT:')
    insert_success = True
    for i in range(10):
        # insert values into the table
        val = str(keys[i])
        val2 = {'name':'fred'}
        r = htc.insert(val, val2)
        print(r)
    
    print("LOOKUP:")
    lookup_success = True
    for i in range(10):
        val = str(keys[i])
        lookup = htc.lookup(val)
        print(lookup)

    print("QUERY:")
    query_success = True
    for i in range(10):
        val = str(keys[i])
        val2 = {'name':'fred'}
        query = htc.query('name', 'fred')
        print(query)

    print("REMOVE:")
    remove_success = True
    for i in range(10):
        val = str(keys[i])
        remove = htc.remove(val)
        print(remove)

# call main execution
if __name__ == '__main__':
    main()
