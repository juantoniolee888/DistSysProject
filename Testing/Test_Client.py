#!/usr/bin/env python3

import sys
import time

sys.path.append("..")
from HashTableClient import *

def main():
    if len(sys.argv) != 2:
        print('Wrong number of command line arguments')
        return
    nameserver = sys.argv[1]
    
    # create client object
    htc = HashTableClient(nameserver)

    print('INSERT:')
    insert_success = True
    for i in range(600):
        # insert values into the table
        val = str(i)
        val2 = {'name':'fred'}
        r = htc.insert(val, val2)
        if r != "success" and insert_success:
            print('insert failed', r)
            insert_success = False
    if insert_success:
        print('insert succeeded')
    
    print("LOOKUP:")
    lookup_success = True
    for i in range(600):
        val = str(i)
        lookup = htc.lookup(val)
        if lookup == 'key not in table' and lookup_success:
            print('lookup failed')
            lookup_success = False

    if lookup_success:
        print('lookup succeeded')

    print("QUERY:")
    query_success = True
    for i in range(600):
        val = str(i)
        val2 = {'name':'fred'}
        query = htc.query('name', 'fred')
        if query == 'no matches found' and query_success:
            print('query failed')
            query_success = False
    if query_success:
        print('query succeeded')

    print("REMOVE:")
    remove_success = True
    for i in range(600):
        val = str(i)
        remove = htc.remove(val)
        if remove != "success" and remove_success:
            print('remove failed')
            remove_success = False

    if remove_success:
        print('remove succeeded')

# call main execution
if __name__ == '__main__':
    main()
