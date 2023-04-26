#!/usr/bin/env python3

import sys
import time
import random

sys.path.append("..")
from Admin import *

def main():
    if len(sys.argv) != 2:
        print('Wrong number of command line arguments')
        return
    nameserver = sys.argv[1]
    
    # create client object
    htc = HashTableClient(nameserver)

    keys = [1000000,'keyagain',700,'anotherkey',345,789,1,999,256,600]

    print('INSERT:')
    start_time = time.time_ns()
    insert_success = True
    for i in range(10):
        # insert values into the table
        val = str(keys[i])
        val2 = {'name':'fred'}
        r = htc.insert(val, val2)
        print(r)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    print("THROUGHPUT:", time_spent/10)
    print("LATENCY:", 10/time_spent)
    

    print("LOOKUP:")
    start_time = time.time_ns()
    lookup_success = True
    for i in range(10):
        val = str(keys[i])
        lookup = htc.lookup(val)
        print(lookup)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    print("THROUGHPUT:", time_spent/10)
    print("LATENCY:", 10/time_spent)


    print("QUERY:")
    start_time = time.time_ns()
    query_success = True
    for i in range(10):
        val = str(keys[i])
        val2 = {'name':'fred'}
        query = htc.query('name', 'fred')
        print(query)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    print("THROUGHPUT:", time_spent/10)
    print("LATENCY:", 10/time_spent)


    print("REMOVE:")
    start_time = time.time_ns()
    remove_success = True
    for i in range(10):
        val = str(keys[i])
        remove = htc.remove(val)
        print(remove)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    print("THROUGHPUT:", time_spent/10)
    print("LATENCY:", 10/time_spent)


    #for i in range(10):
        # insert values into the table
        #val = str(keys[i])
        #val2 = {'name':'fred'}
        #r = htc.insert(val, val2)
        #print(r)



# call main execution
if __name__ == '__main__':
    main()
