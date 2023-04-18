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

    throughput_values = []
    latency_values = []

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
    throughput_values.append(time_spent/10)
    latency_values.append(10/time_spent)
    

    print("LOOKUP:")
    start_time = time.time_ns()
    lookup_success = True
    for i in range(10):
        val = str(keys[i])
        lookup = htc.lookup(val)
        print(lookup)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    throughput_values.append(time_spent/10)
    latency_values.append(10/time_spent)


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
    throughput_values.append(time_spent/10)
    latency_values.append(10/time_spent)

    print("REMOVE:")
    start_time = time.time_ns()
    remove_success = True
    for i in range(10):
        val = str(keys[i])
        remove = htc.remove(val)
        print(remove)
    end_time = time.time_ns()
    time_spent = end_time-start_time
    throughput_values.append(time_spent/10)
    latency_values.append(10/time_spent)

    return throughput_values, latency_values

    #for i in range(10):
        # insert values into the table
        #val = str(keys[i])
        #val2 = {'name':'fred'}
        #r = htc.insert(val, val2)
        #print(r)



# call main execution
if __name__ == '__main__':
    throughput = [0, 0, 0, 0]
    latency = [0, 0, 0, 0]
    for i in range(0,10):
        xput, lat = main()
        for i in range(0,4):
            throughput[i] += xput[i]
            latency[i] += lat[i]
    print('\t'.join([str(x/10) for x in throughput]))
    print('\t'.join([str(x/10) for x in latency]))
