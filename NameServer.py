#!/usr/bin/env python3


import sys
import socket
import select
import json
from json.decoder import JSONDecodeError
import os
import shutil
import time
import random


#resend informtation to catalog.cse.nd.edu
def update_info(project_name, port_number):
    project_name = project_name.strip() + "-NS"
    dic = {
        "type" : "nameserver",
        "owner" : "jlee89",
        "port" : port_number,
        "project" : project_name,
    }   

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    sock.sendto(bytes(json.dumps(dic), "utf-8"), (socket.gethostbyname('catalog.cse.nd.edu'), 9097))


# return a socket on a viable port number
def setup_port():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setblocking(0)

    port_number = 5000
    while True:
        try:
            server_socket.bind((socket.gethostname(), port_number))
            break
        except socket.error as e:
            port_number+=1

    server_socket.listen(5)
    return (server_socket, port_number)

def check_status(server_list, self_id, direction):
    connection_count = 0
    test_server = server_list[self_id][direction]

    dead_server = []
    live_server = -1



    message = json.dumps({'method':'server_alive'})
    while not connection_count and test_server != self_id:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                print('connecting to', server_list[test_server]['host'])
                s.connect((server_list[test_server]['host'], server_list[test_server]['port']))
                msg_len = str(len(str(message)))
                s.sendall(bytes(msg_len, 'utf-8'))
                s.sendall(bytes(message, 'utf-8'))
                data = str(s.recv(1024))
                live_server = test_server
                connection_count += 1 # added because it was infinite looping but not really sure !!
                
            except (socket.timeout, ConnectionRefusedError) as e:
                dead_server.append(test_server)
                test_server = server_list[test_server][direction]

    if live_server == -1:
        live_server = self_id
    return dead_server, live_server
                
                
 

# main running program of the server
def active_server(project_name):
    server_socket = setup_port()

    port_number = server_socket[1]
    server_socket = server_socket[0]

    read_list = [server_socket]
    init_time = time.time_ns()

    server_count = 0
    server_list = {}
    responsibility_list = {}

    server_first = 0
    server_last = 0


    update_info(project_name, port_number)
    print("Established Port on " + socket.gethostname() + ":" + str(port_number))

    while True:
        readable, writable, errored = select.select(read_list, [], [], .01)
        if server_socket in readable:
            (clientsocket, address) = server_socket.accept()
            print('Connected with:', address[0] + ':' + str(address[1]))
            data = clientsocket.recv(6)
            data = clientsocket.recv(int(data))
            data = json.loads(data)

            response = None

            if data['type'] == 'hashtableserver':
                if 'self-id' in data:
                    direction = 'next'

                    server_list[int(data['self-id'])]['host'] = data['host']
                    server_list[int(data['self-id'])]['port'] = data['port']

                    maintaining_server = responsibility_list[int(data['self-id'])]
                    take_over_responsibilities = False

                    if int(maintaining_server) != int(data['self-id']):
                        if int(maintaining_server) == int(data['self-id'])-1 or int(maintaining_server) == int(data['self-id'])+1:
                            take_over_responsibilities = True

                        if int(maintaining_server) < int(data['self-id']):
                            server_prev = server_list[int(maintaining_server)]
                            server_next = server_list[server_list[int(maintaining_server)]['next']]
                        elif int(maintaining_server) > int(data['self-id']):
                            server_prev = server_list[server_list[int(maintaining_server)]['prev']]
                            server_next = server_list[int(maintaining_server)]
   

                        
                        server_prev['next'] = int(data['self-id'])
                        server_next['prev'] = int(data['self-id'])
                        prev_node = server_prev['host'] + ":" + str(server_prev['port'])
                        next_node = server_next['host'] + ":" + str(server_next['port'])
                        response = {'prev': prev_node, 'next': next_node, 'server_count': server_count}


                        responsibility_list[int(data['self-id'])] = int(data['self-id'])

                        if take_over_responsibilities:
                            take_over = []
                            for key, value in responsibility_list.items():
                                if int(value) == int(maintaining_server):
                                    take_over.append(key)
                            for key in take_over:
                                responsibility_list[key] = int(data['self-id'])
                            response['responsibilities'] = ','.join([str(x) for x in take_over])
                    else:    
                        server_prev = server_list[int(maintaining_server)]['prev']
                        server_next = server_list[int(maintaining_server)]['next']
   
                        prev_node = server_list[server_prev]['host'] + ":" + str(server_list[server_prev]['port'])
                        next_node = server_list[server_next]['host'] + ":" + str(server_list[server_next]['port'])
                        response = {'prev': prev_node, 'next': next_node, 'server_count': server_count}
                else:
                    if server_count == 0:
                        server_list[server_count] = {'host':data['host'], 'port':data['port'], 'prev':0, 'next':0}
                        node = server_list[server_first]['host'] + ":" + str(server_list[server_first]['port'])
                        response = {'prev': node, 'next': node, 'server_count': server_count+1}
                    else:
                        server_list[server_count] = {'host':data['host'], 'port':data['port'], 'prev':server_last, 'next':server_first}
                        server_list[server_first]['prev'] = server_count
                        server_list[server_last]['next'] = server_count
                        next_node = server_list[server_first]['host'] + ":" + str(server_list[server_first]['port'])
                        prev_node = server_list[server_last]['host'] + ":" + str(server_list[server_last]['port'])
                        response = {'prev': prev_node, 'next': next_node, 'server_count': server_count+1}
   
                    responsibility_list[server_count] = server_count

                    server_last = server_count
                    server_count += 1
                print(server_list)

            elif data['type'] == 'hashtableclient':
                random_generated = []
                response = {}
                if server_count > 0:
                    for i in range(0,3):
                        if i < server_count:
                            random_numbers = random.randint(0,server_count-1)  
                            while random_numbers in random_generated:
                                random_numbers = random.randint(0,server_count-1)
                            random_generated.append(random_numbers)
                            response[str(i)] = server_list[random_numbers]['host'] + ":" + str(server_list[random_numbers]['port'])

            elif data['type'] == 'hashtableserver-neighbor_error':
                dead_server, live_server = check_status(server_list, data['self-id'], data['direction'])
                for server in dead_server:
                    responsibility_list[server] = data['self-id']
                server_list[data['self-id']][data['direction']] = live_server
                if server_first in dead_server:
                    server_first = data['self-id']
                if server_last in dead_server:
                    server_last = data['self-id']

                response = {'change':server_list[live_server]['host']+":"+str(server_list[live_server]['port']), 'direction':data['direction'], 'replace':','.join([str(x) for x in dead_server])}
                print(response)
            else:
                response['error'] = 'no server available'    
    


            if response:
                msg_len = str(len(str(response)))
                while len(msg_len) < 6:
                    msg_len = '0' + msg_len    
                clientsocket.sendall(bytes(str(msg_len),'utf-8'))
                clientsocket.sendall(bytes(json.dumps(response), 'utf-8'))
                clientsocket.close()
    
        if (time.time_ns() - init_time)/60000000000 >= 1:
            init_time = time.time_ns()
            update_info(project_name, port_number)



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Need Project Name")
        exit(1)
    active_server(sys.argv[1])
    
    

