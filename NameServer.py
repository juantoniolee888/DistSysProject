import sys
import socket
import select
import json
from json.decoder import JSONDecodeError
import os
import shutil
import time


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

# main running program of the server
def active_server(project_name):
    server_socket = setup_port()

    port_number = server_socket[1]
    server_socket = server_socket[0]

    read_list = [server_socket]
    init_time = time.time_ns()


    update_info(project_name, port_number)
    print("Established Port on " + socket.gethostname() + ":" + str(port_number))

    while True:
        readable, writable, errored = select.select(read_list, [], [], .01)
        if server_socket in readable:
            (clientsocket, address) = server_socket.accept()
            print('Connected with:', address[0] + ':' + str(address[1]))
            clientsocket.close()
    
        if (time.time_ns() - init_time)/60000000000 >= 1:
            init_time = time.time_ns()
            update_info(project_name, port_number)



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Need Project Name")
        exit(1)
    active_server(sys.argv[1])
    
    

