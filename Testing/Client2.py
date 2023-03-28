import sys, os
import socket
import select
import json
import time
import threading
import http.client


def main():
        message = {
            "type": "hashtableclient",
        }

        nameserver = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        nameserver.request("GET", "/query.json")
        response = nameserver.getresponse()
        data = json.loads(response.read().decode('utf-8'))

        nameservers = [] # list of all possible project nameservers
        for line in data:
            if line['type'] == 'nameserver' and line['project'] == 'jlee89' + "-NS":
                nameservers.append(line)
        
        # sort nameservers by last heard from
        nameservers = sorted(nameservers, key = lambda item: item['lastheardfrom'], reverse = True)
        
        #print('nameservers', self.nameservers)
        if len(nameservers) < 1:
            print("Unable to find nameserver, trying again...")
            return False

        # receive info from nameserver about next node, previous node, and server count
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            for name in nameservers:
                print('checking names:', name)
                try:
                    s.connect((name['name'], name['port']))
                    msg_len = str(len(str(message)))
                    while len(msg_len) < 6:
                        msg_len = '0' + msg_len
                    s.sendall(bytes(msg_len, 'utf-8'))
                    s.sendall(bytes(json.dumps(message), 'utf-8'))
                    data = s.recv(6)
                    data = s.recv(int(data))
                    data = json.loads(data)
                    print(data)

                    # attempt to connect to the server sent
                    for server in data:
                        print(data[server]) # server information
                        server_info = data[server].split(":")
                        print(server_info) # server information
                        new_host = server_info[0]
                        new_port = int(server_info[1])
                        info = (socket.getaddrinfo(new_host, new_port, proto=socket.IPPROTO_TCP))
                        address_family = info[0][0]
                        socket_type = info[0][1]
                        proto = info[0][2]

                        try:
                            client_socket = socket.socket(address_family, socket_type, proto)
                            client_socket.connect((new_host, new_port))
                            print('host', new_host, 'port', new_port)
                            send_info(client_socket)
                        except Exception as e:
                            print('host', new_host, 'exception', e)

                except Exception as e:
                    print('unable to connect, exception', e)

def send_info(client_socket):
    insert_stub = json.dumps({"method":"insert", "key":"key", "value":"value"})
    length = str(len(insert_stub))
    message = bytes(length, 'utf-8') + bytes(insert_stub, 'utf-8')
    
    client_socket.sendall(message)
    response = client_socket.recv(1024).decode('utf-8')
    print("RESPONSE", response)

    lookup_stub = json.dumps({"method":"lookup", "key":"key"})
    length = str(len(lookup_stub))
    message = bytes(length, 'utf-8') + bytes(lookup_stub, 'utf-8')
    
    client_socket.sendall(message)
    response = client_socket.recv(1024).decode('utf-8')
    print("RESPONSE", response)

    remove_stub = json.dumps({"method":"remove", "key":"key"})
    length = str(len(remove_stub))
    message = bytes(length, 'utf-8') + bytes(remove_stub, 'utf-8')
    
    client_socket.sendall(message)
    response = client_socket.recv(1024).decode('utf-8')
    print("RESPONSE", response)



main()

