#!/usr/bin/env python3

import sys
import os
import socket
import select
import json
import time
import threading
import http.client 

class HashTableClient():
    
    def __init__(self, project_name):
        self.project_name = project_name

        while not self.find_nameserver(): # go to catalog and connect to our nameserver
            continue 

    def __del__(self):
        try:
            self.client_socket.close()
        except AttributeError:
            pass # there is no client socket to close

    def find_nameserver(self):
        self.message = {
            "type": "hashtableclient",
        }
        catalog = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        catalog.request("GET", "/query.json")
        response = catalog.getresponse()
        data = json.loads(response.read().decode('utf-8'))

        self.nameservers = [] # all the nameservers it can find in the catalog

        for line in data:
            if 'type' and 'project' in line:
                if line['type'] == 'nameserver' and line['project'] == self.project_name + '-NS':
                    self.nameservers.append(line)

        self.nameservers = sorted(self.nameservers, key = lambda item: item['lastheardfrom'], reverse=True)
        if len(self.nameservers) < 1:
            print("unable to find nameserver, trying again...")
            return False

        # receive info from nameserver about next node, previous node, and server count
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            for name in self.nameservers:
                try:
                    s.connect((name['name'], name['port']))
                    msg_len = str(len(str(self.message)))
                    while len(msg_len) < 6:
                        msg_len = '0' + msg_len
                    s.sendall(bytes(msg_len, 'utf-8'))
                    s.sendall(bytes(json.dumps(self.message), 'utf-8'))
                    data = s.recv(6)
                    data = s.recv(int(data))
                    data = json.loads(data)

                    # if the data is that there are no servers available, don't even try
                    try:
                        print("Data error:", data['error'])
                        return False
                    except Exception:
                        pass

                    # attempt to connect to the server that was sent
                    for server in data:
                        server_info = data[server].split(":")
                        new_host = server_info[0]
                        new_port = int(server_info[1])
                        info = (socket.getaddrinfo(new_host, new_port, proto=socket.IPPROTO_TCP))
                        address_family = info[0][0]
                        socket_type = info[0][1]
                        proto = info[0][2]

                        try:
                            self.client_socket = socket.socket(address_family, socket_type, proto)
                            self.client_socket.connect((new_host, new_port))
                            self.client_socket.settimeout(20) 
                            print('host', new_host, 'port', new_port)
                            return True
                            # once you have found a server, continue
                        except Exception as e:
                            print('host', new_host, 'exception', e)
                except Exception as e:
                    print("unable to connect, exception", e)
            return False


# client stubs
    def insert(self, key=None, value=None):
        if key == None or value == None: # user did not pass enough values
            return {'error': 'not enough input'}
        # make JSON for inserting key/value
        # include backup so that the server knows the data still needs to be backed up
        client_stub = json.dumps({"method":"insert", "key":key, "value":value, "backup": False})
        response = self.send_and_wait(client_stub)
        print("response", response)
        if response["success"] == "true":
            return "success"
        else:
            return "response"

    def lookup(self, key=None):
        if key == None:
            return {'error': 'not enough input'}
        # make JSON for lookup
        client_stub = json.dumps({"method":"lookup", "key":key, "backup": False})
        response = self.send_and_wait(client_stub)
        if response["success"] == "true":
            return response["value"]
        else:
            return response

    def remove(self, key=None):
        if key == None:
            return {'error': 'not enough input'}
        # make JSON for remove
        client_stub = json.dumps({"method":"remove", "key":key, "backup": False})
        response = self.send_and_wait(client_stub)
        if response["success"] == "true":
            return "success"
        else:
            return response

    def size(self):
        # make JSON for size
        client_stub = json.dumps({"method":"size", "backup": False})
        response = self.send_and_wait(client_stub)
        if response["success"] == "true":
            return response["size"]
        else:
            return response

    def query(self, subkey=None, subvalue=None):
        if subkey == None or subvalue == None:
            return {'error': 'not enough input'}
        # make JSON for query
        client_stub = json.dumps({"method":"query", "subkey":subkey, "subvalue":subvalue, "backup": False})
        response = self.send_and_wait(client_stub)
        if response["success"] == "true":
            return response["values"]
        else:
            return response

    # send and wait will send the client stub to the server and wait until it gets a response
    def send_and_wait(self, client_stub):
        length = str(len(client_stub))
        self.server_message = bytes(length, 'utf-8') + bytes(client_stub, 'utf-8')

        # send the message to the server
        try:
            self.client_socket.sendall(self.server_message)
        except (ConnectionResetError, BrokenPipeError, socket.timeout) as e:
            response = {"success":"false", "error": "exception" + str(e)}
            return response

        # wait for response from server since it successfully sent the message
        response = self.wait_for_response()
        try:
            response = json.loads(response)
            return response
        except Exception as e: # if unable to load it as json, just return the response
            return response 
            

    # receives data & makes sure it has the full message before returning
    def wait_for_response(self):
        self.data = self.get_data() # receives data & checks for timeouts

        found_num = False
        num = ''
        count = 0

        # find the number for how much data you should have
        while not found_num:
            while count >= len(self.data): # you need more data to continue
                self.data += self.get_data()

            # check if there is a digit (for the number)
            if self.data[count].isdigit():
                num += self.data[count]
                count += 1
            else:
                found_num = True # reached the end of the number & am looking at the data

        # make sure you have enough data as given by the number
        msg_length = count+int(num)
        while len(self.data) < msg_length: 
            self.data += self.get_data()

        # if you have more than the length keep the leftover data
        if len(self.data) > msg_length:
            print("YOU HAVE MORE THAN ENOUGH CUTTING OFF DATA")
            self.data = self.data[msg_length:]
            count = 0
        
        # return the data after the number to the end of the message
        return self.data[count:msg_length]


    # receive data from server and checks for timeouts & sleeps if unable to connect
    def get_data(self):
        data = ''
        sleep_time = 1
        while sleep_time < 30: # wait for a maximum of 30 seconds
            try:
                data = self.client_socket.recv(1024).decode('utf-8')
                if len(data) > 0:
                    return data
                else:
                    self.client_socket.sendall(self.server_message)
                    data = self.client_socket.recv(1024).decode('utf-8')
                    if len(data) > 0:
                        return data
            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError):
                time.sleep(sleep_time) # sleep before trying to connect again
                while not self.find_nameserver():
                    continue
                self.client_socket.sendall(self.server_message)
                if sleep_time == 1: # increase the sleep time after trying again
                    sleep_time += 1 
                else:
                    sleep_time *= sleep_time

        return data


def main():
    if len(sys.argv) != 2:
        print("Wrong number of command line arguments, please give a project name")
        return
    project_name = sys.argv[1]
    client = HashTableClient(project_name)

# call main execution
if __name__ == '__main__':
    main()
