#!/usr/bin/env python3

import sys, os
import socket
import select
import json
import time
import threading
import http.client
import random

from HashTable import *

# server structure

class HashTableServer():

    def __init__(self, project_name):
        self.project_name = project_name
        self.hash_table = HashTable()
        self.host = socket.gethostname()
        self.port = 0

        #machine information for next and prev nodes
        self.prev = None
        self.next = None
        self.server_count = 0


        # set up timer for connecting to nameserver (still want to do 60 sec?)
#        t = threading.Timer(60.0, self.find_nameserver)
#        t.start()
        self.connect_to_nameserver()
        self.checkpoint()
        self.receive_from_client()
    
    # send server information to nameserver
    def connect_to_nameserver(self): 
        # TCP socket
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(0) 

        self.server.bind((self.host, self.port))
        self.server.listen(5)

        # figure out which port it connected to
        self.port = self.server.getsockname()[1]

        # create message to nameserver
        self.message = {
            "type": "hashtableserver",
            "owner": "cgoodwi2",
            "port": self.port,
            "project": self.project_name,
            "host": self.host,
        }
        self.message = json.dumps(self.message) # make it a string to send

        # make UDP socket
        self.message_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
        self.find_nameserver()

        self.connections = [self.server]
        self.conn_received = {}
        self.conn_received[self.server] = ''

        print("Listening on port:", self.port)
        print("Host name:", self.host)

    # connect to catalog to find project nameserver
    def find_nameserver(self):
        nameserver = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        nameserver.request("GET", "/query.json")
        response = nameserver.getresponse()
        data = json.loads(response.read().decode('utf-8'))

        self.nameservers = [] # list of all possible project nameservers
        for line in data:
            if line['type'] == 'nameserver' and line['project'] == self.project_name.strip() + "-NS":
                self.nameservers.append(line)
        
        # sort nameservers by last heard from
        self.nameservers = sorted(self.nameservers, key = lambda item: item['lastheardfrom'], reverse = True)
        
        #print('nameservers', self.nameservers)
        if len(self.nameservers) < 1:
            print("Unable to find nameserver, trying again...")
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
                    s.sendall(bytes(self.message, 'utf-8'))
                    data = s.recv(6)
                    data = s.recv(int(data))
                    data = json.loads(data)

                    self.prev = data['prev'].split(":")
                    self.next = data['next'].split(":")
                    self.server_count = data['server_count']

                    if self.prev and self.next and self.server_count:
                        print(self.prev, self.next, self.server_count)
                        break

                except Exception as e:
                    print('unable to connect, exception', e)

    def checkpoint(self):
        current_id = 0 # keeps track of the last transaction id in the checkpoint
        
        # check if the checkpoint has data
        try:
            ckpt = open('table.ckpt', 'r')
            lines = ckpt.readlines()
            for line in lines:
                try:
                    line = json.loads(line)
                except Exception:
                    break # if it can't load the json then it didn't finish writing it 
                self.hash_table.insert(line['key'], line['value'])
                current_id = line['id']
        except FileNotFoundError:
            pass # no checkpoint

        # check if the log has data
        try:
            log = open('table.txn', 'r')
            lines = log.readlines()
            for line in lines:
                try:
                    if '\n' not in line: # didn't complete writing log transaction
                        continue
                    line = json.loads(line)
                    if line['method'] == 'insert' and line['id'] > current_id:
                        self.hash_table.insert(line['key'], line['value'])
                    elif line['method'] == 'remove' and line['id'] > current_id:
                        self.hash_table.remove(lines['key'])
                except Exception:
                    continue # unable to load json for some reason
        except FileNotFoundError:
            pass # there just wasn't a table

    def receive_from_client(self):
        self.transaction_count = 0 # compact the log every 100 transactions
        self.transaction_id = 0 # no transactions yet

        while True:
            read_sockets, write_sockets, error_sockets = select.select(self.connections, [], [])
            socket = read_sockets[random.randrange(0, len(read_sockets))]

            if socket == self.server:
                self.accept_new() # there is a new client to accept
            else:
                self.current_connection = socket
                self.look_for_message()
        self.server.close()

    # accept new client: called when there is a new client ready to connect
    def accept_new(self):
        self.conn, self.address = self.server.accept()
        self.connections.append(self.conn)
        self.conn_received[self.conn] = ''

        self.conn.settimeout(1)
        self.current_connection = self.conn
        self.look_for_message()


    def look_for_message(self):
        # check if there is a message waiting
        if self.current_connection:
            try:
                received_data = self.current_connection.recv(1024).decode('utf-8')
            except Exception as e:
                print("could not get more data due to:", e)
                return
            self.conn_received[self.current_connection] += received_data
            if len(self.conn_received[self.current_connection]) > 0:
                self.found_num = False
                self.num = ''
                self.count = 0
                self.find_num() # find the number for the amount of data to look for
                if not self.found_num:
                    return # did not have enough data to even find the number of the length of the data
                operation_len = self.count + int(self.num) # size of the string for the operation
                if len(self.conn_received[self.current_connection]) >= operation_len:
                    self.result = self.decode_json(self.conn_received[self.current_connection][self.count:operation_len])
                    self.send_result()
            # otherwise you need more data so just return

    def find_num(self):
        while not self.found_num:
            if self.count >= len(self.conn_received[self.current_connection]):
                return
            if self.conn_received[self.current_connection][self.count].isdigit():
                self.num += self.conn_received[self.current_connection][self.count]
                self.count += 1
            else:
                self.found_num = True


    def send_result(self):
        # given a result, send that information to the client
        result_len = str(len(self.result))
        try:
            self.current_connection.sendall(bytes(result_len, 'utf-8') + bytes(self.result, 'utf-8'))
        except (BrokenPipeError, ConnectionResetError) as e:
            self.connections.remove(self.current_connection) # no longer active
            return
        self.conn_received[self.current_connection] = self.conn_received[self.current_connection][int(self.num)+self.count:]

        # if it's coming from a server you're done unless the key is wrong
        # final server destination in the string
        # if it's coming from a client give it to previous and next

    def decode_json(self, json_data):
        # given json data from the client, decode and call resulting functions
        self.data = json.loads(json_data)
        if self.data['method'] == 'insert':
            key = self.data['key']
            value = self.data['value']
            result = self.hash_table.insert(key,value)
            if result == 'inserted': # based on result in the hash table
                log = open('table.txn', 'a')
                log.write(json.dumps({'method':'insert', 'key':key,'value':value, 'id':self.transaction_id}))
                log.write('\n') # to show end of this log value
                log.flush()
                os.sync()
                log.close()
                json_result = json.dumps({"success":"true", "inserted":"true"})
            elif result =='key already in table': # does not need to be written to log
                json_result = json.dumps({"success":"true", "inserted":"false"})
            else: # result = None
                json_result = json.dumps({"success":"false", "error":"key already in hash table", "key_title":key})
        elif self.data['method'] == 'lookup':
            key = self.data['key']
            result = self.hash_table.lookup(key)
            if result:
                json_result = json.dumps({"success":"true", "value":result})
            else:
                json_result = json.dumps({"success":"false", "error":"key not in table"})
        elif self.data['method'] == 'remove':
            key = self.data['key']
            result = self.hash_table.remove(key)
            if result:
                json_result = json.dumps({"success":"true", "removed":"true"})
                log = open('table.txn', 'a')
                log.write(json.dumps({"method":"remove", "key":key, "id":self.transaction_id}))
                log.write("\n")
                log.flush()
                os.sync()
                log.close()
            else:
                json_result = json.dumps({"success":"true", "removed":"false"}) # key not in table already
        elif self.data['method'] == 'size':
            result = self.hash_table.size()
            if result:
                json_result = json.dumps({"success":"true", "size":result})
            else:
                json_result = json.dumps({"success":"false", "error":"size failure"})
        elif self.data['method'] == 'query':
            subkey = self.data['subkey']
            subvalue = self.data['subvalue']
            result = self.hash_table.query(subkey, subvalue)
            if result:
                json_result = json.dumps({"success":"true", "values":result})
            else:
                json_result = json.dumps({"success":"false", "error":"no matches found"})
        self.transaction_count += 1
        if self.transaction_count >= 100:
            self.transaction_count = 0
            self.backup_table()
        self.transaction_id += 1
        return json_result


    def backup_table(self):
        # given the current table, write to checkpoint and delete log
        backup = os.open('backup.ckpt', os.O_WRONLY|os.O_FSYNC|os.O_CREAT)
        for key in self.hash_table.hash_table:
            os.write(backup, str.encode(json.dumps({"key":key, "value":self.hash_table.hash_table[key], "id": self.transaction_id})))
            os.write(backup, str.encode("\n"))
        os.renmae('backup.ckpt', 'table.ckpt')

        os.fsync(backup)
        os.close(backup)
        try:
            os.remove('table.txn')
        except FileNotFoundError:
            pass

def main():
    if len(sys.argv) != 2:
        print("Wrong number of command line arguments, please give project name")
        return
    project_name = sys.argv[1]
    server = HashTableServer(project_name)


# call main execution
if __name__ == '__main__':
    main()
