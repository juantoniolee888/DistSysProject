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

                except Exception as e:
                    print('unable to connect, exception', e)

main()

