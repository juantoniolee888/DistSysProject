import sys
import socket
import json
import os
import time

def main():
    skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    skt.connect(("student10.cse.nd.edu", 5000))

main()

