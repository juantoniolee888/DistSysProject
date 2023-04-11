#!/usr/bin/env python3

import json


class HashTable():
    def __init__(self):
        self.hash_table = {} # create new hash table
    
    def insert(self, key,value):
        if  key not in self.hash_table:
            self.hash_table[key] = value
            return "inserted"
        else:
            if self.hash_table[key] == value:
                return "key already in table"
            else:
                return None

    def lookup(self, key):
        if key in self.hash_table:
            value = self.hash_table[key]
            return value
        else:
            return None

    def remove(self, key):
        if key in self.hash_table:
            del self.hash_table[key]
            return "removed"
        else:
            return None
    
    def size(self):
        size = len(self.hash_table)
        return size

    def query(self, subkey, subvalue):
        values = []
        for key in self.hash_table:
            if type(self.hash_table[key]) != dict:
                continue # we assume they are dictionaries
            for key2 in self.hash_table[key]:
                if subkey == key2 and subvalue == self.hash_table[key][key2]:
                    values.append((key, self.hash_table[key]))
        if len(values) > 0:
            return values
        else:
            return None

    def return_hash_table(self):
        return self.hash_table
