#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import networkx as nx
import operator

from pymongo import MongoClient


mongo = MongoClient()
db = mongo.vk

gid = '19720218'
output_path = '%s_adjacency_list.txt' % gid

logins = mongo.npl.students.find().sort('_id', 1)

group = db.bgroups.find_one({ '_id': gid })
users = db.graph_users.find({ 'gid': group['_id'] })

nodes = set(map(lambda u: u['id'], users))
graph = nx.Graph()

for u in users:
    graph.add_node(int(u['id']))

for uid in nodes:
    friends = db.user_friends.find_one({ '_id': uid })
    if friends is None:
        continue
    for f in friends['friends']:
        if f in nodes:
            graph.add_edge(int(uid), int(f))

with open(output_path, 'w') as out:
    for n in sorted(graph.edge.items(), reverse = False):
        for e in sorted(n[1]):
            print >> out, '%s %s' % (n[0], e)