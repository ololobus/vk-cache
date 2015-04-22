#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import re
import networkx as nx
import operator
import random
import numpy as np

from dateutil.relativedelta import relativedelta
from collections import Counter
from pymongo import MongoClient
from harmonic_centrality import harmonic_centrality

method = 'calculate'
method_type = ''

min_size = 1000
max_size = 1601

logins_path = '/opt/logins.txt'
logins_path = '/Users/alexk/Downloads/logins'

mongo = MongoClient()
db = mongo.vk

reference_date = datetime.datetime.strptime('01.04.2015', '%d.%m.%Y')


# Get script params
if len(sys.argv) > 1:
    method = sys.argv[1]

if len(sys.argv) > 2:
    method_type = sys.argv[2]


if method == 'calculate':
    groups = db.groups.find({ 'count': { '$gt': min_size, '$lt': max_size } })

    for g in groups:
        stats = {
            'gender': { 'male': 0, 'female': 0, '?': 0 },
            'age': { '<=10': 0, '11-20': 0, '21-30': 0, '>=31': 0, '?': 0},
            'top_interest': ''
        }
        users = db.users.find({ 'gid': g['_id'] })

        group_words = []

        for u in users:
            if u['sex'] == 1:
                stats['gender']['female'] += 1
            elif u['sex'] == 2:
                stats['gender']['male'] += 1
            else:
                stats['gender']['?'] += 1

            if 'bdate' in u:
                bdate = u['bdate']

                if bdate.count('.') == 2:
                    try:
                        bdate = datetime.datetime.strptime(bdate, '%d.%m.%Y')
                        years = relativedelta(reference_date, bdate).years
                    except:
                        years = None

                    if years and years <= 10:
                        stats['age']['<=10'] += 1
                    elif years > 10 and years <= 20:
                        stats['age']['11-20'] += 1
                    elif years > 20 and years <= 30:
                        stats['age']['21-30'] += 1
                    elif years >= 31:
                        stats['age']['>=31'] += 1
                    else:
                        stats['age']['?'] += 1
                else:
                    stats['age']['?'] += 1
            else:
                stats['age']['?'] += 1

            if 'interests' in u:
                words = re.findall(ur'[\u0400-\u0500a-z\s\'\"]{4,}', u['interests'].lower())
                words = [word.strip() for word in words]
                ucounts = Counter(words).most_common(300)
                ucounts = map(lambda c: c[0], ucounts)
                group_words.extend(ucounts)

        word_counts = Counter(group_words)
        stats['top_interest'] = word_counts.most_common(1)[0][0]
        db.groups.update({ '_id': g['_id'] }, { '$set': { 'stats': stats } }, upsert = False, multi = False)

        print stats

if method == 'network':
    gid = '19720218'
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

    uids = random.sample(nodes, logins.count() * 3)

    success = 0
    print 'uid | max | mean'

    if method_type == 'update':
        for u in logins:
            login = u['_id']
            uid = int(u['lab5s']['uid'])

            paths = nx.single_source_shortest_path_length(graph, uid)
            sorted_paths = sorted(paths.items(), key = operator.itemgetter(1), reverse = True)

            max_path = sorted_paths[0][1]
            mean_path = np.mean(map(lambda p: p[1], sorted_paths[:-1]))

            mongo.npl.students.update({ '_id': login }, { '$set': { 'lab5s': { 'gid': gid, 'uid': uid, 'max': max_path, 'mean': mean_path, 'old_max': u['lab5s']['max'], 'old_mean': u['lab5s']['mean'] } } }, upsert = False, multi = False)

            print '%s %s %s assigned to %s' % (uid, max_path, mean_path, login)
    else:
        for uid in uids:
            try:
                paths = nx.single_source_shortest_path_length(graph, uid)
                sorted_paths = sorted(paths.items(), key = operator.itemgetter(1), reverse = True)
                if len(sorted_paths) >= 7:
                    max_path = sorted_paths[0][1]
                    mean_path = np.mean(map(lambda p: p[1], sorted_paths))
                    login = logins[success]['_id']

                    mongo.npl.students.update({ '_id': login }, { '$set': { 'lab5s': { 'gid': gid, 'uid': uid, 'max': max_path, 'mean': mean_path } } }, upsert = False, multi = False)

                    success += 1
                    print '%s %s %s assigned to %s' % (uid, max_path, mean_path, login)
            except:
                pass

            if success == logins.count():
                break


if method == 'pagerank':
    gid = '19720218'

    group = db.bgroups.find_one({ '_id': gid })
    users = db.graph_users.find({ 'gid': group['_id'] })

    nodes = set(map(lambda u: u['id'], users))
    graph = nx.Graph()

    for uid in nodes:
        friends = db.user_friends.find_one({ '_id': uid })
        if friends is None:
            friends = []
        for f in friends['friends']:
            if f in nodes:
                graph.add_edge(int(uid), int(f))

        followers = db.followers.find_one({ '_id': uid })
        if followers is None:
            followers = []
        for f in followers['followers']:
            if f in nodes:
                graph.add_edge(int(uid), int(f))

    pagerank = nx.pagerank(graph, alpha = 0.85)
    sorted_ranks = sorted(pagerank.items(), key = operator.itemgetter(1), reverse = True)

    harmonic = harmonic_centrality(graph)
    sorted_harmonic = sorted(harmonic.items(), key = operator.itemgetter(1), reverse = True)

    print sorted_ranks[0:20], sorted_harmonic[0:20]