#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import re
import networkx as nx
import operator
import random
import numpy as np
import community as comm

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

def load_graph(gid, with_followers = False):
    users = db.graph_users.find({ 'gid': str(gid) }, { 'id': 1, '_id': 0 })

    nodes = set()
    graph = nx.Graph()

    for u in users:
        nodes.update([u['id']])

    print 'Nodes:', len(nodes)

    for uid in nodes:
        friends = db.user_friends.find_one({ '_id': uid })
        if friends is None:
            friends = { 'friends': [] }
        for f in friends['friends']:
            if f in nodes:
                graph.add_edge(int(uid), int(f))

        if with_followers:
            followers = db.followers.find_one({ '_id': uid })
            if followers is None:
                followers = { 'followers': [] }
            for f in followers['followers']:
                if f in nodes:
                    graph.add_edge(int(uid), int(f))

    print 'Graph loaded'

    return graph

def modularity(subgs, G):
    Q = 0
    total_edges = float(nx.number_of_edges(G))

    for g in subgs:
        degree_sum = sum(nx.degree(g).values())
        edges_num = nx.number_of_edges(g)

        Q += edges_num / total_edges - (degree_sum / (2 * total_edges))**2

    return Q

def count_trinodes(node, G, subg = None):
    edges = G.edge
    trinodes = set()
    if subg:
        subg_nodes = subg.node.keys()

    for n in edges[node]:
        if subg and n in subg_nodes:
            continue
        for e in edges[n]:
            if e != node and e in edges[node]:
                trinodes.update([n])
                break


    return len(trinodes)

def wcc(subgs, G):
    Q = 0

    for g in subgs:
        q = 0
        nodes = g.node.keys()
        nodes_len = len(nodes)

        for n in nodes:
            tG = nx.triangles(G, n)

            if tG != 0:
                tS = float(nx.triangles(g, n))
                vtG = count_trinodes(n, G)
                vtGS = count_trinodes(n, G, g)

                q += tS / tG * vtG / (nodes_len - 1 + vtGS)

        Q += q / nodes_len

    return Q / len(subgs)

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

if method == 'paths':
    gid = '19720218'
    logins = mongo.npl.students.find().sort('_id', 1)

    graph = load_graph(gid)

    uids = graph.node.keys()

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


if method == 'centrality':
    gid = '19720218'
    # gid = '26953'

    logins = mongo.npl.students.find().sort('_id', 1)
    logins_count = logins.count()

    astep = (0.9 - 0.7) / (logins_count - 1)
    alphas = np.append(np.arange(0.7, 0.9, astep), 0.9)

    print alphas
    print len(alphas), logins_count

    graph = load_graph(gid, True)

    harmonic = harmonic_centrality(graph)
    sorted_harmonic = sorted(harmonic.items(), key = operator.itemgetter(1), reverse = True)
    db.bgroups.update({ '_id': gid }, { '$set': { 'harmonic_centrality_top200': map(lambda u: u[0], sorted_harmonic[0:200]) } }, upsert = False, multi = False)

    print 'Harmonic:', map(lambda u: u[0], sorted_harmonic[0:5])


    for login, alpha in zip(logins, alphas):
        pagerank = nx.pagerank(graph, alpha = alpha)
        sorted_ranks = sorted(pagerank.items(), key = operator.itemgetter(1), reverse = True)
        mongo.npl.students.update({ '_id': login['_id'] }, { '$set': { 'lab6': { 'alpha': alpha, 'pagerank_top200': map(lambda u: u[0], sorted_ranks[0:200]) } } }, upsert = False, multi = False)

        print '%s pagerank with alpha=%s:' % (login['_id'], alpha), map(lambda u: u[0], sorted_ranks[0:5])


if method == 'cores':
    gid = '19720218'

    graph = load_graph(gid, True)

    max_k = 0

    while True:
        max_k += 1
        k_core = nx.k_core(graph, k = max_k)

        if not bool(k_core.node):
            max_k -= 1
            break

    k4_cores = list(nx.connected_component_subgraphs(nx.k_core(graph, k = 4)))
    kmax_cores = list(nx.connected_component_subgraphs(nx.k_core(graph, k = max_k)))

    k4_mod = modularity(k4_cores, graph)
    kmax_mod = modularity(kmax_cores, graph)

    k4_wcc = wcc(k4_cores, graph)
    kmax_wcc = wcc(kmax_cores, graph)

    dendro = comm.generate_dendrogram(graph)

    louvain_steps = []
    for level in range(len(dendro)):
        louvain_steps.append(len(set(comm.partition_at_level(dendro, level).values())))

    result = { 'max_core': max_k, 'num_4-cores': len(k4_cores), 'modularity_max-cores': kmax_mod, 'modularity_4-cores': k4_mod, "wcc_max-cores": kmax_wcc, "wcc_4-cores": k4_wcc, 'louvain_steps': louvain_steps }

    db.bgroups.update({ '_id': gid }, { '$set': result }, upsert = False, multi = False)

    print result
