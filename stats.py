#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from pymongo import MongoClient

method = 'calculate'

min_size = 1000
max_size = 1601

logins_path = '/Users/alexk/Downloads/logins'

mongo = MongoClient()
db = mongo.vk


# Get script params
if len(sys.argv) > 1:
    method = sys.argv[1]


if method == 'assign':
    groups = db.groups.find({ 'count': { '$gt': min_size, '$lt': max_size } })
    logins = open(logins_path).readlines()

    if groups.count() < len(logins):
        print 'Not enough groups with appropriate size!'
    else:
        for i in range(len(logins)):
            g = groups[i]

            # db.groups.save(g)
            db.groups.update({ '_id': g['_id'] }, { '$set': { 'assigned_to': logins[i].rstrip('\n') } }, upsert = False, multi = False)
            print 'Group # %s assigned to %s' % (g['_id'], logins[i])



if method == 'calculate':
    print ''