#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import re
import networkx as nx
import operator

from dateutil.relativedelta import relativedelta
from collections import Counter
from pymongo import MongoClient

method = 'logins'

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


if method == 'logins':
    logins = sorted(open(logins_path).readlines())
    db = mongo.npl

    for l in logins:
        if l != '':
            db.students.save({ '_id': l.rstrip('\n') })

if method == 'groups':
    groups = db.groups.find({ 'count': { '$gt': min_size, '$lt': max_size } }).sort('_id', 1 )
    logins = sorted(open(logins_path).readlines())

    if groups.count() < len(logins):
        print 'Not enough groups with appropriate size!'
    else:
        db.groups.update({}, { '$set': { 'assigned_to': None } }, upsert = False, multi = True)
        for i in range(len(logins)):
            g = groups[i]

            # db.groups.save(g)
            db.groups.update({ '_id': g['_id'] }, { '$set': { 'assigned_to': logins[i].rstrip('\n') } }, upsert = False, multi = False)
            print 'Group # %s assigned to %s' % (g['_id'], logins[i])