#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import re
from dateutil.relativedelta import relativedelta
from collections import Counter

from pymongo import MongoClient

method = 'calculate'

min_size = 1000
max_size = 1601

logins_path = '/opt/logins.txt'
# logins_path = '/Users/alexk/Downloads/logins'

mongo = MongoClient()
db = mongo.vk

reference_date = datetime.datetime.strptime('01.04.2015', '%d.%m.%Y')


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

                    if years <= 10:
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
                group_words.extend(words)

        word_counts = Counter(group_words)
        stats['top_interest'] = word_counts.most_common(1)[0][0]
        db.groups.update({ '_id': g['_id'] }, { '$set': { 'stats': stats } }, upsert = False, multi = False)

        print stats


