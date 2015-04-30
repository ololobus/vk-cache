#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import urllib2
# import requests
import json
import random
import time

from multiprocessing import Pool, Lock
from pymongo import MongoClient


all_fields = ['music', 'movies', 'interests', 'activities', 'about', 'sex', 'bdate', 'city', 'country', 'photo_50', 'photo_100', 'photo_200_orig', 'photo_200', 'photo_400_orig', 'photo_max', 'photo_max_orig', 'online', 'online_mobile', 'lists', 'domain', 'has_mobile', 'contacts', 'connections', 'site', 'education', 'universities', 'schools', 'can_post', 'can_see_all_posts', 'can_see_audio', 'can_write_private_message', 'status', 'last_seen', 'relation', 'relatives', 'counters']
queries = ['science', 'music', 'cinema', 'games', 'programming', 'news', 'it', 'институт', 'университет', 'кино', 'наука', 'новости', 'искусство', 'живопись', 'музыка', 'фото', 'картинки', 'музей', 'галерея']

# 100k+
specific_group_ids = ['19720218', '90021065', '29937606']

# 500k+
specific_group_ids = ['26953']

method = 'groups'
method_type = 'smart'
groups_type = 'db'

# Group size range
min_size = 90000
max_size = 130000

# min_size = 1001
# max_size = 1600

max_size_missmatches = 20
max_request_fails = 7
pool_size = 20

api_members_url = 'https://api.vk.com/method/groups.getMembers?access_token=%s&v=5.29&lang=en&%s'
api_members_url = 'https://api.vk.com/method/groups.getMembers?v=5.29&lang=en&%s'

api_groups_url = 'https://api.vk.com/method/groups.search?access_token=%s&v=5.29&lang=en&%s'
api_friends_url = 'https://api.vk.com/method/friends.get?&v=5.29&lang=en&%s'
api_followers_url = 'https://api.vk.com/method/users.getFollowers?&v=5.29&lang=en&%s'

mongo = MongoClient()
db = mongo.vk

lock = Lock()

access_token = db.secrets.find_one({ '_id': 'access_token' })['value']

def request(url, ignore_errors = False, skip_delay = False):
    request_fails = 0
    data = '{}'
    result = {}

    while True:
        if not skip_delay:
            time.sleep(0.334)

        try:
            # data = requests.get(url, timeout = 3).text
            data = urllib2.urlopen(url, timeout = 10).read()
            result = json.loads(data)
            # print result
        except:
            pass


        if 'response' in result:
            result = result['response']
            break
        else:
            if ignore_errors:
                break

            request_fails += 1
            print 'zero response: %s/%s' % (request_fails, max_request_fails)
            # print result

            if request_fails >= max_request_fails:
                print 'max request fails exceeded!'
                break

    return result

def get_group(gid):
    params = 'group_id=%s&count=%s' % (gid, 0)
    data = request(api_members_url % params)
    # data = request(api_members_url % (access_token, params))

    count = None

    if data and 'count' in data:
        count = int(data['count'])
        if count <= max_size and count >= min_size:
            print 'Group', gid, 'with count', count, 'found'
            if not table.find_one({ '_id': gid }):
                table.save({ '_id': gid, 'count': count, 'fetched': False })
                print 'Saved'

    return count

def get_friends(user):
    params = 'user_id=%s' % user['id']
    data = request(api_friends_url % params, True)

    friends = []

    if data and 'items' in data:
        friends = data['items']

    lock.acquire()
    db.user_friends.save({'_id': user['id'], 'friends': friends})
    lock.release()

    print user['id'], '--friends-->', len(friends)

def get_followers(user):
    offset = 0
    followers = []
    while True:
        params = 'user_id=%s&offset=%s&count=1000' % (user['id'], offset)
        data = request(api_followers_url % params, True, True)

        fs = []

        if data and 'items' in data:
            fs = data['items']

        if len(fs) == 0:
            break

        followers.extend(fs)

        offset += 1000

    lock.acquire()
    db.followers.save({'_id': user['id'], 'followers': followers})
    lock.release()

    print user['id'], '--flwrs-->', len(followers)

def wipe_groups_flags():
    for group in db.groups.find():
        db.groups.update({ '_id': group['_id'] }, { '$set': { 'fetched': False } }, upsert = False, multi = False)
    print 'Groups wiped'

# Get script params
if len(sys.argv) > 1:
    method = sys.argv[1]

if os.environ.get('NPL_ENV') == 'test':
    env = 'test'

    db = mongo.vk_test
    min_size = 20000
    max_size = 40000

    specific_group_ids = ['16880142']
    all_fields = ['interests', 'sex', 'bdate']



# Groups search
if method == 'groups':
    table = db.bgroups

    if len(sys.argv) > 2:
        method_type = sys.argv[2]


    # Stupid random search or load from ids list
    if method_type == 'stupid':
        # while True:
        #     gid = random.randint(0, 70000000)
        #     get_group(gid)
        for gid in specific_group_ids:
            get_group(gid)


    # Smart search by popular keywords
    if method_type == 'smart':
        for q in queries:
            params = 'q=%s&count=1000' % q
            data = request(api_groups_url % (access_token, params))

            if data and 'items' in data:
                groups = data['items']

                size_missmatches = 0
                for g in groups:
                    count = get_group(g['id'])
                    print count
                    if not count or int(count) < min_size:
                        size_missmatches += 1
                    else:
                        size_missmatches = 0

                    if size_missmatches >= max_size_missmatches:
                        break


# Users loading
if method == 'users':
    if len(sys.argv) > 2:
        groups_type = sys.argv[2]

    if groups_type == 'db':
        groups = db.groups.find()

    if groups_type == 'graph':
        groups = map(lambda gid: { '_id': gid, 'fetched': False }, specific_group_ids)
        table = db.graph_users
    else:
        table = db.users

    for group in groups:
        if not group['fetched']:
            print 'Fetching users from group: %s' % group['_id']

            params = 'group_id=%s&count=%s' % (group['_id'], '1000')

            if groups_type != 'graph' or env == 'test':
                params += '&fields=%s' % ','.join(all_fields)

            offset = 0

            while True:
                # data = request(api_members_url % (access_token, params + '&offset=' + str(offset)))
                data = request(api_members_url % (params + '&offset=' + str(offset)))

                if data and 'items' in data:
                    users = data['items']

                    if offset % 20000 == 0:
                        print 'offset:', offset

                    if len(users) == 0:
                        break
                    else:
                        for u in users:
                            if groups_type != 'graph' or env == 'test':
                                # if not table.find_one({ 'id': u['id'], 'gid': group['_id'] }):
                                u['_id'] = '%s_%s' % (group['_id'], u['id'])
                                u['gid'] = group['_id']
                            else:
                                u = { '_id': '%s_%s' % (group['_id'], u), 'gid': group['_id'], 'id': u }

                            table.save(u)
                        offset += 1000
                else:
                    break

            if group['_id'] not in specific_group_ids:
                db.groups.update({ '_id': group['_id'] }, { '$set': { 'fetched': True } }, upsert = False, multi = False)

            print 'Users from group', group['_id'], 'fetched'
        else:
            print 'Users from group', group['_id'], 'already fetched!'


# Wipe groups 'fetched' flag
if method == 'wipe':
    wipe_groups_flags()


# Load users friends
if method == 'friends':
    gid = specific_group_ids[0]

    users = db.graph_users.find({ 'gid': gid })#.limit(3)
    pool = Pool(pool_size)
    pool.map(get_friends, users)

    # groups = db.bgroups.find()
    #
    # for g in groups:
    #     users = db.graph_users.find({ 'gid': g['_id'] })
    #     pool = Pool(pool_size)
    #     pool.map(get_friends, users)


# Load users followers
if method == 'followers':
    gid = specific_group_ids[0]

    users = db.graph_users.find({ 'gid': gid })#.limit(3)
    pool = Pool(pool_size)
    pool.map(get_followers, users)
