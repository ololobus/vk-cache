#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urllib2
# import requests
import json
import random
import time

from pymongo import MongoClient



all_fields = ['sex', 'bdate', 'city', 'country', 'photo_50', 'photo_100', 'photo_200_orig', 'photo_200', 'photo_400_orig', 'photo_max', 'photo_max_orig', 'online', 'online_mobile', 'lists', 'domain', 'has_mobile', 'contacts', 'connections', 'site', 'education', 'universities', 'schools', 'can_post', 'can_see_all_posts', 'can_see_audio', 'can_write_private_message', 'status', 'last_seen', 'relation', 'relatives', 'counters']
queries = ['science', 'music', 'cinema', 'games', 'programming', 'news', 'it', 'институт', 'университет', 'кино', 'наука', 'новости', 'искусство', 'живопись', 'музыка', 'фото', 'картинки', 'музей', 'галерея']

specific_group_ids = ['26953']

method = 'groups'
method_type = 'stupid'
groups_type = 'db'

# Group size range
min_size = 90000
max_size = 120000

max_size_missmatches = 20
max_request_fails = 7

access_token = '706ea4dacd70052096470b005f9717a54df6368bc9345fab57aa81699b02a8bdbb14fbe664993d75f01be'

api_members_url = 'https://api.vk.com/method/groups.getMembers?v=5.29&%s'
api_groups_url = 'https://api.vk.com/method/groups.search?access_token=%s&v=5.29&%s'
api_friends_url = 'https://api.vk.com/method/friends.get?v=5.29&%s'

mongo = MongoClient()
db = mongo.vk

def request(url, ignore_errors = False):
    request_fails = 0
    data = '{}'
    result = None

    while True:
        time.sleep(0.334)

        try:
            # data = requests.get(url, timeout = 3).text
            data = urllib2.urlopen(url, timeout = 5).read()
        except:
            pass

        result = json.loads(data)

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

    count = None

    if data and 'count' in data:
        count = int(data['count'])
        if count <= max_size and count >= min_size:
            print 'Group', gid, 'with count', count, 'found'
            if not table.find_one({ '_id': gid }):
                table.save({ '_id': gid, 'count': count, 'fetched': False })
                print 'Saved'

    return count

def get_friends(uid):
    params = 'user_id=%s' % uid
    data = request(api_friends_url % params, True)

    friends = []

    if data and 'items' in data:
        friends = data['items']

    return friends

def wipe_groups_flags():
    for group in db.groups.find():
        db.groups.update({ '_id': group['_id'] }, { '$set': { 'fetched': False } }, upsert = False, multi = False)
    print 'Groups wiped'

# Get script params
if len(sys.argv) > 1:
    method = sys.argv[1]


# Groups search
if method == 'groups':
    table = db.groups

    if len(sys.argv) > 2:
        method_type = sys.argv[2]


    # Stupid random search
    if method_type == 'stupid':
        while True:
            gid = random.randint(0, 70000000)
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

                    if not count or int(count) < min_size:
                        size_missmatches += 1

                    if size_missmatches > max_size_missmatches:
                        break


# Users loading
if method == 'users':
    table = db.users

    if len(sys.argv) > 2:
        groups_type = sys.argv[2]

    if groups_type == 'db':
        groups = db.groups.find()

    if groups_type == 'pagerank':
        groups = map(lambda gid: { '_id': gid, 'fetched': False }, specific_group_ids)

    for group in groups:
        if not group['fetched']:
            print 'Fetching users from group: %s' % group['_id']

            params = 'group_id=%s&count=%s&fields=%s' % (group['_id'], '1000', ','.join(all_fields))
            offset = 0

            while True:
                data = request(api_members_url % (params + '&offset=' + str(offset)))

                if data and 'items' in data:
                    users = data['items']

                    if offset % 20000 == 0:
                        print 'offset:', offset

                    if len(users) == 0:
                        break
                    else:
                        for u in users:
                            # if not table.find_one({ 'id': u['id'], 'gid': group['_id'] }):
                            u['_id'] = '%s_%s' % (group['_id'], u['id'])
                            u['gid'] = group['_id']

                            if group['_id'] in specific_group_ids:
                                u['friends'] = get_friends(u['id'])

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

