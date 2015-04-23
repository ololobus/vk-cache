#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urllib2
import json
import random
import time
import io

from multiprocessing import Pool, Lock
from pymongo import MongoClient


# all_fields = ['music', 'movies', 'interests', 'activities', 'about', 'sex', 'bdate', 'city', 'country', 'photo_50', 'photo_100', 'photo_200_orig', 'photo_200', 'photo_400_orig', 'photo_max', 'photo_max_orig', 'online', 'online_mobile', 'lists', 'domain', 'has_mobile', 'contacts', 'connections', 'site', 'education', 'universities', 'schools', 'can_post', 'can_see_all_posts', 'can_see_audio', 'can_write_private_message', 'status', 'last_seen', 'relation', 'relatives', 'counters']
all_fields = 'verified,blacklisted,sex,bdate,city,country,home_town,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_max,photo_max_orig,online,lists,domain,has_mobile,contacts,site,education,universities,schools,status,last_seen,platform,followers_count,counters,occupation,nickname,relatives,relation,personal,connections,wall_comments,activities,interests,music,movies,tv,books,games,about,quotes,can_post,can_see_all_posts,can_see_audio,can_write_private_message,timezone,screen_name'

# 100k+
group_ids = ['19720218', '29937606']

# 500k+
# group_ids = ['26953']

method = 'groups'
method_type = 'smart'
groups_type = 'db'

max_size_missmatches = 20
max_request_fails = 3
pool_size = 20

api_users_url = 'https://api.vk.com/method/users.get?v=5.29&lang=en&fields=%s&' % all_fields

mongo = MongoClient()
db = mongo.vk

lock = Lock()

output_path = '%s_users.json'

def request(url, ignore_errors = False, skip_delay = False):
    request_fails = 0
    data = '{}'
    result = []

    while True:
        if not skip_delay:
            time.sleep(0.334)
        # print url
        # data = urllib2.urlopen(url, timeout = 15).read()

        try:
            # data = requests.get(url, timeout = 3).text
            data = urllib2.urlopen(url, timeout = 15).read()
            # print data
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

users = []
gid = group_ids[1]

uids = map(lambda u: u['id'], db.graph_users.find({ 'gid': gid }))
# uids = uids[0:5050]

sln = 0
slice_size = 300

while True:
    ids = uids[sln*slice_size:(sln + 1)*slice_size]

    if len(ids) == 0:
        break
    else:
        data = request(api_users_url + 'user_ids=' + ','.join(map(lambda uid: str(uid), ids)))
        users.extend(data)
        print sln, len(data)
        sln += 1

# with io.open(output_path % gid, 'w', encoding = 'utf8') as json_file:
#     content = json.dumps({ 'gid': gid, 'users': users }, ensure_ascii = False)
#     json_file.write(unicode(content))

file = open(output_path % gid, 'w')
file.write(json.dumps({ 'gid': gid, 'count': len(users), 'users': users }, ensure_ascii = False).encode('utf8'))
file.close()
