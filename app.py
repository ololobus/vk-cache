#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url
# from tornado.escape import json_encode
import json
import sys
import urllib2
import hashlib

from pymongo import MongoClient

def main():

    check_token = '704a29cff98392968dd44a5a093de3ecda650ea480ca3ceada21444228a245152fb5cbe3a4968f87a104172d21de8fb80fb7619c23b866c88123027b7f3b0fc7'

    host = 'http://ec2-52-17-77-210.eu-west-1.compute.amazonaws.com'
    # host = 'http://localhost:8888'
    oauth_url = 'https://oauth.vk.com/authorize?client_id=4859033&redirect_uri=%s/oauth/success&response_type=code&v=5.29&scope=offline' % host
    token_url = 'https://oauth.vk.com/access_token?client_id=4859033&client_secret=wjmgKa3oUlfWzyoVwSoN&code=%s&redirect_uri=%s/oauth/success'


    class OAuthHandler(RequestHandler):
        def get(self):
            self.redirect(oauth_url)


    class OAuthSuccessHandler(RequestHandler):
        def get(self):
            code = self.get_argument('code', default = None, strip = False)

            if code:
                data = '{}'
                access_token = None

                print token_url % (code, host)

                try:
                    data = urllib2.urlopen(token_url % (code, host), timeout = 5).read()
                except:
                    pass

                data = json.loads(data)
                print data

                if 'access_token' in data:
                    access_token = data['access_token']

                if access_token:
                    db.secrets.save({ '_id': 'access_token', 'value': access_token })
                    self.write('Successful')
                else:
                    self.write('Failed')
            else:
                self.write('Failed')


    class APIMembersHandler(RequestHandler):
        def get(self):
            error = { 'error_code': 100, 'error_msg': 'One of the parameters specified was missing or invalid' }
            sorts = { 'asc': 1, 'desc': -1 }

            gid = self.get_argument('group_id', default = None, strip = False)
            offset = self.get_argument('offset', default = 0, strip = False)
            count = self.get_argument('count', default = 1000, strip = False)
            sort = self.get_argument('sort', default = 'id_asc', strip = False)
            scope = self.get_argument('scope', default = None, strip = False)

            fields = self.get_argument('fields', default = None, strip = False)

            gid = int(gid)
            offset = int(offset)
            count = int(count)

            if not gid or count > 1000:
                self.write(json.dumps(error))
            else:
                mongo_sort = {}
                sort = sort.split('_')
                mongo_sort[sort[0]] = sorts[sort[1]]

                if scope == 'super':
                    table = db.graph_users
                    gid = str(gid)
                else:
                    table = db.users

                users = table.find({ 'gid': gid }).skip(offset).limit(count).sort(sort[0], sorts[sort[1]])

                if not fields:
                    users = map(lambda u: u['id'], users)
                else:
                    fields = fields.split(',')
                    fields.extend(['id', 'first_name', 'last_name'])
                    users = map(lambda u: { k: u[k] for k in u.keys() if k in fields }, users)

                result = { 'response': { 'count': table.find({ 'gid': gid }).count(), 'items': users } }

                self.write(json.dumps(result, ensure_ascii = False))

    class APIFriendsHandler(RequestHandler):
        def get(self):
            error = { 'error_code': 100, 'error_msg': 'One of the parameters specified was missing or invalid' }

            uid = self.get_argument('user_id', default = None, strip = False)
            offset = self.get_argument('offset', default = 0, strip = False)
            count = self.get_argument('count', default = None, strip = False)

            if not uid:
                self.write(json.dumps(error))
            else:
                friends = db.user_friends.find_one({ '_id': int(uid) })

                if not friends:
                    friends = []
                else:
                    friends = friends['friends']

                result = { 'response': { 'count': len(friends), 'items': friends } }

                self.write(json.dumps(result, ensure_ascii = False))

    class AssignmentsHandler(RequestHandler):
        def get(self):
            token = self.get_argument('token', default = 'no auth', strip = False)
            login = self.get_argument('login', default = None, strip = False)
            stats = self.get_argument('stats', default = False, strip = False)

            if hashlib.sha512(token).hexdigest() != check_token:
                self.write('You are not authorized to use this method!')
            elif login:
                group = db.groups.find_one({ 'assigned_to': login })

                if group:
                    if stats:
                        if 'stats' in group:
                            self.write(json.dumps(group['stats'], ensure_ascii = False))
                        else:
                            self.write('Stats not ready')
                    else:
                        self.write(str(group['_id']))
                else:
                    self.write('This user is not assigned to any group')
            else:
                self.write('Please pass the user login')

    class UserLabsHandler(RequestHandler):
        def get(self):
            token = self.get_argument('token', default = 'no auth', strip = False)
            login = self.get_argument('login', default = None, strip = False)

            if hashlib.sha512(token).hexdigest() != check_token:
                self.write('You are not authorized to use this method!')
            elif login:
                user = mongo.npl.students.find_one({ '_id': login })

                if user:
                    self.write(json.dumps(user, ensure_ascii = False, sort_keys = True))
                else:
                    self.write('User not found')
            else:
                self.write('Please pass the user login')

    mongo = MongoClient()
    db = mongo.vk

    app = Application([
        url(r'/method/groups.getMembers', APIMembersHandler),
        url(r'/method/friends.get', APIFriendsHandler),
        url(r'/oauth/authorize', OAuthHandler),
        url(r'/oauth/success', OAuthSuccessHandler),
        url(r'/assignments/get', AssignmentsHandler),
        url(r'/user-labs/get', UserLabsHandler)
    ])

    app.listen(sys.argv[1] if len(sys.argv) > 1 else 8888)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
