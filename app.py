#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url
# from tornado.escape import json_encode
import json
import sys

from pymongo import MongoClient

def main():

    class APIMembersHandler(RequestHandler):
        def get(self):
            error = { 'error_code': 100, 'error_msg': 'One of the parameters specified was missing or invalid' }
            sorts = { 'asc': 1, 'desc': -1 }

            gid = self.get_argument('group_id', default = None, strip = False)
            offset = self.get_argument('offset', default = 0, strip = False)
            count = self.get_argument('count', default = 1000, strip = False)
            sort = self.get_argument('sort', default = 'id_asc', strip = False)

            fields = self.get_argument('fields', default = None, strip = False)

            if not gid or count > 1000:
                self.write(json.dumps(error))
            else:
                gid = int(gid)
                offset = int(offset)
                count = int(count)

                mongo_sort = {}
                sort = sort.split('_')
                mongo_sort[sort[0]] = sorts[sort[1]]

                users = db.users.find({ 'gid': gid }).skip(offset).limit(count).sort(sort[0], sorts[sort[1]])

                if not fields:
                    users = map(lambda u: u['id'], users)
                else:
                    fields = fields.split(',')
                    fields.extend(['id', 'first_name', 'last_name'])
                    users = map(lambda u: { k: u[k] for k in u.keys() if k in fields }, users)

                result = { 'response': { 'count': db.users.find({ 'gid': gid }).count(), 'items': users } }

                self.write(json.dumps(result, ensure_ascii = False))

    mongo = MongoClient()
    db = mongo.vk

    app = Application([
        url(r'/method/groups.getMembers', APIMembersHandler),
    ]) 
    
    app.listen(sys.argv[1] if len(sys.argv) > 1 else 8888)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
