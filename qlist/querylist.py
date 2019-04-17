# -*- coding: utf-8 -*-
"""
| **@created on:** 2019-04-14,
| **@author:** prathyushsp,
| **@version:** v0.0.1
|
| **Description:**
| 
|
| **Sphinx Documentation Status:** --
|
"""

import pymongo
import typing
import json
import uuid
from bson import ObjectId
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = pymongo.MongoClient(host='localhost', port=27017)
client.drop_database('qdict')
client.close()


class JsonEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):

        if isinstance(obj, ObjectId):
            return obj.__str__()
        else:
            try:
                return obj.default()
            except Exception:
                print(f'{obj} is not serializable. ')
                return f'Object not serializable - {obj}'


from collections import UserList


class QueryList(UserList):

    def __init__(self, host: str = 'localhost', port: int = 27017, data: typing.List[typing.Dict] = [],
                 database: str = 'qdict', id: str = None, persist=True):
        self.config_keys = ['_id', '__hash__', '__order__']
        self.id = id or uuid.uuid4().__str__()
        self.host = host
        self.port = port
        self.client = pymongo.MongoClient(host=host, port=port)
        self.db_name = database
        self.db = self.client.get_database(name=database)
        self.collection = self.db.get_collection(name=self.id)
        if self.id in self.db.list_collection_names():
            print(f'Restoring {self.id} collection . . .')
            self.data = list(self.collection.find({}, {i: 0 for i in self.config_keys}))
            self.hashes = self.__hashes__()
        else:
            self.data = data
            self.hashes = self.__hashes__()
            if self.data:
                self.collection.insert(
                    [{**item, **{'__hash__': self.hashes[e], '__order__': e}} for e, item in enumerate(self.data)])
        self.persist = persist
        super().__init__(initlist=self.data)

    def build(self):
        self.purge()
        self.hashes = self.__hashes__()
        self.collection = self.db.get_collection(self.id)
        self.collection.insert(
            [{**item, **{'__hash__': self.hashes[e], '__order__': e}} for e, item in enumerate(self.data)])

    def distinct(self, *args, **kwargs):
        return self.collection.distinct(*args, **kwargs)

    def __get_hash__(self):
        return uuid.uuid4().__str__()

    def __hashes__(self):
        return [uuid.uuid4().__str__() for _ in self.data]

    def find(self, *args, **kwargs):
        self._recent_find = self.collection.find(*args, **kwargs)
        self._recent_find.pretty = lambda: json.dumps(list(self._recent_find), indent=2, cls=JsonEncoder)
        return self._recent_find

    def aggregate(self, *args, **kwargs):
        return self.collection.aggregate(*args, **kwargs)

    def findp(self, *args, **kwargs):
        return json.dumps(list(self.collection.find(*args, **kwargs)), indent=2, cls=JsonEncoder)

    def purge(self):
        self.db.drop_collection(self.id)

    def sync(self):
        logger.info("Syncing data from mongo to in-mem object")
        self.data = list(self.collection.find({}, {i: 0 for i in self.config_keys}).sort('__order__'))

    def __delitem__(self, index):
        super().__delitem__(i=index)
        self.hashes.__delitem__(index)
        self.__update_db__()

    def __iadd__(self, other):
        if not isinstance(other, list):
            other = [other]
        _d = []
        self.hashes += [self.__get_hash__() for _ in other]
        self.collection.insert_many(
            [{**item, **{'__hash__': self.hashes[e + len(self.data)], '__order__': e + len(self.data)}} for e, item in
             enumerate(other)])
        super().__iadd__(other)

        return self

    def __update_order__(self):
        # Update order
        for e, h in enumerate(self.hashes):
            self.collection.update_one({'__hash__': h}, {'$set': {'__order__': e}})

    def __update_db__(self):
        to_be_removed_ids = list(self.collection.find({}, {'__hash__': 1}))
        to_be_removed_ids = [d['_id'] for d in to_be_removed_ids if d['__hash__'] not in self.hashes]

        # Remove Items
        for id in to_be_removed_ids:
            self.collection.delete_one({'_id': id})
        self.__update_order__()

    def pop(self, i: int = -1):
        if i == -1:
            self.collection.delete_one({'__order__': len(self.data) - 1})
            self.hashes.pop()
        else:
            self.hashes.pop(i)
        super().pop(i)
        self.__update_db__()

    def __delete__(self, instance):
        print('delete called')

    def append(self, item):
        super().append(item)
        self.hashes.append(self.__get_hash__())
        self.collection.insert_one({**item, '__hash__': self.hashes[-1], '__order__': len(self.data) - 1})

    def remove(self, item):
        _order = self.index(item)
        del self.hashes[_order]
        self.collection.delete_one({'__order__': _order})
        self.__update_order__()
        super().remove(item)

    def __del__(self, *args, **kwargs):
        try:
            if not self.persist:
                self.collection.drop()
                self.client.close()
        except Exception as e:
            print('exception: ', e)
            pass

# data = [
#     {
#         "text": "HDFC Bank Q3 net profit increases 20.3% to ₹5,586 crore",
#         "result": {
#             "documentSentiment": {
#                 "magnitude": 0.3,
#                 "score": -0.3
#             },
#             "language": "en",
#             "sentences": [
#                 {
#                     "sentiment": {
#                         "magnitude": 0.3,
#                         "score": -0.3
#                     },
#                     "text": {
#                         "beginOffset": 0,
#                         "content": "HDFC Bank Q3 net profit increases 20.3% to \u20b95,586 crore"
#                     }
#                 }
#             ]
#         }
#
#     },
#     {
#         "text": "HDFC Bank Q3 net profit decreases 20.3% to ₹5,586 crore",
#         "result": {
#             "documentSentiment": {
#                 "magnitude": 0.6,
#                 "score": -0.6
#             },
#             "language": "en",
#             "sentences": [
#                 {
#                     "sentiment": {
#                         "magnitude": 0.6,
#                         "score": -0.6
#                     },
#                     "text": {
#                         "beginOffset": 0,
#                         "content": "HDFC Bank Q3 net profit decreases 20.3% to \u20b95,586 crore"
#                     }
#                 }
#             ]
#         }
#     },
#     {"text": "HDFC Bank Q3 net loss increases 20.3% to ₹5,586 crore",
#      "result": {
#          "documentSentiment": {
#              "magnitude": 0.5,
#              "score": -0.5
#          },
#          "language": "en",
#          "sentences": [
#              {
#                  "sentiment": {
#                      "magnitude": 0.5,
#                      "score": -0.5
#                  },
#                  "text": {
#                      "beginOffset": 0,
#                      "content": "HDFC Bank Q3 net loss increases 20.3% to \u20b95,586 crore"
#                  }
#              }
#          ]
#      }},
#     {
#         "text": "HDFC Bank Q3 net loss decreases 20.3% to ₹5,586 crore",
#         "result": {
#             "documentSentiment": {
#                 "magnitude": 0.6,
#                 "score": -0.6
#             },
#             "language": "en",
#             "sentences": [
#                 {
#                     "sentiment": {
#                         "magnitude": 0.6,
#                         "score": -0.6
#                     },
#                     "text": {
#                         "beginOffset": 0,
#                         "content": "HDFC Bank Q3 net loss decreases 20.3% to \u20b95,586 crore"
#                     }
#                 }
#             ]
#         }
#     }]

# Simple Basic Initialization
# ql = QueryList()
# ql.append(data[0])
# ql += data[1:3]
# print(data[0])
# qd = QueryList(id='tttx', persist=True)
# Ops
# qd += qd[0:2]
# del qd[-1]
# qd.pop(1)
# qd.append(qd[0])
# print(qd.index(qd[0]))
# qd.remove(qd[0])
# print(len(qd))
# print(len(qd))
# qd.sync()
# print(len(qd))
# print(qd)

# del qd[0]
# print(qd)
# print(qd)
# print(qd[0])
# print(qd.findp())
#
# print(qd.find({}, {'text': 1}).pretty())

# qd.purge()
# del qd

# print('done')
# qd.find({},{'text':1})
