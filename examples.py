# -*- coding: utf-8 -*-
"""
| **@created on:** 2019-04-17,
| **@author:** prathyushsp,
| **@version:** v0.0.1
|
| **Description:**
| 
|
| **Sphinx Documentation Status:** --
|
"""

from qlist import QLIST_MODULE_PATH
from qlist.querylist import QueryList
import json

data = json.load(open(QLIST_MODULE_PATH + '/sample_data.json'))
print(data)

# Query List Example
ql = QueryList(data=data)

# Find total number of items
len(ql)

"""
Mongo based Operations
"""

# List all names
list(ql.find({}, {"name": 1}))

# List all names, gender and age
list(ql.find({}, {"name": 1, "age": 1, "gender": 1}))

# List all name, where age is greater then 30
list(ql.find({"age": {"$gt": 35}}, {"name": 1}))

# Sum of all balances of age group > 30
list(ql.aggregate([{"$match": {"age": {"$gt": 30}}}, {"$group": {"_id": None, "age": {"$sum": "$balance"}}}]))

# Sum of all balances of age group > 30 and < 40
list(
    ql.aggregate([{"$match": {"age": {"$gt": 30, "$lt": 40}}}, {"$group": {"_id": None, "age": {"$sum": "$balance"}}}]))

# Sum of all balances
list(ql.aggregate([{"$group": {"_id": None, "age": {"$sum": "$balance"}}}]))

# Find the youngest person
age = list(ql.aggregate([{"$group": {"_id": None, "age": {"$min": "$age"}}}]))[0]['age']
list(ql.find({"age": {"$eq": age}}, {"name": 1, "age": 1}))

# Find distinct names
list(ql.distinct("name", {}))

"""
List Operations
"""

# List Operations
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))
# Delete
del ql[0]
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

# Delete on slices
del ql[0:2]
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

# Add data
ql += data[:4]
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

# Append an item
ql.append(data[1])
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

# Pop an item
ql.pop()
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

# Pop an index
ql.pop(3)
list(ql.find({}, {'name': 1, '__order__': 1, '_id': 0}))

"""
General Operations
"""
# Sync if changes done in MongoDB to in-mem
ql.sync()

# Purge all data in mongo
ql.purge()

# Rebuild Mongo cache
ql.build()
