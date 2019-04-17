# QueryList

#####1. What is QueryList?<br/>
QueryList is a Mongo Style Queryable Python List.

#####2. What are the uses of QueryList?<br/>
Imagine the trivial nature of python list manipulation combined with 
powerful query feature of mongo with persist support. Voila - QueryList

#####3. What are the features supported by QueryList?<br/>
- Order of Insertion 
- Append, Pop, Remove methods of List
- del, += operation in python
- find api of mongo support with find().pretty() [or use findp]
- aggregate api
- distinct api
- Support for data persist


#### Examples:

```python
from qlist import QLIST_MODULE_PATH
import json
data = json.loads(open(QLIST_MODULE_PATH))
print(data)

```
 




Json Data - Courtesy of JSON Generator
