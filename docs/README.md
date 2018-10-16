Python async api for creating and managing queues in postgres
 
[![Travis CI](https://travis-ci.org/maximdanilchenko/async-pq.svg?branch=master)](https://travis-ci.org/maximdanilchenko/async-pq)
[![PyPI version](https://badge.fury.io/py/async-pq.svg)](https://badge.fury.io/py/async-pq)
[![Documentation Status](https://readthedocs.org/projects/async-pq/badge/?version=latest)](https://async-pq.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/maximdanilchenko/async-pq/branch/master/graph/badge.svg)](https://codecov.io/gh/maximdanilchenko/async-pq)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Postgres is not best solution for storing and managing queues, 
but sometimes we have no choice.

```async-pq``` can work with millions of entities and thousands of 
requests in one queue. Is used in production. So its well tested and stable.

## Install
```
> pip install async-pq
```

## Quick start

To work with ```async-pq``` we need ```asyncpg``` library:
```python
import asyncpg

conn = await asyncpg.connect('postgresql://postgres@localhost/test')
```

```QueueFabric.find_queue``` method will create needed 
tables (one for requests and one for messages) in database if it is new queue. 
Also it has ```is_exists_queue``` method for situations when you 
need to know that it is exists or not and will be the new queue.
```python
from async_pq import Queue, QueueFabric

queue: Queue = await QueueFabric(conn).find_queue('items')
```
## Operations with queue
Put new items (should be dumped JSONs) in queue:
```python
await queue.put('{"id":1,"data":[1,2,3]}', '{"id":2,"data":[3,2,6]}')
```

Pop items from queue with some ```limit```. 
It will create one request and return its id. 
It is useful when you want to use acknowledgement pattern:
```python
# If with_ack=False (default from > 0.2.1), massage will be acknowledged in place automatically
request_id, data = await queue.pop(limit=2, with_ack=True)
```

To acknowledge request use ```ack``` method:
```python
# returns False if request does not found or acked already
is_acked: bool = await queue.ack(request_id)
```

Or vice versa:
```python
# returns False if request does not found or acked already
is_unacked: bool = await queue.unack(request_id)
```

You can return unacknowledged massages older than ```timeout``` seconds 
(default limit=1000 requests) to queue:
```python
requests_number = await queue.return_unacked(timeout=300, limit=500)
```

Clean queue (delete acknowledged massages) to not overfill database with old data 
(default limit= messages of 1000 requests):
```python
requests_number = await queue.clean_acked_queue(limit=500)
```
