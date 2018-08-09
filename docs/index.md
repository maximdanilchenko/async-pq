[![Travis CI](https://travis-ci.org/maximdanilchenko/async-pq.svg?branch=master)](https://travis-ci.org/maximdanilchenko/async-pq)
[![PyPI version](https://badge.fury.io/py/async-pq.svg)](https://badge.fury.io/py/async-pq)
[![Documentation Status](https://readthedocs.org/projects/async-pq/badge/?version=latest)](https://async-pq.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/maximdanilchenko/async-pq/branch/master/graph/badge.svg)](https://codecov.io/gh/maximdanilchenko/async-pq)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
# Install
```
> pip install async-pq
```

# Usage

To work with queue we need ```asyncpg``` library
```python
import asyncpg

conn = await asyncpg.connect('postgresql://postgres@localhost/test')
```

```QueueFabric.find_queue``` method will create needed 
tables in database if it is new queue. 
Also it has ```is_exists_queue``` method for situations when you 
need to know that it will be the new queue
```python
from async_pq import Queue, QueueFabric

queue: Queue = await QueueFabric(conn).find_queue('items')
```

Put new items (dumped JSONs) in queue
```python
await queue.put('{"id":1,"data":[1,2,3]}', '{"id":2,"data":[3,2,6]}')
```

Pop items from queue with some ```limit```. It is possible to use acknowledge pattern
```python
# If with_ack=False, massage will be acknowledged in place automatically
request_id, data = await queue.pop(limit=2, with_ack=True)
```

Acknowledge request
```python
# returns False if request does not found or acked already
is_acked: bool = await queue.ack(request_id)
```

Or vice versa 
```python
# returns False if request does not found or acked already
is_unacked: bool = await queue.unack(request_id)
```

Return to queue all unacknowledged massages older than ```timeout``` seconds 
```python
await queue.return_unacked(timeout=300)
```

Clean queue (delete all acknowledged massages)
```python
await queue.clean_acked_queue()
```
