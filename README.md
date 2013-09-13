Riakcached
==========

[![Build Status](https://travis-ci.org/brettlangdon/riakcached.png?branch=master)](https://travis-ci.org/brettlangdon/riakcached)
[![Coverage Status](https://coveralls.io/repos/brettlangdon/riakcached/badge.png?branch=master)](https://coveralls.io/r/brettlangdon/riakcached?branch=master)
[![PyPI version](https://badge.fury.io/py/riakcached.png)](http://badge.fury.io/py/riakcached)
[![Downloads](https://pypip.in/d/riakcached/badge.png)](https://github.com/brettlangdon/riakcached)

A Memcached like interface to the Riak HTTP Client. [Read The Docs](http://riakcached.readthedocs.org/en/latest/)

## Installing
### From PyPI
```bash
pip install riakcached
```

### From Git
```bash
git clone git://github.com/brettlangdon/riakcached.git
cd ./riakcached
pip install -r requirements.txt
python setup.py install
```

## Usage
### Basic Usage
```python
from riakcached.clients import RiakClient

client = RiakClient("my_bucket")

client.set("hello", "world")
print client.get("hello")
# 'hello'

client.delete("hello")
print client.get("hello")
# None

values = {
    "hello": "world",
    "foo": "bar",
}
client.set_many(values)

keys = ["hello", "foo", "test"]
print client.get_many(keys)
# {'foo': 'bar', 'hello': 'world'}

client.close()
```

### Connection Pool Settings
```bash
from riakcached.clients import RiakClient
from riakcached.pools import Urllib3Pool

pool = Urllib3Pool(base_url="http://my-host.com:8098/", timeout=1)
client = RiakClient("my_bucket", pool=pool)

client.get("foo")
```

### Custom Connection Pool
```bash
from riakcached.clients import RiakClient
from riakcache.pools import Pool

class CustomPool(Pool):
    __slots__ = ["connection"]

    def connect(self):
        self.connection = make_a_connection()

    def close(self):
        if self.connection:
            close_connection(self.connection)

    def request(self, method, url, body=None, headers=None):
        results = make_request(self.connection, method, url, body, headers, timeout=self.timeout)
        return results.status, results.data, results.headers


custom_pool = CustomPool(base_url="http://my-host.com:8098", timeout=1)
client = RiakClient("my_bucket", pool=pool)
```

### Threaded Client
The exists a `riakcached.clients.ThreadedRiakClient` which inherits from `riakcached.clients.RiakClient` and which uses threading to
try to parallelize calls to `get_many`, `set_many` and `delete_many`.

## Documentation
The documentation can be found in the `/docs` directory in this repository and should be fairly complete for the codebase.

### Building Documentation
```bash
git clone git://github.com/brettlangdon/riakcached.git
cd riakcached
pip install -r docs-requirements.txt
cd ./docs
make html
```
