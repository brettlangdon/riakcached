Riakcached documentation
========================

Contents:

.. toctree::
   :maxdepth: 2

   clients
   exceptions
   pools

|Build Status| |Coverage Status| |PyPI version|

A Memcached like interface to the Riak HTTP Client. `Read The
Docs <http://riakcached.readthedocs.org/en/latest/>`__

Installing
----------

From PyPI
~~~~~~~~~

.. code:: bash

    pip install riakcached

From Git
~~~~~~~~

.. code:: bash

    git clone git://github.com/brettlangdon/riakcached.git
    cd ./riakcached
    pip install -r requirements.txt
    python setup.py install

Usage
-----

Basic Usage
~~~~~~~~~~~

.. code:: python

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

Connection Pool Settings
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    from riakcached.clients import RiakClient
    from riakcached.pools import Urllib3Pool

    pool = Urllib3Pool(base_url="http://my-host.com:8098/", timeout=1)
    client = RiakClient("my_bucket", pool=pool)

    client.get("foo")

Custom Connection Pool
~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

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

Threaded Client
~~~~~~~~~~~~~~~

The exists a :class:`riakcached.clients.ThreadedRiakClient` which inherits
from :class:`riakcached.clients.RiakClient` and which uses threading to try
to parallelize calls to ``get_many``, ``set_many`` and ``delete_many``.

Documentation
-------------

The documentation can be found in the ``/docs`` directory in this
repository and should be fairly complete for the codebase.

Building Documentation
~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    git clone git://github.com/brettlangdon/riakcached.git
    cd riakcached
    pip install -r docs-requirements.txt
    cd ./docs
    make html


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |Build Status| image:: https://travis-ci.org/brettlangdon/riakcached.png?branch=master
   :target: https://travis-ci.org/brettlangdon/riakcached
.. |Coverage Status| image:: https://coveralls.io/repos/brettlangdon/riakcached/badge.png?branch=master
   :target: https://coveralls.io/r/brettlangdon/riakcached?branch=master
.. |PyPI version| image:: https://badge.fury.io/py/riakcached.png
   :target: http://badge.fury.io/py/riakcached
