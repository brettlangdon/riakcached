__all__ = ["RiakClient", "ThreadedRiakClient"]

import json
import Queue
import threading

from riakcached import exceptions
from riakcached.pools import Urllib3Pool


class RiakClient(object):
    """A Memcache like client to the Riak HTTP Interface
    """
    __slots__ = [
        "_serializers",
        "_deserializers",
        "base_url",
        "bucket",
        "pool",
    ]

    def __init__(self, bucket, pool=None):
        """Constructor for a new :class:`riakcached.clients.RiakClient`

        Pool - if no pool is provided then a default :class:`riakcached.pools.Urllib3Pool` is used

        :param bucket: The name of the Riak bucket to use
        :type bucket: str
        :param pool: The :class:`riakcached.pools.Pool` to use for requests
        :type pool: :class:`riakcached.pools.Pool`
        """
        if pool is None:
            self.pool = Urllib3Pool()
        else:
            self.pool = pool

        self.bucket = bucket
        self.base_url = self.pool.url.rstrip("/")
        self._serializers = {
            "application/json": json.dumps,
        }
        self._deserializers = {
            "application/json": json.loads,
        }

    def add_serializer(self, content_type, serializer):
        """Add a content-type serializer to the client

        The `serializer` function should have the following definition::

            def serializer(data):
                return do_something(data)

        and should return a `str`

        Example::

            def base64_serializer(data):
                return base64.b64encode(data)
            client.add_serializer("application/base64", base64_serializer)


        :param content_type: the content-type to associate `serializer` with
        :type content_type: str
        :param serializer: the serializer function to use with `content_type`
        :type serializer: function
        """
        content_type = content_type.lower()
        self._serializers[content_type] = serializer

    def add_deserializer(self, content_type, deserializer):
        """Add a content-type deserializer to the client

        The `deserializer` function should have the following definition::

            def deserializer(data):
                return undo_something(data)

        Example::

            def base64_deserializer(data):
                return base64.b64decode(data)
            client.add_deserializer("application/base64", base64_deserializer)


        :param content_type: the content-type to associate `deserializer` with
        :type content_type: str
        :param deserializer: the deserializer function to use with `content_type`
        :type deserializer: function
        """
        content_type = content_type.lower()
        self._deserializers[content_type] = deserializer

    def serialize(self, data, content_type):
        """Serialize the provided `data` to `content_type`

        This method will lookup the registered serializer for the provided Content-Type
        (defaults to str(data)) and passes `data` through the serializer.

        :param data: the data to serialize
        :type data: object
        :param content_type: the desired Content-Type for the provided `data`
        :type content_type: str
        :returns: str - the serialized data
        """
        serializer = self._serializers.get(content_type, str)
        return serializer(data)

    def deserialize(self, data, content_type):
        """Deserialize the provided `data` from `content_type`

        This method will lookup the registered deserializer for the provided Content-Type
        (defaults to str(data)) and passes `data` through the deserializer.

        :param data: the data to deserialize
        :type data: str
        :param content_type: the Content-Type to deserialize `data` from
        :type content_type: str
        :returns: object - whatever the deserializer returns
        """
        deserializer = self._deserializers.get(content_type, str)
        return deserializer(data)

    def get(self, key, counter=False):
        """Get the value of the key from the client's `bucket`

        :param key: the key to get from the bucket
        :type key: str
        :param counter: whether or not the `key` is a counter
        :type counter: bool
        :returns: object - the deserialized value of `key`
        :returns: None - if the call was not successful or the key was not found
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedServiceUnavailable`
        """
        url = "%s/buckets/%s/keys/%s" % (self.base_url, self.bucket, key)
        if counter:
            url = "%s/buckets/%s/counters/%s" % (self.base_url, self.bucket, key)
        status, data, headers = self.pool.request(method="GET", url=url)
        if status == 400:
            raise exceptions.RiakcachedBadRequest(data)
        elif status == 503:
            raise exceptions.RiakcachedServiceUnavailable(data)

        if status not in (200, 300, 304):
            return None

        return self.deserialize(data, headers.get("content-type", "text/plain"))

    def get_many(self, keys):
        """Get the value of multiple keys at once from the client's `bucket`

        :param keys: the list of keys to get
        :type keys: list
        :returns: dict - the keys are the keys provided and the values are the results from calls
            to :func:`get`, except keys whose values are `None` are not included in the result
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedServiceUnavailable`
        """
        results = dict((key, self.get(key)) for key in keys)
        return dict((key, value) for key, value in results.iteritems() if value is not None)

    def set(self, key, value, content_type="text/plain"):
        """Set the value of a key for the client's `bucket`

        :param key: the key to set the value for
        :type key: str
        :param value: the value to set, this will get serialized for the `content_type`
        :type value: object
        :param content_type: the Content-Type for `value`
        :type content_type: str
        :returns: bool - True if the call is successful, False otherwise
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedPreconditionFailed`
        """
        value = self.serialize(value, content_type)

        status, data, _ = self.pool.request(
            method="POST",
            url="%s/buckets/%s/keys/%s" % (self.base_url, self.bucket, key),
            body=value,
            headers={
                "Content-Type": content_type,
            },
        )
        if status == 400:
            raise exceptions.RiakcachedBadRequest(data)
        elif status == 412:
            raise exceptions.RiakcachedPreconditionFailed(data)
        return status in (200, 201, 204, 300)

    def set_many(self, values, content_type="text/plain"):
        """Set the value of multiple keys at once for the client's `bucket`

        :param values: the key -> value pairings for the keys to set
        :type values: dict
        :param content_type: the Content-Type for all of the values provided
        :type content_type: str
        :returns: dict - the keys are the keys provided and the values are True or False from
            the calls to :func:`set`
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedPreconditionFailed`
        """
        return dict(
            (key, self.set(key, value, content_type))
            for key, value in values.iteritems()
        )

    def delete(self, key):
        """Delete the provided key from the client's `bucket`

        :param key: the key to delete
        :type key: str
        :returns: bool - True if the key was removed, False otherwise
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        """
        status, data, _ = self.pool.request(
            method="DELETE",
            url="%s/buckets/%s/keys/%s" % (self.base_url, self.bucket, key),
        )
        if status == 400:
            raise exceptions.RiakcachedBadRequest(data)
        return status in (204, 404)

    def delete_many(self, keys):
        """Delete multiple keys at once from the client's `bucket`

        :param keys: list of `str` keys to delete
        :type keys: list
        :returns: dict - the keys are the keys provided and the values are True or False from
            the calls to :func:`delete`
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        """
        return dict((key, self.delete(key)) for key in keys)

    def stats(self):
        """Get the server stats

        :returns: dict - the stats from the server
        :returns: None - when the call is not successful
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/stats" % self.base_url,
        )
        if status == 200:
            return self.deserialize(data, "application/json")
        return None

    def props(self):
        """Get the properties for the client's `bucket`

        :returns: dict - the `bucket`'s set properties
        :returns: None - when the call is not successful
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/buckets/%s/props" % (self.base_url, self.bucket),
        )
        if status == 200:
            return json.loads(data)
        return None

    def set_props(self, props):
        """Set the properties for the client's `bucket`

        :param props: the properties to set
        :type props: dict
        :returns: bool - True if it is successful otherwise False
        """
        status, _, _ = self.pool.request(
            method="PUT",
            url="%s/buckets/%s/props" % (self.base_url, self.bucket),
            body=self.serialize(props, "application/json"),
            headers={
                "Content-Type": "application/json",
            }
        )
        return status == 200

    def keys(self):
        """Get a list of all keys

        :returns: list - list of keys on the server
        :returns: None - when the call is not successful
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/buckets/%s/keys?keys=true" % (self.base_url, self.bucket),
        )
        if status == 200:
            return self.deserialize(data, "application/json")
        return None

    def ping(self):
        """Ping the server to ensure it is up

        :returns: bool - True if it is successful, False otherwise
        """
        status, _, _ = self.pool.request(
            method="GET",
            url="%s/ping" % self.base_url,
        )
        return status == 200

    def incr(self, key, value=1):
        """Increment the counter with the provided key

        :param key: the counter to increment
        :type key: str
        :param value: how much to increment by
        :type value: int
        :returns: bool - True/False whether or not it was successful
        :raises: :class:`riakcached.exceptions.RiakcachedConflict`
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        """
        status, data, _ = self.pool.request(
            method="POST",
            url="%s/buckets/%s/counters/%s" % (self.base_url, self.bucket, key),
            body=str(value),
        )
        if status == 409:
            raise exceptions.RiakcachedConflict(data)
        elif status == 400:
            raise exceptions.RiakcachedBadRequest(data)
        return status in (200, 201, 204, 300)


class ThreadedRiakClient(RiakClient):
    """A threaded version of :class:`riakcached.clients.RiakClient`

    The threaded version uses threads to try to parallelize the {set,get,delete}_many method calls
    """
    def _many(self, target, args_list):
        workers = []
        worker_results = Queue.Queue()
        for args in args_list:
            args.append(worker_results)
            worker = threading.Thread(target=target, args=args)
            worker.daemon = True
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()

        results = {}
        while not worker_results.empty():
            key, value = worker_results.get()
            results[key] = value
        return results

    def delete_many(self, keys):
        """Delete multiple keys at once from the client's `bucket`

        :param keys: list of `str` keys to delete
        :type keys: list
        :returns: dict - the keys are the keys provided and the values are True or False from
            the calls to :func:`delete`
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        """
        def worker(key, results):
            results.put((key, self.delete(key)))

        args = [[key] for key in keys]

        return self._many(worker, args)

    def set_many(self, values):
        """Set the value of multiple keys at once for the client's `bucket`

        :param values: the key -> value pairings for the keys to set
        :type values: dict
        :param content_type: the Content-Type for all of the values provided
        :type content_type: str
        :returns: dict - the keys are the keys provided and the values are True or False from
            the calls to :func:`set`
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedPreconditionFailed`
        """
        def worker(key, value, results):
            results.put((key, self.set(key, value)))

        args = [list(data) for data in values.items()]
        return self._many(worker, args)

    def get_many(self, keys):
        """Get the value of multiple keys at once from the client's `bucket`

        :param keys: the list of keys to get
        :type keys: list
        :returns: dict - the keys are the keys provided and the values are the results from calls
            to :func:`get`, except keys whose values are `None` are not included in the result
        :raises: :class:`riakcached.exceptions.RiakcachedBadRequest`
        :raises: :class:`riakcached.exceptions.RiakcachedServiceUnavailable`
        """
        def worker(key, results):
            results.put((key, self.get(key)))

        args = [[key] for key in keys]
        results = self._many(worker, args)
        results = dict((key, value) for key, value in results.iteritems() if value is not None)
        return results or None
