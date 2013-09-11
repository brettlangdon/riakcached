__all__ = ["RiakClient", "ThreadedRiakClient"]

import json
import Queue
import threading

from riakcached import exceptions
from riakcached.pools import Urllib3Pool


class RiakClient(object):
    """
    """
    __slots__ = [
        "_serializers",
        "_deserializers",
        "base_url",
        "bucket",
        "pool",
    ]

    def __init__(self, bucket, pool=None):
        """
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
        """
        """
        content_type = content_type.lower()
        self._serializers[content_type] = serializer

    def add_deserializer(self, content_type, deserializer):
        """
        """
        content_type = content_type.lower()
        self._deserializers[content_type] = deserializer

    def serialize(self, data, content_type):
        """
        """
        serializer = self._serializers.get(content_type, str)
        return serializer(data)

    def deserialize(self, data, content_type):
        """
        """
        deserializer = self._deserializers.get(content_type, str)
        return deserializer(data)

    def get(self, key, counter=False):
        """
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
        """
        """
        results = dict((key, self.get(key)) for key in keys)
        return dict((key, value) for key, value in results.iteritems() if value is not None)

    def set(self, key, value, content_type="text/plain"):
        """
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

    def set_many(self, values):
        """
        """
        return dict((key, self.set(key, value)) for key, value in values.iteritems())

    def delete(self, key):
        """
        """
        status, data, _ = self.pool.request(
            method="DELETE",
            url="%s/buckets/%s/keys/%s" % (self.base_url, self.bucket, key),
        )
        if status == 400:
            raise exceptions.RiakcachedBadRequest(data)
        return status in (204, 404)

    def delete_many(self, keys):
        """
        """
        return dict((key, self.delete(key)) for key in keys)

    def stats(self):
        """
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/stats" % self.base_url,
        )
        if status == 200:
            return self.deserialize(data, "application/json")
        return None

    def props(self):
        """
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/buckets/%s/props" % (self.base_url, self.bucket),
        )
        if status == 200:
            return json.loads(data)
        return None

    def set_props(self, props):
        """
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
        """
        """
        status, data, _ = self.pool.request(
            method="GET",
            url="%s/buckets/%s/keys?keys=true" % (self.base_url, self.bucket),
        )
        if status == 200:
            return self.deserialize(data, "application/json")
        return None

    def ping(self):
        """
        """
        status, _, _ = self.pool.request(
            method="GET",
            url="%s/ping" % self.base_url,
        )
        return status == 200

    def incr(self, key, value=1):
        """
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
    """
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
        """
        """
        def worker(key, results):
            results.put((key, self.delete(key)))

        args = [[key] for key in keys]

        return self._many(worker, args)

    def set_many(self, values):
        """
        """
        def worker(key, value, results):
            results.put((key, self.set(key, value)))

        args = [list(data) for data in values.items()]
        return self._many(worker, args)

    def get_many(self, keys):
        """
        """
        def worker(key, results):
            results.put((key, self.get(key)))

        args = [[key] for key in keys]
        results = self._many(worker, args)
        results = dict((key, value) for key, value in results.iteritems() if value is not None)
        return results or None
