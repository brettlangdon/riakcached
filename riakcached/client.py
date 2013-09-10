__all__ = ["RiakClient"]

import json
import Queue
import threading

import urllib3

from riakcached import exceptions


class RiakClient(object):
    """
    """
    __slots__ = [
        "_serializers",
        "_deserializers",
        "_pool",
        "_timeout",
        "bucket",
        "url",
    ]

    def __init__(self, bucket, url="http://127.0.0.1:8098", timeout=2, auto_connect=True):
        """
        """
        self.bucket = bucket
        self.url = url.strip("/")
        self._timeout = timeout
        self._serializers = {
            "application/json": json.dumps,
        }
        self._deserializers = {
            "application/json": json.loads,
        }

        if auto_connect:
            self._connect()

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

    def close(self):
        """
        """
        if self._pool:
            self._pool.close()

    def get(self, key, counter=False):
        """
        """
        url = "%s/buckets/%s/keys/%s" % (self.url, self.bucket, key)
        if counter:
            url = "%s/buckets/%s/counters/%s" % (self.url, self.bucket, key)
        response = self._request(method="GET", url=url)
        if response.status == 400:
            raise exceptions.RiakcachedBadRequest(response.data)
        elif response.status == 503:
            raise exceptions.RiakcachedServiceUnavailable(response.data)

        if response.status not in (200, 300, 304):
            return None

        return self.deserialize(response.data, response.getheader("content-type"))

    def get_many(self, keys):
        """
        """
        def worker(key, results):
            results.put((key, self.get(key)))

        args = [[key] for key in keys]
        results = self._many(worker, args)
        results = dict((key, value) for key, value in results.iteritems() if value is not None)
        return results or None

    def set(self, key, value, content_type="text/plain"):
        """
        """
        value = self.serialize(value, content_type)

        response = self._request(
            method="POST",
            url="%s/buckets/%s/keys/%s" % (self.url, self.bucket, key),
            body=value,
            headers={
                "Content-Type": content_type,
            },
        )
        if response.status == 400:
            raise exceptions.RiakcachedBadRequest(response.data)
        elif response.status == 412:
            raise exceptions.RiakcachedPreconditionFailed(response.data)
        return response.status in (200, 201, 204, 300)

    def set_many(self, values):
        """
        """
        def worker(key, value, results):
            results.put((key, self.set(key, value)))

        args = [list(data) for data in values.items()]
        return self._many(worker, args)

    def delete(self, key):
        """
        """
        response = self._request(
            method="DELETE",
            url="%s/buckets/%s/keys/%s" % (self.url, self.bucket, key),
        )
        if response.status == 400:
            raise exceptions.RiakcachedBadRequest(response.data)
        return response.status in (204, 404)

    def delete_many(self, keys):
        """
        """
        def worker(key, results):
            results.put((key, self.delete(key)))

        args = [[key] for key in keys]

        return self._many(worker, args)

    def stats(self):
        """
        """
        response = self._request(
            method="GET",
            url="%s/stats" % self.url,
        )
        if response.status == 200:
            return self.deserialize(response.data, "application/json")
        return None

    def props(self):
        """
        """
        response = self._request(
            method="GET",
            url="%s/buckets/%s/props" % (self.url, self.bucket),
        )
        if response.status == 200:
            return json.loads(response.data)
        return None

    def set_props(self, props):
        """
        """
        response = self._request(
            method="PUT",
            url="%s/buckets/%s/props" % (self.url, self.bucket),
            body=self.serialize(props, "application/json"),
            headers={
                "Content-Type": "application/json",
            }
        )
        return response.status == 200

    def keys(self):
        """
        """
        response = self._request(
            method="GET",
            url="%s/buckets/%s/keys?keys=true" % (self.url, self.bucket),
        )
        if response.status == 200:
            return self.deserialize(response.data, "application/json")
        return None

    def ping(self):
        """
        """
        response = self._request(
            method="GET",
            url="%s/ping" % self.url,
        )
        return response.status == 200

    def incr(self, key, value=1):
        """
        """
        response = self._request(
            method="POST",
            url="%s/buckets/%s/counters/%s" % (self.url, self.bucket, key),
            body=str(value),
        )
        if response.status == 409:
            raise exceptions.RiakcachedConflict(response.data)
        elif response.status == 400:
            raise exceptions.RiakcachedBadRequest(response.data)
        return response.status in (200, 201, 204, 300)

    def _connect(self):
        self._pool = urllib3.connection_from_url(self.url)

    def _request(self, method, url, body=None, headers=None):
        try:
            return self._pool.urlopen(
                method=method,
                url=url,
                body=body,
                headers=headers,
                timeout=self._timeout,
                redirect=False,
            )
        except urllib3.exceptions.TimeoutError, e:
            raise exceptions.RiakcachedTimeout(e.message)
        except urllib3.exceptions.HTTPError, e:
            raise exceptions.RiakcachedConnectionError(e.message)

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
