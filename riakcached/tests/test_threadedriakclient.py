import mock
import unittest2

from riakcached.clients import ThreadedRiakClient
import riakcached.pools


class TestThreadedRiakClient(unittest2.TestCase):
    def test_get_many(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "result", {"content-type": "text/plain"}
        pool.url = "http://127.0.0.1:8098"

        client = ThreadedRiakClient("test_bucket", pool=pool)
        results = client.get_many(["test1", "test2"])
        self.assertEqual(results, {
            "test1": "result",
            "test2": "result",
        })
        self.assertEqual(2, pool.request.call_count)
        pool.request.assert_any_call(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
        )
        pool.request.assert_any_call(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
        )

    def test_set_many(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "", {"content-type": "text/plain"}
        pool.url = "http://127.0.0.1:8098"

        client = ThreadedRiakClient("test_bucket", pool=pool)
        client.set_many({
            "test1": "value1",
            "test2": "value2",
        })
        self.assertEqual(2, pool.request.call_count)
        pool.request.assert_any_call(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
            body="value1",
            headers={
                "Content-Type": "text/plain",
            },
        )
        pool.request.assert_any_call(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
            body="value2",
            headers={
                "Content-Type": "text/plain",
            },
        )

    def test_delete_many(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = ThreadedRiakClient("test_bucket", pool=pool)
        client.delete_many(["test1", "test2"])
        self.assertEqual(2, pool.request.call_count)
        pool.request.assert_any_call(
            method="DELETE",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
        )
        pool.request.assert_any_call(
            method="DELETE",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
        )
