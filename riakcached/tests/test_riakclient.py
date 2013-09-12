
import mock
import unittest2


from riakcached import exceptions
from riakcached.clients import RiakClient
import riakcached.pools


class TestRiakClient(unittest2.TestCase):
    def test_uses_default_pool(self):
        client = RiakClient("test_bucket")
        self.assertIsInstance(client.pool, riakcached.pools.Urllib3Pool)

    def test_client_strips_trailing_url_slash(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.url = "http://127.0.0.1:8098/"
        client = RiakClient("test_bucket", pool=pool)
        self.assertEqual(client.base_url, "http://127.0.0.1:8098")

    def test_client_adds_serializer(self):
        serializer = mock.Mock()

        client = RiakClient("test_bucket", pool=mock.Mock(spec=riakcached.pools.Pool))
        client.add_serializer("application/test", serializer)
        self.assertEqual(client._serializers["application/test"], serializer)
        client.serialize("test", "application/test")
        serializer.assert_called()

    def test_client_adds_deserializer(self):
        deserializer = mock.Mock()

        client = RiakClient("test_bucket", pool=mock.Mock(spec=riakcached.pools.Pool))
        client.add_deserializer("application/test", deserializer)
        self.assertEqual(client._deserializers["application/test"], deserializer)
        client.deserialize("test", "application/test")
        deserializer.assert_called()

    def test_get_calls_pool_request_for_counters(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = None, None, None
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        client.get("test", counter=True)
        self.assertTrue(pool.request.called)
        pool.request.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/counters/test",
        )

    def test_get_invalid_status_code(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 302, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertIsNone(client.get("test"))

    def test_get_400_raises_bad_request(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 400, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedBadRequest, client.get, "test")

    def test_get_503_raises_unavailable(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 503, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedServiceUnavailable, client.get, "test")

    def test_get_uses_deserializer(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "some data", {"content-type": "application/test"}
        pool.url = "http://127.0.0.1:8098"

        def deserializer(data):
            return "deserialized"

        client = RiakClient("test_bucket", pool=pool)
        client.add_deserializer("application/test", deserializer)
        result = client.get("test")
        self.assertEqual("deserialized", result)

    def test_get_many(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "result", {"content-type": "text/plain"}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
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

    def test_set_calls_pool_request_with_correct_content_type(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = None, None, None
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        client.set("test", "value", content_type="application/test")
        self.assertTrue(pool.request.called)
        pool.request.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body="value",
            headers={
                "Content-Type": "application/test",
            },
        )

    def test_set_invalid_status_code(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 302, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertFalse(client.get("test"))

    def test_set_400_raises_bad_request(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 400, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedBadRequest, client.set, "test", "value")

    def test_set_412_raises_precondition_failed(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 412, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedPreconditionFailed, client.set, "test", "value")

    def test_set_uses_serializer(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        def serializer(data):
            return "serialized"

        client = RiakClient("test_bucket", pool=pool)
        client.add_serializer("application/test", serializer)
        client.set("test", "value", content_type="application/test")
        pool.request.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body="serialized",
            headers={
                "Content-Type": "application/test",
            },
        )

    def test_set_many(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "", {"content-type": "text/plain"}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
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

    def test_delete_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertFalse(client.delete("test"))

    def test_delete_valid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertTrue(client.delete("test"))

    def test_delete_400_raises_bad_request(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 400, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedBadRequest, client.delete, "test")

    def test_delete_many_calls_pool_request(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
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

    def test_incr_calls_pool_urlopen_with_value(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = None, None, None
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        client.incr("test", value=5)
        self.assertTrue(pool.request.called)
        pool.request.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/counters/test",
            body="5",
        )

    def test_incr_409_raises_conflict(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 409, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedConflict, client.incr, "test")

    def test_incr_400_raises_bad_request(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 400, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertRaises(exceptions.RiakcachedBadRequest, client.incr, "test")

    def test_incr_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 302, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertFalse(client.incr("test"))

    def test_ping_valid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        client.ping()
        self.assertTrue(client.ping())

    def test_ping_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        client.ping()
        self.assertFalse(client.ping())

    def test_stats_valid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, '{"test": "stats"}', {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertEqual(client.stats(), {
            "test": "stats",
        })

    def test_stats_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertIsNone(client.stats())

    def test_props_valid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, '{"test": "props"}', {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertEqual(client.props(), {
            "test": "props",
        })

    def test_props_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertIsNone(client.props())

    def test_set_props(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertTrue(client.set_props({
            "some": "props"
        }))
        pool.request.assert_called_once_with(
            method="PUT",
            url="http://127.0.0.1:8098/buckets/test_bucket/props",
            body='{"some": "props"}',
            headers={
                "Content-Type": "application/json",
            },
        )

    def test_keys_valid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 200, '["key1", "key2"]', {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertEqual(client.keys(), ["key1", "key2"])

    def test_keys_invalid_status(self):
        pool = mock.Mock(spec=riakcached.pools.Pool)
        pool.request.return_value = 204, "", {}
        pool.url = "http://127.0.0.1:8098"

        client = RiakClient("test_bucket", pool=pool)
        self.assertIsNone(client.keys())
