
import mock
import unittest2
import urllib3.exceptions


from riakcached import exceptions
from riakcached.client import RiakClient
from riakcached.tests.utils import InlineClass


class TestRiakClient(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_client_strips_trailing_url_slash(self):
        client = RiakClient("test_bucket", url="http://127.0.0.1:8098/", auto_connect=False)
        self.assertEqual(client.url, "http://127.0.0.1:8098")

    def test_client_adds_serializer(self):
        serializer = mock.Mock()

        client = RiakClient("test_bucket", auto_connect=False)
        client.add_serializer("application/test", serializer)
        self.assertEqual(client._serializers["application/test"], serializer)
        client.serialize("test", "application/test")
        serializer.assert_called()

    def test_client_adds_deserializer(self):
        deserializer = mock.Mock()

        client = RiakClient("test_bucket", auto_connect=False)
        client.add_deserializer("application/test", deserializer)
        self.assertEqual(client._deserializers["application/test"], deserializer)
        client.deserialize("test", "application/test")
        deserializer.assert_called()

    def test_client_calls_pool_close(self):
        client = RiakClient("test_bucket", auto_connect=False)
        client._pool = mock.Mock()
        client.close()
        self.assertTrue(client._pool.close.called)

    def test_client_calls_pool_connect_with_auto_connect_set(self):
        RiakClient("test_bucket", url="http://127.0.0.1:8098", auto_connect=True)
        self.assertTrue(self.connection_from_url.called)
        self.connection_from_url.assert_called_once_with("http://127.0.0.1:8098")

    def test_client_calls_pool_connect_with_auto_connect_default(self):
        RiakClient("test_bucket", url="http://127.0.0.1:8098")
        self.assertTrue(self.connection_from_url.called)
        self.connection_from_url.assert_called_once_with("http://127.0.0.1:8098")

    def test_client_doesnt_call_pool_connect_with_auto_connect_false(self):
        RiakClient("test_bucket", url="http://127.0.0.1:8098", auto_connect=False)
        self.assertFalse(self.connection_from_url.called)

    def test_client_request_timeout_raises_timeout(self):
        client = RiakClient("test_bucket")
        timeout_error = urllib3.exceptions.TimeoutError(InlineClass({}), "url", "timeout")
        client._pool.urlopen.side_effect = timeout_error
        self.assertRaises(
            exceptions.RiakcachedTimeout,
            client._request,
            "GET",
            "http://127.0.0.1:9080/stats",
        )

    def test_client_request_http_error_raises_connection_error(self):
        client = RiakClient("test_bucket")
        http_error = urllib3.exceptions.HTTPError("http error")
        client._pool.urlopen.side_effect = http_error
        self.assertRaises(
            exceptions.RiakcachedConnectionError,
            client._request,
            "GET",
            "http://127.0.0.1:9080/stats",
        )


class TestGet(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_get_calls_pool_urlopen(self):
        client = RiakClient("test_bucket")
        client.get("test")
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_get_calls_pool_urlopen_for_counters(self):
        client = RiakClient("test_bucket")
        client.get("test", counter=True)
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/counters/test",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_get_invalid_status_code(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 302,
            "data": "",
        })
        self.assertIsNone(client.get("test"))

    def test_get_400_raises_bad_request(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 400,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedBadRequest, client.get, "test")

    def test_get_503_raises_unavailable(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 503,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedServiceUnavailable, client.get, "test")

    def test_get_uses_deserializer(self):
        def deserializer(data):
            return "deserialized"

        client = RiakClient("test_bucket")
        client.add_deserializer("application/test", deserializer)
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "some data",
            "getheader": (lambda header: "application/test"),
        })
        result = client.get("test")
        self.assertEqual("deserialized", result)

    def test_get_many(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "result",
            "getheader": (lambda header: "text/plain"),
        })
        results = client.get_many(["test1", "test2"])
        self.assertEqual(results, {
            "test1": "result",
            "test2": "result",
        })
        self.assertEqual(2, client._pool.urlopen.call_count)
        client._pool.urlopen.assert_any_call(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )
        client._pool.urlopen.assert_any_call(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )


class TestSet(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_set_calls_pool_urlopen(self):
        client = RiakClient("test_bucket")
        client.set("test", "value")
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body="value",
            headers={
                "Content-Type": "text/plain",
            },
            timeout=client._timeout,
            redirect=False,
        )

    def test_set_calls_pool_urlopen_with_correct_content_type(self):
        client = RiakClient("test_bucket")
        client.set("test", "value", content_type="application/test")
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body="value",
            headers={
                "Content-Type": "application/test",
            },
            timeout=client._timeout,
            redirect=False,
        )

    def test_set_invalid_status_code(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 302,
            "data": "",
        })
        self.assertFalse(client.get("test"))

    def test_set_400_raises_bad_request(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 400,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedBadRequest, client.set, "test", "value")

    def test_set_412_raises_precondition_failed(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 412,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedPreconditionFailed, client.set, "test", "value")

    def test_set_uses_serializer(self):
        def serializer(data):
            return "serialized"

        client = RiakClient("test_bucket")
        client.add_serializer("application/test", serializer)
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        client.set("test", "value", content_type="application/test")
        client._pool.urlopen.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body="serialized",
            headers={
                "Content-Type": "application/test",
            },
            timeout=client._timeout,
            redirect=False,
        )

    def test_set_many(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "",
            "getheader": (lambda header: "text/plain"),
        })
        client.set_many({
            "test1": "value1",
            "test2": "value2",
        })
        self.assertEqual(2, client._pool.urlopen.call_count)
        client._pool.urlopen.assert_any_call(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
            body="value1",
            headers={
                "Content-Type": "text/plain",
            },
            timeout=client._timeout,
            redirect=False,
        )
        client._pool.urlopen.assert_any_call(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
            body="value2",
            headers={
                "Content-Type": "text/plain",
            },
            timeout=client._timeout,
            redirect=False,
        )


class TestDelete(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_delete_calls_pool_urlopen(self):
        client = RiakClient("test_bucket")
        client.delete("test")
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="DELETE",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_delete_invalid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "",
        })
        self.assertFalse(client.delete("test"))

    def test_delete_valid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        self.assertTrue(client.delete("test"))

    def test_delete_400_raises_bad_request(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 400,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedBadRequest, client.delete, "test")

    def test_delete_many_calls_pool_urlopen(self):
        client = RiakClient("test_bucket")
        client.delete_many(["test1", "test2"])
        self.assertEqual(2, client._pool.urlopen.call_count)
        client._pool.urlopen.assert_any_call(
            method="DELETE",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test1",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )
        client._pool.urlopen.assert_any_call(
            method="DELETE",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys/test2",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )


class TestCounter(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_incr_calls_pool_urlopen(self):
        client = RiakClient("test_bucket")
        client.incr("test")
        self.assertTrue(client._pool.urlopen.called)
        self.assertTrue(client._pool.urlopen.called_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/counters/test",
            body="1",
            headers=None,
            timeout=client._timeout,
            redirect=False,
        ))

    def test_incr_calls_pool_urlopen_with_value(self):
        client = RiakClient("test_bucket")
        client.incr("test", value=5)
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="POST",
            url="http://127.0.0.1:8098/buckets/test_bucket/counters/test",
            body="5",
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_incr_409_raises_conflict(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 409,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedConflict, client.incr, "test")

    def test_incr_400_raises_bad_request(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 400,
            "data": "",
        })
        self.assertRaises(exceptions.RiakcachedBadRequest, client.incr, "test")

    def test_incr_invalid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 302,
            "data": "",
        })
        self.assertFalse(client.incr("test"))


class TestPing(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_ping(self):
        client = RiakClient("test_bucket")
        client.ping()
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/ping",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_ping_valid_status(self):
        client = RiakClient("test_bucket")
        client.ping()
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "",
        })
        self.assertTrue(client.ping())

    def test_ping_invalid_status(self):
        client = RiakClient("test_bucket")
        client.ping()
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        self.assertFalse(client.ping())


class TestStats(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_stats(self):
        client = RiakClient("test_bucket")
        client.stats()
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/stats",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_stats_valid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": '{"test": "stats"}',
        })
        self.assertEqual(client.stats(), {
            "test": "stats",
        })

    def test_stats_invalid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        self.assertIsNone(client.stats())


class TestProps(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_props(self):
        client = RiakClient("test_bucket")
        client.props()
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/props",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_props_valid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": '{"test": "props"}',
        })
        self.assertEqual(client.props(), {
            "test": "props",
        })

    def test_props_invalid_status(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        self.assertIsNone(client.props())

    def test_set_props(self):
        client = RiakClient("test_bucket")
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": "",
        })
        self.assertTrue(client.set_props({
            "some": "props"
        }))
        client._pool.urlopen.assert_called_once_with(
            method="PUT",
            url="http://127.0.0.1:8098/buckets/test_bucket/props",
            body='{"some": "props"}',
            headers={
                "Content-Type": "application/json",
            },
            timeout=client._timeout,
            redirect=False,
        )


class TestKeys(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.patched_connection_from_url.stop()

    def test_keys(self):
        client = RiakClient("test_bucket")
        client.keys()
        self.assertTrue(client._pool.urlopen.called)
        client._pool.urlopen.assert_called_once_with(
            method="GET",
            url="http://127.0.0.1:8098/buckets/test_bucket/keys?keys=true",
            body=None,
            headers=None,
            timeout=client._timeout,
            redirect=False,
        )

    def test_keys_valid_status(self):
        client = RiakClient("test_bucket")
        client.ping()
        client._pool.urlopen.return_value = InlineClass({
            "status": 200,
            "data": '["key1", "key2"]',
        })
        self.assertEqual(client.keys(), ["key1", "key2"])

    def test_ping_invalid_status(self):
        client = RiakClient("test_bucket")
        client.ping()
        client._pool.urlopen.return_value = InlineClass({
            "status": 204,
            "data": "",
        })
        self.assertIsNone(client.keys())
