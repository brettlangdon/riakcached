import mock
import unittest2
import urllib3.exceptions

from riakcached import exceptions
from riakcached.pools import Urllib3Pool


class TestUrllib3Pool(unittest2.TestCase):
    def setUp(self):
        self.patched_connection_from_url = mock.patch("urllib3.connection_from_url")
        self.connection_from_url = self.patched_connection_from_url.start()

    def tearDown(self):
        self.connection_from_url = self.patched_connection_from_url.stop()

    def test_connect_sets_up_pool(self):
        pool = Urllib3Pool()
        self.assertTrue(pool.pool)
        self.connection_from_url.assert_called()
        self.connection_from_url.assert_called_with("http://127.0.0.1:8098")

    def test_connect_with_different_url(self):
        pool = Urllib3Pool(base_url="http://example.org:8098")
        self.assertEqual(pool.url, "http://example.org:8098")
        self.connection_from_url.assert_called()
        self.connection_from_url.assert_called_with("http://example.org:8098")

    def test_connect_auto_connect_doesnt_call_connect(self):
        Urllib3Pool(auto_connect=False)
        self.connection_from_url.assert_not_called()

    def test_close_calls_pool_close(self):
        pool = Urllib3Pool()
        pool.close()
        pool.pool.assert_called()

    def test_request_calls_pool_urlopen(self):
        pool = Urllib3Pool()
        result = mock.Mock()
        result.status = 200
        result.data = ""
        result.getheaders = lambda: {}
        pool.pool.urlopen.return_value = result
        self.assertEqual(pool.request("GET", "http://127.0.0.1:8098/stats"), (200, "", {}))
        pool.pool.urlopen.assert_called_with(
            method="GET",
            url="http://127.0.0.1:8098/stats",
            body=None,
            headers=None,
            timeout=2,
            redirect=False,
        )

    def test_request_urlopen_with_headers_and_body(self):
        pool = Urllib3Pool()
        result = mock.Mock()
        result.status = 200
        result.data = ""
        result.getheaders = lambda: {}
        pool.pool.urlopen.return_value = result
        self.assertEqual(
            pool.request(
                "POST", "http://127.0.0.1:8098/stats", body="test",
                headers={"Content-Type": "application/test"}
            ),
            (200, "", {})
        )
        pool.pool.urlopen.assert_called_with(
            method="POST",
            url="http://127.0.0.1:8098/stats",
            body="test",
            headers={
                "Content-Type": "application/test",
            },
            timeout=2,
            redirect=False,
        )

    def test_request_raise_timeout_error(self):
        pool = Urllib3Pool()
        timeout_error = urllib3.exceptions.TimeoutError(pool.pool, "http://127.0.0.1:8098/stats", "timeout")
        pool.pool.urlopen.side_effect = timeout_error
        self.assertRaises(
            exceptions.RiakcachedTimeout,
            pool.request,
            "GET",
            "http://127.0.0.1:8098/stats",
        )

    def test_request_raise_connections_error(self):
        pool = Urllib3Pool()
        http_error = urllib3.exceptions.HTTPError(pool.pool, "http://127.0.0.1:8098/stats", "error")
        pool.pool.urlopen.side_effect = http_error
        self.assertRaises(
            exceptions.RiakcachedConnectionError,
            pool.request,
            "GET",
            "http://127.0.0.1:8098/stats",
        )
