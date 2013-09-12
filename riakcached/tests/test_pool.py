import unittest2

from riakcached.pools import Pool


class TestPool(unittest2.TestCase):
    def test_not_implemented(self):
        pool = Pool(auto_connect=False)
        self.assertRaises(NotImplementedError, pool.connect)
        self.assertRaises(NotImplementedError, pool.close)
        self.assertRaises(
            NotImplementedError,
            pool.request,
            "GET",
            "http://127.0.0.1:8098/stats",
        )
