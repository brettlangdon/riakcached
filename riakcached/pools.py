import urllib3

from riakcached import exceptions


class Pool(object):
    """
    """
    __slots__ = ["timeout", "url"]

    def __init__(self, base_url="http://127.0.0.1:8098", timeout=2, auto_connect=True):
        """
        """
        self.url = base_url
        self.timeout = timeout
        if auto_connect:
            self.connect()

    def connect(self):
        """
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)

    def close(self):
        """
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)

    def request(self, method, url, body=None, headers=None):
        """
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)


class Urllib3Pool(Pool):
    """
    """
    __slots__ = ["pool"]

    def connect(self):
        """
        """
        self.pool = urllib3.connection_from_url(self.url)

    def close(self):
        """
        """
        if self.pool:
            self.pool.close()

    def request(self, method, url, body=None, headers=None):
        """
        """
        try:
            response = self.pool.urlopen(
                method=method,
                url=url,
                body=body,
                headers=headers,
                timeout=self.timeout,
                redirect=False,
            )
            return response.status, response.data, response.getheaders()
        except urllib3.exceptions.TimeoutError, e:
            raise exceptions.RiakcachedTimeout(e.message)
        except urllib3.exceptions.HTTPError, e:
            raise exceptions.RiakcachedConnectionError(e.message)
