import urllib3

from riakcached import exceptions


class Pool(object):
    """A Riak HTTP request connection pool base class

    This is the base class that should be used for any custom connection
    pool to be used by any of the :class:`riakcached.clients.RiakClient`
    """
    __slots__ = ["timeout", "url"]

    def __init__(self, base_url="http://127.0.0.1:8098", timeout=2, auto_connect=True):
        """Constructs a new :class:`riakcached.pools.Pool`

        :param base_url: the base url that the client should use for requests
        :type base_url: str
        :param timeout: the connection timeout to use
        :type timeout: int
        :param auto_connect: whether or not to call :func:`connect` on __init__
        :type auto_connect: bool
        """
        self.url = base_url
        self.timeout = timeout
        if auto_connect:
            self.connect()

    def connect(self):
        """Create the connection pool
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)

    def close(self):
        """Closes the connection pool if it is opened
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)

    def request(self, method, url, body=None, headers=None):
        """Makes a single HTTP request

        :param method: the HTTP method to make the requets with
        :type method: str
        :param url: the full url for the request
        :type url: str
        :param body: the data to POST or PUT with the request
        :type body: str
        :param headers: extra headers to add to the request
        :type headers: dict
        :returns: tuple - status, data, headers
        :raises: :class:`riakcached.exceptions.RiakcachedTimeout`
        :raises: :class:`riakcached.exceptions.RiakcachedConnectionError`
        """
        raise NotImplementedError("You must not use %s directly" % self.__class__.__name__)


class Urllib3Pool(Pool):
    """A subclass of :class:`riakcached.pools.Pool` which uses `urllib3` for requests
    """
    __slots__ = ["pool"]

    def connect(self):
        """Create the connection pool
        """
        self.pool = urllib3.connection_from_url(self.url)

    def close(self):
        """Closes the connection pool if it is opened
        """
        if self.pool:
            self.pool.close()

    def request(self, method, url, body=None, headers=None):
        """Makes a single HTTP request

        :param method: the HTTP method to make the requets with
        :type method: str
        :param url: the full url for the request
        :type url: str
        :param body: the data to POST or PUT with the request
        :type body: str
        :param headers: extra headers to add to the request
        :type headers: dict
        :returns: tuple - status, data, headers
        :raises: :class:`riakcached.exceptions.RiakcachedTimeout`
        :raises: :class:`riakcached.exceptions.RiakcachedConnectionError`
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
