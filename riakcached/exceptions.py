class RiakcachedException(Exception):
    """Base class for Riakcached Exceptions
    """
    pass


class RiakcachedBadRequest(RiakcachedException):
    """Exception that is raised for unexpected HTTP 400 status responses

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedNotFound(RiakcachedException):
    """Exception that is raised for unexpected HTTP 204 status responses

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedServiceUnavailable(RiakcachedException):
    """Exception that is raised for unexpected HTTP 503 status responses

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedPreconditionFailed(RiakcachedException):
    """Exception that is raised for unexpected HTTP 412 status responses

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedConflict(RiakcachedException):
    """Exception that is raised for unexpected HTTP 409 status responses

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedTimeout(RiakcachedException):
    """Exception that is raised when pool requests takes too long

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass


class RiakcachedConnectionError(RiakcachedException):
    """Exception that is raised when pool requests raises an `HTTPError`

    Inherits from :class:`riakcached.exceptions.RiakcachedException`
    """
    pass
