

class RedaqlException(Exception):
    """base"""


class InvalidSpCommandException(RedaqlException):
    """"invalid sp command"""


class NotFoundDataSourceException(RedaqlException):
    """"invalid ds"""


class FutureFeatureException(RedaqlException):
    """ future """


class LatestQueryFailedException(RedaqlException):
    """ Failed Query Buffer """


class InvalidArgumentException(RedaqlException):
    """ invalid arguments """
