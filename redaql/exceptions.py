

class RedaqlException(Exception):
    """base"""


class InvalidSpCommandException(RedaqlException):
    """"invalid sp command"""


class NotFoundDataSourceException(RedaqlException):
    """"invalid ds"""

