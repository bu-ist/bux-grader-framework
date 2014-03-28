"""
    bux_grader_framework.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines all exceptions thrown by this package.
"""


class ImproperlyConfiguredGrader(Exception):
    """ A :class:`BaseGrader` subclass was improperly configured """
    pass


class XQueueException(Exception):
    """ Base XQueue exception """
    pass


class ConnectionTimeout(XQueueException):
    """ XQueue failed to respond within the configured timeout """
    pass


class BadQueueName(XQueueException):
    """ An invalid queue name was used """
    pass


class BadCredentials(XQueueException):
    """ An invalid username or password was used """
    pass


class InvalidReply(XQueueException):
    """ An invalid username or password was used """
    pass
