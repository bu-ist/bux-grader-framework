"""
    bux_grader_framework.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines all exceptions thrown by this package.
"""


class ImproperlyConfiguredGrader(Exception):
    """ A :class:`Grader` instance was improperly configured """
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


class InvalidRequest(XQueueException):
    """ XQueue was unable to process the request """
    pass


class InvalidGraderReply(XQueueException):
    """ XQueue was unable to process the grader reply """
    pass


class InvalidXRequest(XQueueException):
    """ An invalid XQueue request was sent or received """
    pass
