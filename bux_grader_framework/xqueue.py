"""
    bux_grader_framework.xqueue
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines the XQueue REST client.
"""

import json
import logging
import urlparse

import requests

from .exceptions import BadCredentials, BadQueueName

log = logging.getLogger(__name__)


class InvalidXReply(Exception):
    """ Used internally to indicate malformed XQueue reply string """
    pass


class XQueueClient(object):
    """ A XQueue REST client.

        Implements the external grader interface to XQueue.

        Establishes an HTTP session used in subsequent requests.

        :param str url: XQueue URL
        :param str username: Django auth user
        :param str password: Django auth password
        :param int timeout: Maximum time to wait for XQueue to respond

        :raises XQueueTimeout: if XQueue fails to respond in ``timeout``
                               seconds
        :raises BadCredentials: if the supplied ``username`` or ``password``
                                are invalid

        All XQueue-related exceptions extend :class:`XQueueException`.

        Usage::

            >>> from bux_grader_framework import XQueueClient
            >>> xqueue = XQueueClient('http://localhost:18040', 'lms',
             'password')
            >>> xqueue.get_queuelen('test_queue')
            1
            >>> xqueue.get_submission('test_queue')
            {'xqueue_header': ... , 'xqueue_body': ... , 'xqueue_files': ...}
            >>> # Handle submission
            >>> response = {
            ...     'correct': True,
            ...     'score': 1,
            ...     'msg': '<p>Correct!</p>'
            ... }
            >>> xqueue.put_result(response)
            True

    """
    def __init__(self, url, username, password, timeout=10):
        self.url = url
        self.username = username
        self.password = password
        self.timeout = timeout

        self.session = requests.session()

    def login(self):
        """ Login to XQueue."""
        url = urlparse.urljoin(self.url, "/xqueue/login/")
        data = {"username": self.username, "password": self.password}

        # TODO: Handle connection error / timeouts
        response = self.session.post(url, data=data)

        if not response.ok:
            log.critical("Unable to login to XQueue: {}".format(response.text))
            raise BadCredentials(response.text)

        success, content = self._parse_xreply(response.content)
        if not success:
            log.critical("Unable to login to XQueue: {}".format(content))
            raise BadCredentials(content)

        log.debug("Succesfully logged in as {}".format(self.username))
        return success

    def get_queuelen(self, queue_name):
        """ Returns the current queue length.

            :param str queue_name: an existing XQueue queue name
            :raises: :class:`BadQueueName` if the supplied ``queue_name``
                     is invalid

        """
        log.debug("Fetching queue length for \"{}\"".format(queue_name))
        url = urlparse.urljoin(self.url, "/xqueue/get_queuelen/")
        params = {"queue_name": queue_name}

        # TODO: Handle connection error / timeouts
        response = self.session.get(url, params=params)

        success, content = self._parse_xreply(response.content)
        if not success:
            if "login_required" == content:
                log.debug("Login required, attempting login")
                self.login()
                return self.get_queuelen(queue_name)
            else:
                log.error("Could not get queue length, invalid queue name: {}."
                          "{}".format(queue_name, content))
                raise BadQueueName(content)

        log.debug("Retrieved queue length for \"{}\": {}".format(queue_name,
                                                                 content))
        return content

    def get_submission(self, queue_name):
        """ Pop a submission off of XQueue.

            :param str queue_name: an existing XQueue queue name
            :raises: :class:`BadQueueName` if the supplied ``queue_name``
                     is invalid

            Returns a submission :class:`dict` or :class:`None`.

        """
        log.debug("Fetching submission from \"{}\"".format(queue_name))
        url = urlparse.urljoin(self.url, "/xqueue/get_submission/")
        params = {"queue_name": queue_name}

        success, content = self._get(url, params)
        if not success:
            log.error("Could not get submission: {}".format(content))
            raise BadQueueName(content)

        submission = json.loads(content)
        log.debug("Retrieved submission from \"{}\": {}".format(queue_name,
                                                                submission))

        return submission

    def put_result(self, result):
        """ Posts a result to XQueue.

            :param dict result: A response :class:`dict`
            :return: ``True`` or ``False``

            :raises: :class:`InvalidXQueueReply` if reply :class:`dict`
                     posted to XQueue is improperly formatted

            The response :class:`dict` should be formatted as follows::

                {
                    'correct': True|False,
                    'score': 0-1,
                    'msg': 'Valid XML message string'
                }

            .. warning::

                If the ``msg`` value contains invalid XML the LMS will not
                accept the response.
        """
        pass

    def _get(self, url, params, retry_login=True):
        """ Helper method for XQueue get requests

        Will automatically login if requested

        """
        # TODO: Handle connection error / timeouts
        response = self.session.get(url, params=params)

        success, content = self._parse_xreply(response.content)
        if not success:
            if "login_required" == content and retry_login:
                log.debug("Login required, attempting login")
                self.login()
                return self._http_get(url, params, False)

        return success, content

    def _validate_response(self, reply):
        """ Check the format of the response :class:`dict`.

            XQueue checks the reply to assert:

            1. Presence of ``xqueue_header`` and ``xqueue_body``
            2. Presence of specific metadata in ``xqueue_header``
               (``submission_id``, ``submission_key``)
        """
        pass

    def _parse_xreply(self, reply):
        """ XQueue reply format:

            JSON-serialized :class:`dict`:
               { 'return_code': 0(success)/1(error),
                 'content'    : 'my content', }
        """
        try:
            xreply = json.loads(reply)
        except (TypeError, ValueError):
            raise InvalidXReply

        if not isinstance(xreply, dict):
            raise InvalidXReply

        for key in ['return_code', 'content']:
            if key not in xreply:
                raise InvalidXReply

        success = xreply['return_code'] == 0
        return success, xreply['content']
