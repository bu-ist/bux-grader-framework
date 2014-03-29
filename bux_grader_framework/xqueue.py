"""
    bux_grader_framework.xqueue
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines the XQueue REST client.
"""

import json
import logging
import urlparse

import requests

from .exceptions import BadCredentials, BadQueueName, InvalidXRequest

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

        success, content = self._get(url, params)
        if not success:
            log.error("Could not get queue length, invalid queue name: {}."
                      "{}".format(queue_name, content))
            raise BadQueueName(content)

        queuelen = int(content)
        log.debug("Retrieved queue length for \"{}\": {}".format(queue_name,
                                                                 queuelen))
        return queuelen

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

        # Convert response string to dicts
        submission = json.loads(content)
        try:
            header, body, files = self._parse_xrequest(submission)
        except InvalidXRequest:
            log.exception("Malformed submission received from XQueue: {}"
                          .format(submission))
            raise

        log.debug("Retrieved submission from \"{}\": {}".format(queue_name,
                                                                submission))

        return {"xqueue_header": header,
                "xqueue_body": body,
                "xqueue_files": files}

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

    def _get(self, url, params=None, retry_login=True):
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
                return self._get(url, params, False)

        return success, content

    def _parse_xrequest(self, reply):
        """ Check the format of an XQueue request :class:`dict`.

            :param dict reply: response body from xqueue
            :return: (xqueue_header dict, xqueue_body dict, xqueue_files dict)
            :rtype: tuple

            Ensures that XQueue request content consists of:

            1. ``xqueue_header``, ``xqueue_body`` and ``xqueue_files`` dicts
            2. ``submission_id`` and ``submission_key`` in ``xqueue_header``
            2. ``student_info`` and ``grader_payload`` in ``xqueue_body``

            Response dicts have json decoded all non-string values.

        """
        try:
            header = reply['xqueue_header']
            body = reply['xqueue_body']
            files = reply['xqueue_files']
        except KeyError:
            raise InvalidXRequest

        try:
            header_dict = json.loads(header)
            body_dict = json.loads(body)
            files_dict = json.loads(files)
        except (TypeError, ValueError):
            raise InvalidXRequest

        if not isinstance(header_dict, dict):
            raise InvalidXRequest

        for header_key in ['submission_id', 'submission_key']:
            if header_key not in header_dict:
                raise InvalidXRequest

        if not isinstance(body_dict, dict):
            raise InvalidXRequest

        for body_key in ['student_info', 'grader_payload', 'student_response']:
            if body_key not in body_dict:
                raise InvalidXRequest

        try:
            student_info = json.loads(body_dict['student_info'])
            grader_payload = json.loads(body_dict['grader_payload'])
        except (TypeError, ValueError):
            raise InvalidXRequest

        body_dict['student_info'] = student_info
        body_dict['grader_payload'] = grader_payload

        return header_dict, body_dict, files_dict

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
