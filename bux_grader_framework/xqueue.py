"""
    bux_grader_framework.xqueue
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines the XQueue REST client.
"""

import json
import logging
import urlparse

import requests
from requests.exceptions import Timeout, HTTPError, ConnectionError
from statsd import statsd

from .exceptions import (XQueueException,
                         BadCredentials, BadQueueName,
                         InvalidXRequest, InvalidGraderReply)

log = logging.getLogger(__name__)


class InvalidXReply(Exception):
    """ Used internally to indicate a malformed XQueue reply """
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
    INVALID_LOGIN_MSG = "Incorrect login credentials"
    QUEUE_NOT_FOUND_MSG = "Queue '%s' not found"
    EMPTY_QUEUE_MSG = "Queue '%s' is empty"

    def __init__(self, url, username, password, timeout=10):
        self.url = url
        self.username = username
        self.password = password
        self.timeout = timeout

        self.session = requests.session()

    @statsd.timer('bux_grader_framework.xqueue.login')
    def login(self):
        """ Login to XQueue."""
        url = urlparse.urljoin(self.url, "/xqueue/login/")
        post_data = {"username": self.username, "password": self.password}

        success, content = self._request(url, 'post', data=post_data,
                                         retry_login=False)
        if not success:
            error_msg = "Unable to login to XQueue: {}".format(content)
            if self.INVALID_LOGIN_MSG == content:
                raise BadCredentials(error_msg)
            else:
                raise XQueueException(error_msg)

        log.debug("Succesfully logged in as {}".format(self.username))
        return success

    @statsd.timer('bux_grader_framework.xqueue.get_queuelen')
    def get_queuelen(self, queue_name):
        """ Returns the current queue length.

            :param str queue_name: an existing XQueue queue name
            :raises: :class:`BadQueueName` if the supplied ``queue_name``
                     is invalid

        """
        log.debug("Fetching queue length for \"{}\"".format(queue_name))
        url = urlparse.urljoin(self.url, "/xqueue/get_queuelen/")
        params = {"queue_name": queue_name}

        success, content = self._request(url, "get", params=params)
        if not success:
            error_msg = "Could not get queue length: {}".format(content)
            if content.startswith("Valid queue names are"):
                raise BadQueueName(error_msg)
            else:
                raise XQueueException(error_msg)

        queuelen = int(content)
        log.debug("Retrieved queue length for \"{}\": {}".format(queue_name,
                                                                 queuelen))
        return queuelen

    @statsd.timer('bux_grader_framework.xqueue.get_submission')
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

        success, content = self._request(url, 'get', params=params)
        if not success:
            error_msg = "Could not get submission: {}".format(content)
            if self.QUEUE_NOT_FOUND_MSG % queue_name == content:
                raise BadQueueName(error_msg)
            elif self.EMPTY_QUEUE_MSG % queue_name == content:
                return None
            else:
                raise XQueueException(error_msg)

        # Convert response string to dicts
        submission = json.loads(content)
        header, body, files = self._parse_xrequest(submission)

        log.debug("Retrieved submission from \"{}\": {}".format(queue_name,
                                                                submission))

        return {"xqueue_header": header,
                "xqueue_body": body,
                "xqueue_files": files}

    @statsd.timer('bux_grader_framework.xqueue.put_result')
    def put_result(self, submission, result):
        """ Posts a result to XQueue.

            :param dict submission: the submission this result is for
            :param dict result: the grader response :class:`dict`
            :return: ``True`` or ``False``

            :raises: :class:`InvalidGraderReply` if XQueue rejects the
                                                 result

            The result :class:`dict` should be formatted as follows::

                {
                    'correct': True|False,
                    'score': 0-1,
                    'msg': '<p>Good work!</p>'
                }

            .. warning::

                If the ``msg`` value contains invalid XML the LMS will not
                accept the response.

        """
        log.debug("Posting result to XQueue: {}".format(result))
        url = urlparse.urljoin(self.url, "/xqueue/put_result/")

        # TODO: Add validation of submission, result dict
        post_data = {
            "xqueue_header": json.dumps(submission["xqueue_header"]),
            "xqueue_body": json.dumps(result)
        }

        success, content = self._request(url, 'post', data=post_data)
        if not success:
            log.error("Could not post result: {}".format(content))
            raise InvalidGraderReply(content)

        log.debug("Succesfully posted result to XQueue.")
        return success

    def _request(self, url, method='get', params=None, data=None,
                 retry_login=True):
        """ Thin wrapper around ``requests.request``.

        Fails gracefully and retries request after login when denied.

        """

        try:
            response = self.session.request(method, url, params=params,
                                            data=data, timeout=self.timeout)
        except Timeout:
            return False, "XQueue request exceeded timeout of {}s".format(
                          self.timeout)
        except (ConnectionError, HTTPError) as e:
            return False, "XQueue request failed: {}".format(str(e))
        log.debug("Raw XQueue response: {}".format(response.text))

        # Check status code before attempting to parse
        if response.status_code not in [200]:
            log.error('HTTP request failed: %s [%d]', response.content,
                      response.status_code)
            return False, 'Unexpected HTTP status code [{}]'.format(
                          response.status_code)

        try:
            success, content = self._parse_xreply(response.content)
        except InvalidXReply:
            log.exception("Invalid XQueue reply: {}".format(response.content))
            return False, "Could not parse XQueue reply"

        if not success:
            if "login_required" == content and retry_login:
                log.debug("Login required, attempting login")
                self.login()
                return self._request(url, method, params, data, False)

        return success, content

    # TODO: Break in to two functions -- one for validation, one for parsing
    def _parse_xrequest(self, request):
        """ Check the format of an XQueue request :class:`dict`.

            :param dict request: response body from xqueue
            :return: (xqueue_header dict, xqueue_body dict, xqueue_files dict)
            :rtype: tuple

            Ensures that XQueue request content consists of:

            1. ``xqueue_header``, ``xqueue_body`` and ``xqueue_files`` dicts
            2. ``submission_id`` and ``submission_key`` in ``xqueue_header``
            2. ``student_info`` and ``grader_payload`` in ``xqueue_body``

            Response dicts have json decoded all non-string values.

        """
        try:
            header = request['xqueue_header']
            body = request['xqueue_body']
            files = request['xqueue_files']
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

        for body_key in ['grader_payload', 'student_response']:
            if body_key not in body_dict:
                raise InvalidXRequest

        # Attempt to parse grader payload as JSON
        try:
            payload = body_dict['grader_payload']
            body_dict['grader_payload'] = json.loads(payload, strict=False)
        except (TypeError, ValueError):
            log.warning('Unable to parse "grader_payload": %s', payload)
            # Could be an invalid JSON string, but might not be JSON at all.
            # Leave it as-is and let the calling code deal with it.
            pass

        # Attempt to parse student info dict
        try:
            student_info = body_dict['student_info']
            body_dict['student_info'] = json.loads(student_info)
        except (KeyError, TypeError, ValueError):
            # Student info dict isn't always present (e.g. load test
            # submissions) so we fail silently.
            body_dict['student_info'] = {}

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
            raise InvalidXReply("XQueue response is not JSON")

        if not isinstance(xreply, dict):
            raise InvalidXReply("XQueue reply is not a dict")

        for key in ['return_code', 'content']:
            if key not in xreply:
                raise InvalidXReply("XQueue response dict is missing keys")

        success = (xreply['return_code'] == 0)
        return success, xreply['content']
