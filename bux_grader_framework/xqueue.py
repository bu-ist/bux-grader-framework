"""
    bux_grader_framework.xqueue
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines the XQueue REST client.
"""


class XQueueClient(object):
    """ A XQueue REST client.

        Implements the external grader interface to XQueue.

        Establishes an HTTP session used in subsequent requests.

        :param str queue: An existing XQueue queue name
        :param str host: XQueue host name or IP
        :param int port: XQueue port number
        :param str username: Django auth user
        :param str password: Django auth password
        :param int timeout: Maximum time to wait for XQueue to respond

        :raises XQueueTimeout: if XQueue fails to respond in ``timeout``
                               seconds
        :raises BadCredentials: if the supplied ``username`` or ``password``
                                are invalid
        :raises BadQueueName: if the supplied ``queue`` name is invalid
        :raises InvalidXQueueReply: if reply :class:`dict` posted to XQueue is
                                    improperly formatted

        All XQueue-related exceptions extend :class:`XQueueException`.

        Usage::

            >>> from bux_grader_framework import XQueueClient
            >>> xqueue = XQueueClient('demo', 'localhost', 18040, 'lms', 'password')
            >>> xqueue.get_queuelen()
            1
            >>> xqueue.get_submission()
            {'xqueue_header': ... , 'xqueue_body': ... , 'xqueue_files': ...}
            >>> # Handle submission
            >>> response = {'correct': True, 'score': 1, 'msg': '<p>Correct!</p>'}
            >>> xqueue.put_result(response)
            True

    """
    def __init__(self, queue, host, port, username, password, timeout=10):
        pass

    def login(self):
        """ Login to XQueue."""
        pass

    def get_queuelen(self):
        """ Returns the current queue length."""
        pass

    def get_submission(self):
        """ Pop a submission off of XQueue.

        Returns a submission :class:`dict` or :class:`None`.
        """
        pass

    def put_result(self, result):
        """ Posts a result to XQueue.

            :param dict result: A response :class:`dict`
            :return: True or False

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
        pass
