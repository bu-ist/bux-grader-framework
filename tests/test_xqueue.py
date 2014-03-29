import json
import unittest

import requests
from mock import MagicMock, patch

from bux_grader_framework.xqueue import XQueueClient, InvalidXReply
from bux_grader_framework.exceptions import BadCredentials

XQUEUE_TEST_CONFIG = {
    "url": "http://localhost:18040",
    "username": "test",
    "password": "passwd",
    "timeout": 10
}


class TestXQueueClient(unittest.TestCase):

    @patch('bux_grader_framework.xqueue.requests.session')
    def setUp(self, mock_session):
        self.client = XQueueClient(**XQUEUE_TEST_CONFIG)

    def test_login(self):
        xqueue_response = {"return_code": 0, "content": "Logged in"}
        response = MagicMock(spec=requests.Response())
        response.content = json.dumps(xqueue_response)

        self.client.session.post = MagicMock(return_value=response)

        self.assertEquals(True, self.client.login())

        post_data = {
            "username": XQUEUE_TEST_CONFIG["username"],
            "password": XQUEUE_TEST_CONFIG["password"]
        }
        self.client.session.post.assert_called_with(
            XQUEUE_TEST_CONFIG["url"] + "/xqueue/login/",
            data=post_data
            )

    def test_login_invalid_requests_response(self):
        bad_response = MagicMock(spec=requests.Response())
        bad_response.ok = False
        bad_response.text = "Bad response"

        self.client.session.post = MagicMock(return_value=bad_response)

        self.assertRaises(BadCredentials, self.client.login)

    def test_login_invalid_credentials(self):
        xreply = {"return_code": 1, "content": "Incorrect login credentials"}
        bad_response = MagicMock(spec=requests.Response())
        bad_response.ok = True
        bad_response.content = json.dumps(xreply)

        self.client.session.post = MagicMock(return_value=bad_response)

        self.assertRaises(BadCredentials, self.client.login)

    def test_parse_xreply(self):
        xreply = json.dumps({"return_code": 0, "content": "Test content"})
        self.assertEquals((True, "Test content"),
                          self.client._parse_xreply(xreply))

    def test_parse_xreply_missing_content(self):
        xreply = json.dumps({"return_code": 1})
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test_parse_xreply_not_dict(self):
        xreply = json.dumps(['not', 'dict'])
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test_parse_xreply_not_json(self):
        xreply = {"return_code": 1, "content": "Not JSON string"}
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test_parse_xreply_empty(self):
        xreply = ""
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)


if __name__ == '__main__':
    unittest.main()
