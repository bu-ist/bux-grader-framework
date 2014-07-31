import json
import unittest

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout, ConnectionError

from mock import MagicMock, patch

from bux_grader_framework.xqueue import XQueueClient, InvalidXReply
from bux_grader_framework.exceptions import (BadCredentials,
                                             BadQueueName,
                                             InvalidXRequest,
                                             InvalidGraderReply)

XQUEUE_TEST_CONFIG = {
    "url": "http://localhost:18040",
    "username": "test",
    "password": "passwd",
    "basic_username": "edx",
    "basic_password": "edx",
    "timeout": 10
}

DUMMY_XREQUEST = {
    u"xqueue_header": {
        u"submission_id": u"123",
        u"submission_key": u"532bf863b9cb0a57313ab2f473196a27"
        },
    u"xqueue_body": {
        u"student_info": {
            u"anonymous_student_id": u"a87ff679a2f3e71d9181a67b7542122c",
            u"submission_time": u"20140329192521"
        },
        u"student_response": u"Test 1 2 3",
        u"grader_payload": {}
    },
    u"xqueue_files": {}
}

DUMMY_XREQUEST_ENCODED = json.dumps({
    "xqueue_header": json.dumps(DUMMY_XREQUEST["xqueue_header"]),
    "xqueue_body": json.dumps({
        "student_response": DUMMY_XREQUEST["xqueue_body"]["student_response"],
        "student_info": json.dumps(
            DUMMY_XREQUEST["xqueue_body"]["student_info"]
            ),
        "grader_payload": json.dumps(
            DUMMY_XREQUEST["xqueue_body"]["grader_payload"]
            ),
    }),
    "xqueue_files": json.dumps(DUMMY_XREQUEST["xqueue_files"])
})

DUMMY_SUBMISSION = {
    u"xqueue_header": {
        u"submission_id": u"123",
        u"submission_key": u"532bf863b9cb0a57313ab2f473196a27"
        }
}

DUMMY_GRADER_RESPONSE = {
    "correct": True,
    "score": 1.0,
    "msg": "<p>Good Job!</p>"
}


class TestXQueueClient(unittest.TestCase):

    @patch('bux_grader_framework.xqueue.requests.session')
    def setUp(self, mock_session):
        self.client = XQueueClient(**XQUEUE_TEST_CONFIG)

    @patch('bux_grader_framework.xqueue.requests.session')
    def test_basic_auth_is_set_correctly(self, mock_session):
        client = XQueueClient("http://localhost:18040", "test", "passwd", "foo", "bar")
        self.assertIsInstance(client.basic_auth, HTTPBasicAuth)
        self.assertEquals(client.basic_auth.username, "foo")
        self.assertEquals(client.basic_auth.password, "bar")

    @patch('bux_grader_framework.xqueue.requests.session')
    def test_basic_auth_is_not_required(self, mock_session):
        client = XQueueClient("http://localhost:18040", "test", "passwd")
        self.assertIsNone(client.basic_auth)

    def test_login(self):
        response = (True, "Logged in")
        self.client._request = MagicMock(return_value=response)

        self.assertEquals(True, self.client.login())

        post_data = {
            "username": XQUEUE_TEST_CONFIG["username"],
            "password": XQUEUE_TEST_CONFIG["password"]
        }
        self.client._request.assert_called_with(
            XQUEUE_TEST_CONFIG["url"] + "/xqueue/login/",
            'post',
            data=post_data,
            retry_login=False
            )

    def test_login_invalid_credentials(self):
        response = (False, "Incorrect login credentials")

        self.client._request = MagicMock(return_value=response)

        self.assertRaises(BadCredentials, self.client.login)

    def test_get_queuelen(self):
        response = (True, "2")
        self.client._request = MagicMock(return_value=response)

        self.assertEquals(2, self.client.get_queuelen("foo"))

        self.client._request.assert_called_with(
            XQUEUE_TEST_CONFIG["url"] + "/xqueue/get_queuelen/",
            'get',
            params={"queue_name": "foo"}
            )

    def test_get_queuelen_invalid_queue_name(self):
        response = (False, "Valid queue names are: certificates,"
                           "edX-Open_DemoX, open-ended, test-pull")
        self.client._request = MagicMock(return_value=response)

        self.assertRaises(BadQueueName, self.client.get_queuelen, "bar")

    def test_get_submission(self):
        response = (True, DUMMY_XREQUEST_ENCODED)
        self.client._request = MagicMock(return_value=response)

        self.assertEquals(DUMMY_XREQUEST, self.client.get_submission("foo"))

        self.client._request.assert_called_with(
            XQUEUE_TEST_CONFIG["url"] + "/xqueue/get_submission/",
            'get',
            params={"queue_name": "foo"}
            )

    def test_get_submission_invalid_queue_name(self):
        response = (False, "Queue 'bar' not found")
        self.client._request = MagicMock(return_value=response)

        self.assertRaises(BadQueueName, self.client.get_submission, "bar")

    def test_get_submission_empty_queue(self):
        response = (False, "Queue 'foo' is empty")
        self.client._request = MagicMock(return_value=response)

        self.assertEquals(None, self.client.get_submission("foo"))

    def test_put_reply(self):
        response = (True, "")
        self.client._request = MagicMock(return_value=response)

        self.assertEquals(True,
                          self.client.put_result(DUMMY_SUBMISSION,
                                                 DUMMY_GRADER_RESPONSE))

        post_data = {
            "xqueue_header": json.dumps(DUMMY_SUBMISSION["xqueue_header"]),
            "xqueue_body": json.dumps(DUMMY_GRADER_RESPONSE)
        }
        self.client._request.assert_called_with(
            XQUEUE_TEST_CONFIG["url"] + "/xqueue/put_result/",
            'post',
            data=post_data
            )

    def test_put_reply_invalid_reply_format(self):
        response = (False, "Incorrect reply format")
        self.client._request = MagicMock(return_value=response)

        self.assertRaises(InvalidGraderReply,
                          self.client.put_result,
                          DUMMY_SUBMISSION,
                          DUMMY_GRADER_RESPONSE)

    def test__request(self):
        xreply = {"return_code": 0, "content": "success"}
        response = MagicMock(spec=requests.Response())
        response.status_code = 200
        response.content = json.dumps(xreply)

        self.client.session.request = MagicMock(return_value=response)

        self.assertEquals((True, "success"),
                          self.client._request("http://example.com"))

    @patch('bux_grader_framework.XQueueClient.login', return_value=True)
    def test__request_retries_login(self, mock_login):
        initial_xreply = {"return_code": 1, "content": "login_required"}
        pre_login_response = MagicMock(spec=requests.Response())
        pre_login_response.status_code = 200
        pre_login_response.content = json.dumps(initial_xreply)

        final_xreply = {"return_code": 0, "content": "success"}
        post_login_response = MagicMock(spec=requests.Response())
        post_login_response.status_code = 200
        post_login_response.content = json.dumps(final_xreply)

        side_effect = [pre_login_response, post_login_response]
        self.client.session.request = MagicMock(side_effect=side_effect)

        self.assertEquals((True, "success"),
                          self.client._request("http://example.com"))

    @patch('bux_grader_framework.XQueueClient.login',
           side_effect=BadCredentials)
    def test__request_retries_login_failure(self, mock_login):
        # Initial `GET "/xqueue/queuelen/"` response
        initial_response = {"return_code": 1, "content": "login_required"}
        pre_login_response = MagicMock(spec=requests.Response())
        pre_login_response.status_code = 200
        pre_login_response.content = json.dumps(initial_response)

        self.client.session.request = MagicMock(return_value=pre_login_response)

        self.assertRaises(BadCredentials, self.client._request,
                          "http://example.com")

    def test__request_error(self):
        response = (False, "XQueue request failed: foobar")
        mock_request = MagicMock(side_effect=ConnectionError("foobar"))
        self.client.session.request = mock_request

        self.assertEquals(response,
                          self.client._request('http://example.com', 'get'))

    def test__request_exceeds_timeout(self):
        response = (False, "XQueue request exceeded timeout of {}s".format(
                           self.client.timeout))
        mock_request = MagicMock(side_effect=Timeout)
        self.client.session.request = mock_request

        self.assertEquals(response,
                          self.client._request('http://example.com', 'get'))

    def test__request_non_200(self):
        response = (False, "Unexpected HTTP status code [503]")
        mock_response = MagicMock(spec=requests.Response())
        mock_response.status_code = 503
        mock_response.content = 'Xqueue 500 error'
        mock_request = MagicMock(return_value=mock_response)
        self.client.session.request = mock_request

        self.assertEquals(response,
                          self.client._request('http://example.com', 'get'))

    def test__request_basic_auth_fail(self):
        response = MagicMock(spec=requests.Response())
        response.status_code = 401
        self.client.session.request = MagicMock(return_value=response)

        self.assertEquals((False, "Unexpected HTTP status code [401]"),
                          self.client._request("http://example.com"))

    def test__parse_xreply(self):
        xreply = json.dumps({"return_code": 0, "content": "Test content"})
        self.assertEquals((True, "Test content"),
                          self.client._parse_xreply(xreply))

    def test__parse_xreply_missing_content(self):
        xreply = json.dumps({"return_code": 1})
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test__parse_xreply_not_dict(self):
        xreply = json.dumps(['not', 'dict'])
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test__parse_xreply_not_json(self):
        xreply = {"return_code": 1, "content": "Not JSON string"}
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test__parse_xreply_empty(self):
        xreply = ""
        self.assertRaises(InvalidXReply,
                          self.client._parse_xreply, xreply)

    def test__parse_xrequest(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        response = (DUMMY_XREQUEST["xqueue_header"],
                    DUMMY_XREQUEST["xqueue_body"],
                    DUMMY_XREQUEST["xqueue_files"])
        self.assertEquals(response,
                          self.client._parse_xrequest(xrequest))

    def test__parse_xrequest_header_not_dict(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xrequest['xqueue_header'] = "[]"

        self.assertRaises(InvalidXRequest,
                          self.client._parse_xrequest, xrequest)

    def test__parse_xrequest_body_not_dict(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xrequest['xqueue_body'] = "[]"

        self.assertRaises(InvalidXRequest,
                          self.client._parse_xrequest, xrequest)

    def test__parse_xrequest_missing_header_key(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xheader = json.loads(xrequest["xqueue_header"])
        del xheader["submission_id"]
        xrequest["xqueue_header"] = json.dumps(xheader)

        self.assertRaises(InvalidXRequest,
                          self.client._parse_xrequest, xrequest)

    def test__parse_xrequest_missing_body_key(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xbody = json.loads(xrequest["xqueue_body"])
        del xbody["grader_payload"]
        xrequest["xqueue_body"] = json.dumps(xbody)

        self.assertRaises(InvalidXRequest,
                          self.client._parse_xrequest, xrequest)

    def test__parse_xrequest_payload_string(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xbody = json.loads(xrequest["xqueue_body"])
        xbody["grader_payload"] = "test string"
        xrequest["xqueue_body"] = json.dumps(xbody)

        header, body, files = self.client._parse_xrequest(xrequest)
        self.assertEquals("test string", body["grader_payload"])

    def test__parse_xrequest_payload_json_allows_control_chars(self):
        xrequest = json.loads(DUMMY_XREQUEST_ENCODED)
        xbody = json.loads(xrequest["xqueue_body"])
        xbody["grader_payload"] = '{"answer": "SELECT *\r\nWHERE id = 10"}'
        xrequest["xqueue_body"] = json.dumps(xbody)

        header, body, files = self.client._parse_xrequest(xrequest)
        self.assertEquals("SELECT *\r\nWHERE id = 10",
                          body["grader_payload"]["answer"])

if __name__ == '__main__':
    unittest.main()
