import unittest

from mock import MagicMock

from bux_grader_framework.grader import Grader
from bux_grader_framework.workers import XQueueWorker
from bux_grader_framework.queues import WorkQueue
from bux_grader_framework.xqueue import XQueueClient


class TestXQueueWorker(unittest.TestCase):

    def setUp(self):
        mock_grader = MagicMock(spec=Grader)
        mock_grader.xqueue.return_value = MagicMock(spec=XQueueClient)
        mock_grader.work_queue.return_value = MagicMock(spec=WorkQueue)

        self.worker = XQueueWorker("dummy_queue", mock_grader)

    def test_enqueue_submission(self):
        submission = {
            "xqueue_header": {"submission_id": 1},
            "xqueue_body": {"grader_payload": {"evaluator": "foo"}}
            }
        self.worker.enqueue_submission(submission)

        self.worker.queue.put.assert_called_with('foo', submission)
