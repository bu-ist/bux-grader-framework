import unittest

from mock import MagicMock

from bux_grader_framework.grader import Grader
from bux_grader_framework.workers import XQueueWorker
from bux_grader_framework.queues import SubmissionProducer
from bux_grader_framework.xqueue import XQueueClient


class TestXQueueWorker(unittest.TestCase):

    def setUp(self):
        mock_grader = MagicMock(spec=Grader)
        mock_grader.xqueue.return_value = MagicMock(spec=XQueueClient)
        mock_grader.producer.return_value = MagicMock(spec=SubmissionProducer)

        self.worker = XQueueWorker("dummy_queue", mock_grader)

    def test_enqueue_submission(self):
        pass
