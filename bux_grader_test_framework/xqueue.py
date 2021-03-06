import datetime
import logging
import multiprocessing
import Queue
import time
import random

from statsd import statsd


log = logging.getLogger(__name__)


class XQueueStub(object):
    """ Mocks the XQueue interface, using multiprocessing.Queues
        to keep track of submissions / posted results.

    """
    def __init__(self, count, rate, simulation=False):
        self.count = count
        self.rate = rate
        self.simulation = simulation

        self.submissions = multiprocessing.JoinableQueue()
        self.results = multiprocessing.Queue()

    def login(self):
        return True

    def submit(self, submission):
        statsd.incr('bux_grader_test_framework.submissions.sent')
        self.submissions.put_nowait(submission)

    def get_queuelen(self, queue_name):

        # /xqueue/get_queuelen/ typically finishes in under 150ms
        if self.simulation:
            time.sleep(random.uniform(0.09, 0.15))
        else:
            time.sleep(0.125)

        return self.submissions.qsize()

    def get_submission(self, queue_name):

        # /xqueue/get_submission/ typically finishes in under 200ms
        if self.simulation:
            time.sleep(random.uniform(0.1, 0.2))
        else:
            time.sleep(0.2)

        try:
            submission = self.submissions.get_nowait()
            statsd.incr('bux_grader_test_framework.submissions.received')
            return submission
        except Queue.Empty:
            return None

    def put_result(self, submission, result):

        # /xqueue/put_result/ typically finishes in under 400ms
        if self.simulation:
            time.sleep(random.uniform(0.2, 0.4))
        else:
            time.sleep(0.3)

        submission_id = submission['xqueue_header']['submission_id']

        pull_time = submission['xqueue_body']['student_info']['submission_time']
        put_time = time.time()
        response_time = int((put_time - pull_time)*1000.0)
        statsd.timing('bux_grader_test_framework.response_time', response_time)
        print "Response for %d received in %0.3f ms" % (
              submission_id, response_time)

        utc_pull = datetime.datetime.utcfromtimestamp(pull_time)
        utc_put = datetime.datetime.utcfromtimestamp(put_time)

        result = (
            self.count,
            self.rate,
            submission_id,
            response_time,
            utc_pull,
            utc_put,
            submission['xqueue_body']['student_response'],
            result['correct'],
            result['score'],
        )
        statsd.incr('bux_grader_test_framework.submissions.completed')
        self.submissions.task_done()
        self.results.put(result)
        return True

    def get_results(self):
        while self.results.qsize() > 0:
            yield self.results.get()
