import logging
import multiprocessing
import Queue
import time


log = logging.getLogger(__name__)


class XQueueStub(object):
    """ Mocks the XQueue interface, using multiprocessing.Queues
        to keep track of submissions / posted results.

    """
    def __init__(self):
        self.submissions = multiprocessing.JoinableQueue()
        self.results = multiprocessing.Queue()

    def login(self):
        return True

    def submit(self, submission):
        self.submissions.put(submission)

    def get_queuelen(self, queue_name):
        return self.submissions.qsize()

    def get_submission(self, queue_name):
        try:
            return self.submissions.get()
        except Queue.Empty:
            return None

    def put_result(self, submission, result):
        submission_id = submission['xqueue_header']['submission_id']

        pull_time = submission['xqueue_body']['submission_info']['submission_time']
        put_time = time.time()
        response_time = put_time - pull_time

        print "Response for %d received in %0.3f seconds" % (
              submission_id, response_time)

        result = {
            "response_time": response_time,
            "submission_id": submission_id,
            "submission": submission,
            "result": result
        }
        self.submissions.task_done()
        self.results.put(result)

    def get_results(self):
        while self.results.qsize() > 0:
            yield self.results.get()
