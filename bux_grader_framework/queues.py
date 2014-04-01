import logging
import time

import multiprocessing
import Queue

log = logging.getLogger(__name__)


class WorkQueue(object):
    """ Internal submission work queue.

    First pass is a thin wrapper around multiprocessing.Queue.

    Note that the "queue_name" parameter that will be used to route submissions
    to specific evaluators in the final version is ignored. (A single shared
    queue is used for simplicity).

    """
    queue = multiprocessing.Queue()

    def get(self, queue_name):
        """ Pop a submission off the work queue """
        try:
            submission = self.queue.get(False)
        except Queue.Empty:
            return None
        else:
            log.info(" < Popped submission #%d off of '%s' queue",
                     submission["xqueue_header"]["submission_id"], queue_name)
            return submission

    def put(self, queue_name, submission):
        """ Push a submission on to the work queue """
        log.info(" > Put result for submisson #%d to '%s' queue",
                 submission["xqueue_header"]["submission_id"], queue_name)
        self.queue.put(submission)

    def consume(self, queue_name, handler):
        """ Poll a particular queue for submissions """
        while True:
            submission = self.get(queue_name)
            if submission:
                handler(submission)
            else:
                time.sleep(1)
