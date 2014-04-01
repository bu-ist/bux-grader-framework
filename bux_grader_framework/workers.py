"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""

import logging
import multiprocessing
import time

log = logging.getLogger(__name__)


class XQueueWorker(multiprocessing.Process):
    """ Polls XQueue for submissions.

        :param str queue_name: XQueue queue name
        :param Grader grader: a configured grader instance

        Submissions pulled down from XQueue are transferred to an internal work
        queue for evaluation by an :class:`EvaluatorWorker`.
    """

    def __init__(self, queue_name, grader):
        super(XQueueWorker, self).__init__()

        self.queue_name = queue_name
        self.grader = grader

        self.xqueue = grader.xqueue()
        self.queue = grader.work_queue()

        self._poll_interval = grader.config['XQUEUE_POLL_INTERVAL']
        self._is_running = False

    def run(self):
        """ Polls XQueue for submissions. """
        log.info("[%s] XQueue worker (PID=%s) is polling for submissions...",
                 self.name, self.pid)

        self.queue.connect()

        self._is_running = True
        try:
            while self._is_running:
                # Pop any pending submissions and transfer to work queue
                for submission in self.get_submissions():
                    self.enqueue_submission(submission)

                # Sleep once all submissions are transferred
                time.sleep(self._poll_interval)
        except (KeyboardInterrupt, SystemExit):
            self.close()

    def get_submissions(self):
        """ Submission generator """
        if self.xqueue.get_queuelen(self.queue_name):
            has_submissions = True
            while has_submissions:
                submission = self.xqueue.get_submission(self.queue_name)
                if submission:
                    yield submission
                else:
                    has_submissions = False

    def enqueue_submission(self, submission):
        """ Adds a submision popped from XQueue to an internal work queue. """
        payload = submission['xqueue_body']['grader_payload']
        evaluator = payload.get('evaluator', 'default')

        if evaluator:
            self.queue.put(evaluator, submission)

        # TODO: Handle case where "evaluator" is not set

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("[%s] Closing...", self.name)
        self._is_running = False
        self.queue.close()


class EvaluatorWorker(multiprocessing.Process):
    """ Evaluates submissions pulled from the internal work queue.

        :param evaluator: a :class:`BaseEvaluator` subclass for handling
                          submissions
        :param Grader grader: a configured grader instance

        If the evaluation is successful, the result is posted to XQueue.

        If the grader is unable to process the submission the submission is
        requeued.

    """
    def __init__(self, evaluator, grader):
        super(EvaluatorWorker, self).__init__()

        self.evaluator = evaluator()
        self.grader = grader

        self.xqueue = self.grader.xqueue()
        self.queue = self.grader.work_queue()

        self._is_running = False

    def run(self):
        """ Polls submission queue. """
        log.info("[%s] '%s' evaluator (PID=%s) awaiting submissions...",
                 self.name, self.evaluator.name, self.pid)

        self.queue.connect()

        self._is_running = True
        try:
            while self._is_running:
                self.queue.consume(self.evaluator.name,
                                   self.handle_submission)
        except (KeyboardInterrupt, SystemExit):
            self.close()

    def handle_submission(self, submission):
        """ Handles a submission popped off the internal work queue.

        Invokes ``self.evaluator.evaluate()`` to generate a response.

        """
        log.info("[%s] Evaluating submission #%s", self.name,
                 submission['xqueue_header']['submission_id'])

        try:
            result = self.evaluator.evaluate(submission)
        except Exception:
            log.exception("Could not evaluate submission: %s", submission)
            return False

        try:
            self.xqueue.put_result(submission, result)
        except Exception:
            log.exception("Could not post reply to XQueue.")
            return False

        return True

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("[%s] Closing...", self.name)
        self._is_running = False
        self.queue.close()
