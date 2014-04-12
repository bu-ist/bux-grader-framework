"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""

import logging
import multiprocessing
import time

from string import Template

from .exceptions import XQueueException

log = logging.getLogger(__name__)


FAIL_RESPONSE = Template("""
<div>
<p>The external grader was unable to process this submission.
Please contact course staff.</p>
<p><strong>Reason:</strong></p>
<pre><code>$reason</code></pre>
</div>
""")


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
        self._default_evaluator = grader.config['DEFAULT_EVALUATOR']
        self.exit_signal = multiprocessing.Event()

    def run(self):
        """ Polls XQueue for submissions. """
        log.info("XQueue worker (PID=%s) is polling for submissions...",
                 self.pid)

        self.queue.connect()

        try:
            while not self.exit_signal.is_set():
                # Pop any pending submissions and transfer to work queue
                for submission in self.get_submissions():
                    self.enqueue_submission(submission)

                # Sleep once all submissions are transferred
                # Uses queue.sleep which pings RabbitMQ to prevent
                # heartbeat_interval-related timeouts.
                self.queue.sleep(self._poll_interval)
        except (KeyboardInterrupt, SystemExit):
            pass

        self.queue.close()

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
        header = submission['xqueue_header']
        payload = submission['xqueue_body']['grader_payload']

        log.info("Submission #%d received from XQueue", header['submission_id'])

        # Assert the grader payload is a dict
        if not isinstance(payload, dict):
            self.push_failure("Grader payload could not be parsed", submission)
            return False

        # Determine which evaluator queue the submission should be routed to
        evaluator = payload.get('evaluator', self._default_evaluator)

        # Tempory back compat for edge grader
        if not evaluator:
            evaluator = payload.get('grader')

        # Ensure evaluator is registered with this grader
        if evaluator and self.grader.is_registered_evaluator(evaluator):
            # Push to evaluator work queue
            self.queue.put(evaluator, submission)
        else:
            # Notify LMS that the submission could not be handled
            self.push_failure("Evaluator could not be found: {}".format(
                              evaluator), submission)

    def push_failure(self, reason, submission):
        """ Sends a failing response to XQueue

            :param str reason: the reason for the failure
            :param dict submission: the submission that could not be handled

        """
        submit_id = submission['xqueue_header']['submission_id']
        response = {
            "correct": False,
            "score": 0,
            "msg": FAIL_RESPONSE.substitute(reason=reason)
        }
        log.error("Could not handle submission #%d: %s", submit_id, reason)
        self.xqueue.put_result(submission, response)

    def status(self):
        log.info("Checking XQueue status...")
        try:
            status = self.xqueue.status()
        except XQueueException:
            return False
        return status

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("Closing...")
        self.exit_signal.set()


class EvaluatorWorker(multiprocessing.Process):
    """ Evaluates submissions pulled from the internal work queue.

        :param str evaluator: name of the evaluator class for this worker
        :param Grader grader: a configured grader instance

        If the evaluation is successful, the result is posted to XQueue.

        If the grader is unable to process the submission the submission is
        requeued.

    """
    def __init__(self, evaluator, grader):
        super(EvaluatorWorker, self).__init__()

        self.grader = grader

        self.evaluator = self.grader.evaluator(evaluator)
        self.xqueue = self.grader.xqueue()
        self.queue = self.grader.work_queue()

        self._is_running = False

    def run(self):
        """ Polls submission queue. """
        log.info("'%s' evaluator (PID=%s) awaiting submissions...",
                 self.evaluator.name, self.pid)

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
        time_start = time.time()

        submission_id = submission['xqueue_header']['submission_id']
        log.info("Evaluating submission #%d", submission_id)

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

        time_stop = time.time()
        elapsed_time = (time_stop - time_start)*1000.0
        log.info("Submission #%d evaluated in %0.3fms",
                 submission_id, elapsed_time)

        return True

    def status(self):
        log.info("Checking Evaluator status...")
        return self.evaluator.status()

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("Closing...")
        self._is_running = False
        self.queue.close()
