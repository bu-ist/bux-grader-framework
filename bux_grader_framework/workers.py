"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""

import logging
import multiprocessing
import signal
import sys
import threading
import time

from multiprocessing.dummy import Pool

from string import Template
from statsd import statsd

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

        self._xqueue_pool_size = grader.config['XQUEUE_POOL_SIZE']
        self._poll_interval = grader.config['XQUEUE_POLL_INTERVAL']
        self._default_evaluator = grader.config['DEFAULT_EVALUATOR']

        # Shut down handling
        self._stop = multiprocessing.Event()
        signal.signal(signal.SIGTERM, self.on_sigterm)

        self.xqueue.login()

    def run(self):
        """ Polls XQueue for submissions. """
        log.info("XQueue worker (PID=%s) is polling for submissions...",
                 self.pid)

        self.queue.connect()
        self.pool = Pool(processes=self._xqueue_pool_size)

        try:
            while not self._stop.is_set():
                # Pop any pending submissions and transfer to work queue
                submit_count = self.xqueue.get_queuelen(self.queue_name)

                if submit_count:
                    self.get_submissions(submit_count)
                else:
                    # Sleep if no submissions are present
                    # Uses queue.sleep which pings RabbitMQ to prevent
                    # heartbeat_interval-related timeouts.
                    self.queue.sleep(self._poll_interval)
        except (KeyboardInterrupt, SystemExit):
            pass

        self.pool.close()
        self.pool.join()
        self.queue.close()

    def get_submissions(self, submit_count):
        """ Fetches submission from XQueue asynchronously """
        fetched = 0
        while fetched < submit_count:
            self.pool.apply_async(self.xqueue.get_submission,
                                  (self.queue_name,),
                                  callback=self.enqueue_submission)
            fetched += 1

    def enqueue_submission(self, submission):
        """ Adds a submision popped from XQueue to an internal work queue.

        This method is called by ``apply_async`` when an XQueue submission
        request has completed.

        """

        # Bail early if no submission was received
        if not submission:
            return

        received_time = time.time()
        submit_time = submission['xqueue_body']['student_info']['submission_time']

        # For load testing:
        #
        #   The load test suite uses timestamps for submission_time. The LMS uses
        #   a formatted date string. This block only logs submission delays when the
        #   former is being used as the date string resolution isn't high enough to
        #   be useful.
        #
        if isinstance(submit_time, float):
            delay = int((received_time - submit_time)*1000.0)
            log.info("Submitted: %d Received: %d Delay: %d", submit_time, received_time, delay)
            statsd.timing('bux_grader_framework.get_submission_delay', delay)

        frame = {"received_by": self.pid, "received_time": received_time}
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
            frame["submission"] = submission
            # Push to evaluator work queue
            self.queue.put(evaluator, frame)
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

    def close(self):
        """ Gracefully shuts down worker process """
        self._stop.set()

    def on_sigterm(self, signum, frame):
        """ Break out of run loop on SIGTERM """
        self.close()


class EvaluatorWorker(multiprocessing.Process):
    """ Evaluates submissions pulled from the internal work queue.

        :param str evaluator: name of the evaluator class for this worker
        :param Grader grader: a configured grader instance

        If the evaluation is successful, the result is posted to XQueue.

        If the grader is unable to process the submission the submission is
        requeued.

    """
    EVAL_THREAD_COUNT = 12

    def __init__(self, evaluator, grader):
        super(EvaluatorWorker, self).__init__()

        self.grader = grader

        self.evaluator = self.grader.evaluator(evaluator)
        self.xqueue = self.grader.xqueue()
        self.queue = self.grader.work_queue()

        # Attaches a callback handler for SIGTERM signals to
        # handle consumer canceling / connection closing
        signal.signal(signal.SIGTERM, self.on_sigterm)

    def run(self):
        """ Polls submission queue. """
        log.info("'%s' evaluator (PID=%s) awaiting submissions...",
                 self.evaluator.name, self.pid)

        self.queue.connect()
        self.pool = Pool(processes=self.EVAL_THREAD_COUNT)

        try:
            self.queue.consume(self.evaluator.name,
                               self.spawn_evaluator_thread,
                               self.EVAL_THREAD_COUNT)
        except (KeyboardInterrupt, SystemExit):
            pass

        self.pool.close()
        self.pool.join()
        self.queue.close()

    def spawn_evaluator_thread(self, frame, on_complete):
        """ Spawns a thread to handle received submissions """
        self.pool.apply_async(self.handle_submission, (frame,),
                              callback=on_complete)

    def handle_submission(self, frame):
        """ Handles a submission popped off the internal work queue.

        Invokes ``self.evaluator.evaluate()`` to generate a response.

        """
        submission = frame["submission"]
        submission_id = submission['xqueue_header']['submission_id']
        log.info("Evaluating submission #%d", submission_id)
        success = True

        try:
            with statsd.timer('bux_grader_framework.evaluate'):
                result = self.evaluator.evaluate(submission)
        except Exception:
            log.exception("Could not evaluate submission: %s", submission)
            success = False

        if success and result:
            try:
                success = self.xqueue.put_result(submission, result)
            except Exception:
                log.exception("Could not post reply to XQueue.")
                success = False

        elapsed_time = int((time.time() - frame["received_time"])*1000.0)
        statsd.timing('bux_grader_framework.total_time_spent', elapsed_time)

        return success

    def on_sigterm(self, signum, frame):
        """ Breaks out of run loop on SIGTERM """
        # Implicitly stops consumer by raising a SystemExit exception
        # in this process.
        # TODO: Better approach to consumer cancelation
        sys.exit()
