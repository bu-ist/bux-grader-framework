"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""

import logging
import multiprocessing
import threading
import time

from multiprocessing.dummy import Pool

from string import Template
from statsd import statsd

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
        self.queue = grader.producer()

        self._xqueue_pool_size = grader.config['XQUEUE_POOL_SIZE']
        self._poll_interval = grader.config['XQUEUE_POLL_INTERVAL']
        self._default_evaluator = grader.config['DEFAULT_EVALUATOR']

        # The underyling pika BlockingConnection used for the work queue is not
        # thread safe. We utilize a lock to prevent concurrent access while
        # fetching and enqueuing submissions.
        self._queue_lock = threading.Lock()

        # For stopping of the run loop from the main process
        self._stop = multiprocessing.Event()

    def run(self):
        """ Polls XQueue for submissions. """
        log.info("XQueue worker (PID=%s) is polling for submissions...",
                 self.pid)

        self.xqueue.login()
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
                    with self._queue_lock:
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
            lock_before = time.time()
            with self._queue_lock:
                lock_elapsed = int((time.time() - lock_before)*1000.0)
                statsd.timing('bux_grader_framework.xqueue_lock_wait', lock_elapsed)

                # Enqueue submission for EvaluationWorker
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

    def status(self):
        """ Returns whether or not XQueue / RabbitMQ are reachable. """

        # Sanity check of RabbitMQ connection
        try:
            self.queue.connect()
            self.queue.sleep(1)
            self.queue.close()
        except Exception:
            log.exception("XQueueWorker could not connect to RabbitMQ: ")
            return False

        try:
            status = self.xqueue.status()
        except XQueueException:
            return False

        return status

    def stop(self):
        """ Gracefully shuts down worker process """
        self._stop.set()


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
        self.queue = self.grader.consumer()

        self._eval_thread_count = self.grader.config['EVAL_THREAD_COUNT']
        self._eval_max_attempts = self.grader.config['EVAL_MAX_ATTEMPTS']
        self._eval_retry_delay = self.grader.config['EVAL_RETRY_DELAY']

        # For stopping of the run loop from the main process
        self._stop = multiprocessing.Event()

    def run(self):
        """ Polls submission queue. """
        log.info("Evaluator worker '%s' (PID=%s) awaiting submissions...",
                 self.evaluator.name, self.pid)

        # Start consuming in a separate thread
        consumer_thread = threading.Thread(target=self.queue.consume,
                                           args=(self.evaluator.name,
                                                 self.handle_submission,
                                                 self._eval_thread_count,
                                                 self._eval_max_attempts,
                                                 self._eval_retry_delay))
        consumer_thread.daemon = True
        consumer_thread.start()

        # Blocks until stop event is set by main grader process
        try:
            while not self._stop.is_set():

                # If the consumer thread dies unexpectedly we raise an
                # exception to trigger a restart by the main process.
                if not consumer_thread.is_alive():
                    raise Exception("Consumer thread died unexpectedly.")

                time.sleep(1)

        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            # Stop consumer thread by cancelling queue consumer and
            # closing RabbitMQ connection
            self.queue.stop()

        # Wait for consumer thread to exit cleanly
        consumer_thread.join()

    def handle_submission(self, frame, on_complete):
        """ Handles a submission popped off the internal work queue.

        Invokes ``self.evaluator.evaluate()`` to generate a response.

        """
        submission = frame["submission"]
        submission_id = submission['xqueue_header']['submission_id']
        success = True
        log.info("Evaluating submission #%d", submission_id)

        try:
            with statsd.timer('bux_grader_framework.evaluate'):
                result = self.evaluator.evaluate(submission)
        except Exception:
            log.exception("Could not evaluate submission: %s", submission)
            success = False

        # Note time spent in grader (between /xqueue/get_submission/ and
        # /xqueue/put_result/)
        elapsed_time = int((time.time() - frame["received_time"])*1000.0)
        statsd.timing('bux_grader_framework.total_time_spent', elapsed_time)
        log.info("Submission #%d evaluated in %0.3fms",
                 submission_id, elapsed_time)

        if success and result:
            try:
                success = self.xqueue.put_result(submission, result)
            except Exception:
                log.exception("Could not post reply to XQueue.")
                success = False

        # Notifies queue to ack / nack message
        on_complete(success)

    def status(self):
        """ Returns whether or not this evaluator is operational. """
        return self.evaluator.status()

    def stop(self):
        """ Gracefully shuts down worker process """
        self._stop.set()
