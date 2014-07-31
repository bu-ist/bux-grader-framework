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

from .queues import eval_queue_name, dl_queue_name
from .exceptions import XQueueException
from .util import safe_multi_call

log = logging.getLogger(__name__)


FAIL_RESPONSE = Template("""
<div>
<p>The external grader encountered an unexpected issue and is unable to process your submission at this time.</p>
$reason
<p>If this is a graded problem please notify course staff through the discussion forums to get your attempts reset.</p>
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
        payload = submission['xqueue_body'].get('grader_payload', {})

        log.info("Submission #%d received from XQueue", header['submission_id'])

        # Assert the grader payload is a dict
        if not isinstance(payload, dict):
            message = FAIL_RESPONSE.substitute(reason="<pre><code>Grader payload could not be parsed</code></pre>")
            safe_multi_call(self.xqueue.push_failure,
                            args=(message, submission),
                            max_attempts=5,
                            delay=5)
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
                queue_name = eval_queue_name(evaluator)
                self.queue.put(queue_name, frame)
        else:
            # Notify LMS that the submission could not be handled
            message = FAIL_RESPONSE.substitute(reason="<pre><code>Evaluator could not be found: {}</code></pre>".format(evaluator))
            safe_multi_call(self.xqueue.push_failure,
                            args=(message, submission),
                            max_attempts=5,
                            delay=5)

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

        # Make sure /xqueue/status is responding and that we can log in succesfully
        try:
            status = self.xqueue.status()
            self.xqueue.login()
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

        queue_name = eval_queue_name(self.evaluator.name)

        # Start consuming in a separate thread
        consumer_thread = threading.Thread(target=self.queue.consume,
                                           args=(queue_name,
                                                 self.handle_submission,
                                                 self._eval_thread_count))
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

        with statsd.timer('bux_grader_framework.evaluate'):
            result, success = safe_multi_call(self.evaluator.evaluate,
                                              args=(submission,),
                                              max_attempts=self._eval_max_attempts,
                                              delay=self._eval_retry_delay)

        # Note time spent in grader (between /xqueue/get_submission/ and
        # /xqueue/put_result/)
        elapsed_time = int((time.time() - frame["received_time"])*1000.0)
        statsd.timing('bux_grader_framework.total_time_spent', elapsed_time)
        log.info("Submission #%d evaluated in %0.3fms",
                 submission_id, elapsed_time)

        # Post response to XQueue
        if not success or not result:
            reason = "<pre><code>Submission could not be evaluated in %d attempts. Please try again later.</code></pre>" % (
                     self._eval_max_attempts)
            message = FAIL_RESPONSE.substitute(reason=reason)
            result, success = safe_multi_call(self.xqueue.push_failure,
                                              args=(message, submission),
                                              max_attempts=5,
                                              delay=5)

        else:
            result, success = safe_multi_call(self.xqueue.put_result,
                                              args=(submission, result),
                                              max_attempts=5,
                                              delay=5)

        if success:
            statsd.incr('bux_grader_framework.submissions.success')
        else:
            statsd.incr('bux_grader_framework.submissions.failure')

        # Notifies queue to ack / nack message
        on_complete(success)

    def status(self):
        """ Returns whether or not this evaluator is operational. """
        return self.evaluator.status()

    def stop(self):
        """ Gracefully shuts down worker process """
        self._stop.set()


class DeadLetterWorker(multiprocessing.Process):
    """ Consumes failed submissions from the evaluator dead letter queues.

        :param str evaluator: name of the evaluator
        :param Grader grader: a configured grader instance

        Submissions will end up in this queue if they are left in the
        `ready` state for more than 10 seconds, or a EvaluatorWorker dies
        unexpectedly while processing a submission.

        This ensures that students are notified if the grader is unable
        to process their submission, and protects the grader from
        potential outages due to repeat evaluation attempts for bad
        submissions.

    """
    def __init__(self, evaluator, grader):
        super(DeadLetterWorker, self).__init__()

        self.grader = grader
        self.evaluator = self.grader.evaluator(evaluator)

        self.xqueue = self.grader.xqueue()
        self.queue = self.grader.consumer()

        # Limits the number of submission this consumer pulls
        # at any given time.
        self.prefetch_count = 5

        # For stopping of the run loop from the main process.
        self._stop = multiprocessing.Event()

    def run(self):
        """ Polls evaluator dead letter queue. """
        queue_name = dl_queue_name(eval_queue_name(self.evaluator.name))

        log.info("Dead letter worker monitoring '%s' (PID=%s)...",
                 queue_name, self.pid)

        # Start consuming in a separate thread
        consumer_thread = threading.Thread(target=self.queue.consume,
                                           args=(queue_name,
                                                 self.handle_submission,
                                                 self.prefetch_count))
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
        """ Handles a submission popped off the dead letter queue.

        Pushes a failure response to XQueue to notify students of the issue.

        """
        submission = frame["submission"]
        submission_id = submission['xqueue_header']['submission_id']
        log.info("Pulled submission #%d off of dead letter queue", submission_id)
        statsd.incr('bux_grader_framework.submissions.dead_lettered')

        # Note time spent in grader
        elapsed_time = int((time.time() - frame["received_time"])*1000.0)
        statsd.timing('bux_grader_framework.total_time_spent', elapsed_time)
        log.info("Submission #%d evaluated in %0.3fms",
                 submission_id, elapsed_time)

        # Check evaluator for extra context to add to fail message.
        hints = ''
        if 'fail_hints' in dir(self.evaluator):
            hints = self.evaluator.fail_hints()

        # Post response to XQueue.
        message = FAIL_RESPONSE.substitute(reason=hints)
        result, success = safe_multi_call(self.xqueue.push_failure,
                                          args=(message, submission),
                                          max_attempts=5,
                                          delay=5)

        # Notifies queue to ack / nack message.
        on_complete(success)

    def status(self):
        """ Returns whether or not this evaluator is operational. """
        return True

    def stop(self):
        """ Gracefully shuts down worker process """
        self._stop.set()
