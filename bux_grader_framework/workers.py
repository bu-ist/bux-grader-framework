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

        self._poll_interval = grader.config['XQUEUE_POLL_INTERVAL']
        self._is_running = False

    def run(self):
        """ Polls XQueue for submissions. """
        log.info("[%s] XQueue worker (PID=%s) is polling for submissions...",
                 self.name, self.pid)
        self._is_running = True
        try:
            while self._is_running:
                time.sleep(self._poll_interval)
        except (KeyboardInterrupt, SystemExit):
            # Grader manages worker shutdown
            pass

    def enqueue_submission(self, submission):
        """ Adds a submision popped from XQueue to an internal work queue. """
        pass

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("[%s] Closing...", self.name)
        self._is_running = False


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

        self._is_running = False

    def run(self):
        """ Polls submission queue. """
        log.info("[%s] '%s' evaluator (PID=%s) awaiting submissions...",
                 self.name, self.evaluator.name, self.pid)
        self._is_running = True
        try:
            while self._is_running:
                # This will be handled by RabbitMQ consume method
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Grader manages worker shutdown
            pass

    def handle_submission(self, submission):
        """ Handles a submission popped off the internal work queue.

        Invokes ``self.evaluator.evalute()`` to generate a response.

        """
        pass

    def close(self):
        """ Gracefully shuts down worker process """
        log.info("[%s] Closing...", self.name)
        self._is_running = False
