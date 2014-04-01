"""
    bux_grader_framework.grader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the central grader object.
"""

import importlib
import logging
import time

from .conf import Config
from .evaluators import registered_evaluators
from .workers import EvaluatorWorker, XQueueWorker
from .exceptions import ImproperlyConfiguredGrader
from .util import class_imported_from

log = logging.getLogger(__name__)


class Grader(object):
    """ The main grader class.

    >>> grader = Grader()
    >>> grader.config_from_module('yourconfig')
    >>> grader.run()
    """

    #: The configuration class utlized by this grader.
    #: Defaults to :class:`Config`
    config_class = Config

    default_config = {
        "XQUEUE_QUEUE": "",
        "XQUEUE_URL": "http://localhost:18040",
        "XQUEUE_USER": "lms",
        "XQUEUE_PASSWORD": "password",
        "XQUEUE_TIMEOUT": 10,
        "XQUEUE_POLL_INTERVAL": 1,
        "WORKER_COUNT": 2,
        "MONITOR_INTERVAL": 1
    }

    def __init__(self):

        self._config = None
        self._evaluators = None

    def run(self):
        """ Starts the grader daemon

        1. Starts a :class:`XQueueWorker` process
        2. Starts a :class:`EvaluatorWorker` process for each evaluator
        3. Enters a monitoring loop to check on each process
        """
        self.workers = []

        # Create the XQueue worker
        log.info("Creating XQueue worker process...")
        xqueue_worker = XQueueWorker(self.config['XQUEUE_QUEUE'], self)
        self.workers.append(xqueue_worker)

        # Create an evaluator worker for each registered evaluator
        log.info("Creating evaluator worker processes...")
        for evaluator in self.evaluators:
            for num in range(self.config['WORKER_COUNT']):
                worker = EvaluatorWorker(evaluator, self)
                self.workers.append(worker)

        # Start all workers
        for worker in self.workers:
            log.info("Starting worker: %s", worker.name)
            worker.start()

        try:
            while self.workers:
                self.monitor()
                time.sleep(self.config['MONITOR_INTERVAL'])
        except (KeyboardInterrupt, SystemExit):
            self.stop()

        log.info("All workers removed, stopping...")

    def monitor(self):
        """ Monitors grader processes """
        for worker in self.workers:
            if worker.exitcode is None:
                continue
            else:
                log.error("Worker stopped unexpectedly: %s", worker)
                # TODO: Restart
                worker.close()
                self.workers.remove(worker)

    def stop(self):
        """ Shuts down all worker processes """
        log.info("Shutting down worker processes: %s", self.workers)
        for worker in self.workers:
            worker.close()
            worker.join()
        log.info("All workers stopped")

    def config_from_module(self, modulename):
        """ Loads grader configuration from a Python module.

        Forwards request to :class:`Config` instance.
        """
        try:
            self.config.from_module(modulename)
        except (ValueError, ImportError) as e:
            msg = "Could not load configuration module. {}".format(
                  e)
            raise ImproperlyConfiguredGrader(msg)

    def xqueue(self):
        """ Returns a fresh :class:`XQueueClient` instance configured for this grader.

            >>> xqueue = grader.xqueue()
            >>> submission = xqueue.get_submission('test_queue')
            >>> result = {"correct": True, "score": 1, "msg": "<p>Right!</p>"}
            >>> xqueue.put_result('test_queue', submission, result)

        """
        pass

    def work_queue(self):
        """ Returns a fresh :class:`WorkQueue` instance configured for this grader.

            >>> work_queue = grader.work_queue()
            >>> work_queue.get('test_queue')
            >>> work_queue.put('test_queue', 'message')
            >>> work_queue.consume('test_queue')

        """
        pass

    def evaluator(self, name):
        """ Returns an evaluator class by name """
        pass

    @property
    def config(self):
        """ Holds the grader :class:`Config` object. """
        if self._config is None:
            self._config = self.config_class(self.default_config)
        return self._config

    @property
    def evaluators(self):
        """ Registry of ``EVALUATOR_MODULES``

            :raises ImproperlyConfiguredGrader: if ``EVALUATOR_MODULES`` can
                                                not be imported properly
        """
        if self._evaluators is None:
            self._evaluators = self._load_evaluators()
        return self._evaluators

    def _load_evaluators(self):
        """ Loads all evaluators. """

        # Fail if no evaluators are defined
        if "EVALUATOR_MODULES" not in self.config:
            raise ImproperlyConfiguredGrader("No evaluators loaded. "
                                             "Have you set EVALUATOR_MODULES?")

        # Import configured evaluator modules
        modules = self.config["EVALUATOR_MODULES"]
        for mod in modules:
            try:
                importlib.import_module(mod)
            except ImportError:
                raise ImproperlyConfiguredGrader("Could not load evaluator "
                                                 "module: {}".format(mod))

        # Fetch evaluators registered by EVALUATOR_MODULES imports
        evaluator_classes = registered_evaluators()

        # Filter out classes that were not imported from EVALUATOR_MODULES
        evaluators = [cls for cls in evaluator_classes
                      if class_imported_from(cls, modules)]

        if not evaluators:
            evaluator_list = ", ".join(self.config["EVALUATOR_MODULES"])
            raise ImproperlyConfiguredGrader("No evaluators found in listed "
                                             "EVALUATOR_MODULES: {}"
                                             .format(evaluator_list))

        return evaluators
