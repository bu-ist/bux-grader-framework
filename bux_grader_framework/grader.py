"""
    bux_grader_framework.grader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the central grader object.
"""

import importlib
import logging
import logging.config
import multiprocessing
import sys
import time

from . import DEFAULT_LOGGING

from .conf import Config
from .evaluators import registered_evaluators
from .workers import EvaluatorWorker, XQueueWorker
from .exceptions import ImproperlyConfiguredGrader, XQueueException
from .xqueue import XQueueClient
from .queues import RabbitMQueue, AsyncConsumer
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
        "XQUEUE_MAX_RETRIES": 5,
        "XQUEUE_RETRY_INTERVAL": 5,
        "WORKER_COUNT": 2,
        "MONITOR_INTERVAL": 1,
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": 5672,
        "RABBITMQ_VHOST": "/",
        "DEFAULT_EVALUATOR": None
    }

    def __init__(self):

        self._config = None
        self._evaluators = None

        self._stop = multiprocessing.Event()

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

                # Calling the `stop` method will trigger this event
                if self._stop.is_set():
                    break
        except KeyboardInterrupt:
            pass
        finally:
            # Ensure any running workers are terminated gracefully
            # before exiting
            self.close()

    def monitor(self):
        """ Monitors grader processes """
        finished = []
        failed = []

        # Check status codes for all workers
        for worker in self.workers:
            exitcode = worker.exitcode

            # Process is running
            if exitcode is None:
                continue
            # Process has failed
            elif exitcode >= 1:
                failed.append(worker)
            # Process has finished (0) or been interrupted (<0)
            else:
                finished.append(worker)

        # Remove finished workers
        for worker in finished:
            log.info('Worker stopped: %s', worker.name)
            self.workers.remove(worker)

        # Restart failed workers
        for worker in failed:
            log.info('Worker failed: %s', worker.name)
            self.workers.remove(worker)

            # Create a new process using the same class / configuration
            if type(worker) == XQueueWorker:
                new_worker = self.restart_xqueue()
                if not new_worker:
                    sys.exit("Could not restart XQueue.")
            elif type(worker) == EvaluatorWorker:
                new_worker = EvaluatorWorker(worker.evaluator.name, self)

            if new_worker:
                self.workers.append(new_worker)
                log.info('Restarting worker: %s', new_worker.name)
                new_worker.start()
            else:
                log.error('Could not re-start worker: %s', worker.name)

    def restart_xqueue(self):
        """ Restarts XQueueWorker process on failure.

        Will try ``XQUEUE_MAX_RETRIES`` with ``XQUEUE_RETRY_INTERVAL``
        seconds between each attempt before giving up.

        """
        attempts = 0
        while attempts < self.config["XQUEUE_MAX_RETRIES"]:
            attempts += 1
            log.info("Restarting XQueueWorker (attempt %d of %d)",
                     attempts, self.config["XQUEUE_MAX_RETRIES"])

            try:
                worker = XQueueWorker(self.config['XQUEUE_QUEUE'], self)
            except XQueueException:
                log.exception("Restart failed, sleeping %d seconds...",
                              self.config["XQUEUE_RETRY_INTERVAL"])
                time.sleep(self.config["XQUEUE_RETRY_INTERVAL"])
            else:
                return worker
        else:
            log.critical("Failed to restart XQueueWorker after %d attempts",
                         attempts)
            return False
        return worker

    def stop(self):
        """ Sets a signal to break out of the `run` loop

        Utilizes a ``multiprocessing.Event`` to allow stopping
        grader from external process.

        """
        self._stop.set()

    def close(self):
        """ Shuts down all worker processes """
        log.info("Shutting down worker processes: %s", self.workers)
        for worker in self.workers:
            worker.terminate()
            worker.join()
        log.info("All workers stopped")

    def config_from_module(self, modulename):
        """ Loads grader configuration from a Python module.

        Forwards request to :class:`Config` instance.
        """
        try:
            result = self.config.from_module(modulename)
        except (ValueError, ImportError) as e:
            msg = "Could not load configuration module. {}".format(
                  e)
            raise ImproperlyConfiguredGrader(msg)

        return result

    def setup_logging(self):
        """ Sets up logging handlers

        Courses can define a ``LOGGING`` :class:`dict` in
        their settings module to customize.  It will be passed
        to ``logging.config.dictConfig``.

        If none is found, will fall back to the ``DEFAULT_LOGGING``
        :class:`dict` defined in __init__.py.

        """
        if "LOGGING" in self.config:
            logging.config.dictConfig(self.config['LOGGING'])
        else:
            logging.config.dictConfig(DEFAULT_LOGGING)

    def xqueue(self):
        """ Returns a fresh :class:`XQueueClient` instance configured for this grader.

            >>> xqueue = grader.xqueue()
            >>> submission = xqueue.get_submission('test_queue')
            >>> result = {"correct": True, "score": 1, "msg": "<p>Right!</p>"}
            >>> xqueue.put_result('test_queue', submission, result)

        """
        try:
            url = self.config['XQUEUE_URL']
            username = self.config['XQUEUE_USER']
            password = self.config['XQUEUE_PASSWORD']
            timeout = self.config['XQUEUE_TIMEOUT']
        except KeyError as e:
            raise ImproperlyConfiguredGrader(e)

        return XQueueClient(url, username, password, timeout)

    def work_queue(self):
        """ Returns a fresh :class:`WorkQueue` instance configured for this grader.

            >>> work_queue = grader.work_queue()
            >>> work_queue.get('test_queue')
            >>> work_queue.put('test_queue', 'message')
            >>> work_queue.consume('test_queue')

        """
        try:
            username = self.config['RABBITMQ_USER']
            password = self.config['RABBITMQ_PASSWORD']
            host = self.config['RABBITMQ_HOST']
            port = self.config['RABBITMQ_PORT']
            virtual_host = self.config['RABBITMQ_VHOST']
        except KeyError as e:
            raise ImproperlyConfiguredGrader(e)

        return RabbitMQueue(username, password, host, port, virtual_host)

    def queue_consumer(self):
        """ Returns a fresh :class:`WorkQueue` instance configured for this grader.

            >>> work_queue = grader.work_queue()
            >>> work_queue.get('test_queue')
            >>> work_queue.put('test_queue', 'message')
            >>> work_queue.consume('test_queue')

        """
        try:
            username = self.config['RABBITMQ_USER']
            password = self.config['RABBITMQ_PASSWORD']
            host = self.config['RABBITMQ_HOST']
            port = self.config['RABBITMQ_PORT']
            virtual_host = self.config['RABBITMQ_VHOST']
        except KeyError as e:
            raise ImproperlyConfiguredGrader(e)

        return AsyncConsumer(username, password, host, port, virtual_host)

    def evaluator(self, name):
        """ Returns a configured evaluator.

            :raises ImproperlyConfiguredGrader: if ``name`` is not a
                    registered evaluator.

        """
        if not self.is_registered_evaluator(name):
            raise ImproperlyConfiguredGrader

        eval_cls = self.get_evaluator_class(name)
        eval_config = self.get_evaluator_config(name)

        try:
            evaluator = eval_cls(**eval_config)
        except TypeError:
            log.exception("Could not create evaluator: %s", name)
            raise ImproperlyConfiguredGrader("Could not create evaluator: %s" % name)
        return evaluator

    def get_evaluator_class(self, name):
        """ Returns an evaluator class by name """
        if not self.is_registered_evaluator(name):
            return False

        return self.evaluators[name]

    def get_evaluator_config(self, name):
        """ Get evaluator config """
        if not self.is_registered_evaluator(name):
            return False

        return self.config['EVALUATOR_CONFIG'].get(name, {})

    def is_registered_evaluator(self, name):
        """ Returns whether or not an evaluator is registered by name. """
        return name in self.evaluators.keys()

    @property
    def config(self):
        """ Holds the grader :class:`Config` object. """
        if self._config is None:
            self._config = self.config_class(self.default_config)
        return self._config

    @property
    def evaluators(self):
        """ Registry of ``EVALUATOR_MODULES``.

            Returns a dict of evaluator classes keyed on
            their ``name`` attributes.  For instance:

            .. code-block:: python

                class MySQLEvaluator(BaseEvaluator):
                    name = 'mysql'

            Would produce:

            .. code-block:: python

                {'mysql': MySQLEvaluator}

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
        evaluators = dict((cls.name, cls) for cls in evaluator_classes
                          if class_imported_from(cls, modules))

        if not evaluators:
            evaluator_list = ", ".join(self.config["EVALUATOR_MODULES"])
            raise ImproperlyConfiguredGrader("No evaluators found in listed "
                                             "EVALUATOR_MODULES: {}"
                                             .format(evaluator_list))

        return evaluators
