"""
    bux_grader_framework.grader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the central grader object.
"""

import importlib
import logging

from .conf import Config
from .evaluators import registered_evaluators
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
        pass

    def monitor(self):
        """ Monitors grader processes """
        pass

    def config_from_module(self, modulename):
        """ Loads grader configuration from a Python module.

        Forwards request to :class:`Config` instance.
        """
        return self.config.from_module(modulename)

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
