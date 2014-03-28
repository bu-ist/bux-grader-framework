"""
    bux_grader_framework.grader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the central grader object.
"""

from .conf import Config


class Grader(object):
    """ The main grader class.

    >>> grader = Grader()
    >>> grader.config_from_module('yourconfig')
    >>> grader.run()
    """

    #: The configuration class utlized by this grader.
    #: Defaults to :class:`Config`
    config_class = Config

    def __init__(self):

        self._config = None

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
        pass

    @property
    def config(self):
        """ Holds the grader :class:`Config` object. """
        if self._config is None:
            self._config = self.config_class()
        return self._config

    @property
    def evaluators(self):
        """ Registry of :class:`BaseEvaluator` classes. """
        pass

    def _load_evaluators(self):
        """ Loads all evaluators. """
        pass
