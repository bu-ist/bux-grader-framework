"""
    bux_grader_framework.grader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the central grader object.
"""


class Grader(object):
    """ The main grader class.

    >>> grader = Grader()
    >>> grader.config_from_module('yourconfig')
    >>> grader.run()
    """
    def __init__(self):
        pass

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
        pass

    @property
    def evaluators(self):
        """ Registry of :class:`BaseEvaluator` classes. """
        pass

    def _load_config(self):
        """ Creates grader configuration object. """
        pass

    def _load_evaluators(self):
        """ Loads all evaluators. """
        pass
