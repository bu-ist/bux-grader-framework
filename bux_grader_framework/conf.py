"""
    bux_grader_framework.conf
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Defines grader configuration class.
"""

import logging
import importlib

log = logging.getLogger(__name__)


class Config(dict):
    """ A thin :class:`dict` wrapper customized for grader configuration. """

    def __init__(self, defaults={}):
        super(Config, self).__init__()

        # Set default attributes
        for key in defaults:
            self[key] = defaults[key]

    def from_module(self, modulename):
        """ Load configuration from a Python module.

        The loader pulls all uppercase attributes from the passed module.

        .. code-block:: python

            # settings.py:
            FOO = 'bar'
            bar = 'foo'

        Usage:

        .. code-block:: python

            >>> config = Config()
            >>> config.from_module('settings')
            True
            >>> config['FOO']
            'bar'
            >>> config['bar']
            KeyError: 'bar'

        .. note ::

            The module must be discoverable on ``sys.path``.
        """
        if not modulename:
            raise ValueError("Empty module name")

        mod = importlib.import_module(modulename)

        # Copy all uppercase attributes
        for attr in dir(mod):
            if attr.isupper():
                self[attr] = getattr(mod, attr)

        return True
