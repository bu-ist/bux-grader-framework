"""
    bux_grader_framework.conf
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Defines grader configuration class.
"""


class Config(dict):
    """ A thin :class:`dict` wrapper customized for grader configuration. """

    def from_module(self, modulename):
        """ Load configuration from Python module.

        The module must be discoverable on ``sys.path``.
        """
        pass
