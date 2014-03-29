"""
    bux_grader_framework.evaluators
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines the evaluator interface.
"""
import abc


class BaseEvaluator(object):
    """ A abstract base class for problem evaluators.

        Custom evaluators can be created by extending this class and
        implementing the ``evaluate`` method ``name`` property.

        Clients can then add the module which defines the custom evaluator to
        the EVALUATOR_MODULES list.

        Course authors can target specific evaluators by passing the
        evaluator ``name`` in the ``grader_payload``'s ``evaluator`` attribute.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def name(self):
        """ Subclasses must implement with a unique identifying string.

        This string will be used to target the evaluator by the course
        author in the problem defintion.
        """
        pass

    @abc.abstractmethod
    def evaluate(self, submission):
        """ Subclasses must implement with custom evaluation logic.

        The return value should be a :class:`dict` formatted as follows::

            {
                'correct': True|False,
                'score': 0-1,
                'msg': '<p>A custom response message</p>'
            }

        .. warning::

            If the ``msg`` value contains invalid XML the LMS will not accept
            the response.
        """
        pass


def registered_evaluators():
    """ Returns all subclasses of :class:`BaseEvaluator` """
    return BaseEvaluator.__subclasses__()
