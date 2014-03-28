"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""


class EvaluatorWorker(object):
    """ Evaluates submissions pulled from the internal work queue.

        If the evaluation is successful, the result is posted to XQueue.

        If the grader is unable to process the submission the submission is
        requeued.

        :param evaluator: a :class:`BaseEvaluator` subclass to use for problem
                          evaluations.
    """
    def __init__(self, evaluator):
        self.evaluator = evaluator

    def run(self):
        """ Polls submission queue. """
        pass

    def handle_submission(self, submission):
        """ Handles a submission popped off the internal work queue.

        Invokes ``self.evaluator.evalute()`` to generate a response.
        """
        pass


class XQueueWorker(object):
    """ Polls XQueue for submissions.

        :param XQueueClient xqueue: a xqueue REST client

        Submissions pulled down from XQueue are transferred to an internal work
        queue for evaluation by an :class:`EvaluatorWorker`.
    """

    def __init__(self, grader):
        pass

    def poll_for_submissions(self):
        """ Polls XQueue for submissions. """
        pass

    def enqueue_submission(self, submission):
        """ Adds a submision popped from XQueue to an internal work queue. """
        pass
