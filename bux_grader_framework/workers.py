"""
    bux_grader_framework.worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines worker processes for handling submissions.
"""


class EvaluatorWorker(object):
    """ Evaluates submissions pulled from the internal work queue.

        :param str evaluator_name: a evaluator name to handle submissions
        :param Grader grader: a configured grader instance

        If the evaluation is successful, the result is posted to XQueue.

        If the grader is unable to process the submission the submission is
        requeued.

    """
    def __init__(self, evaluator_name, grader):
        pass

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

        :param str queue_name: XQueue queue name
        :param Grader grader: a configured grader instance

        Submissions pulled down from XQueue are transferred to an internal work
        queue for evaluation by an :class:`EvaluatorWorker`.
    """

    def __init__(self, queue_name, grader):
        pass

    def poll_for_submissions(self):
        """ Polls XQueue for submissions. """
        pass

    def enqueue_submission(self, submission):
        """ Adds a submision popped from XQueue to an internal work queue. """
        pass
