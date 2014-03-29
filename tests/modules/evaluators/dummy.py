from bux_grader_framework.evaluators import BaseEvaluator


class DummyEvaluator(BaseEvaluator):
    name = 'dummy'

    def evaluate(self, submission):
        return True
