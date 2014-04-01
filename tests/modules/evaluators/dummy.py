from bux_grader_framework.evaluators import BaseEvaluator


class DummyEvaluator(BaseEvaluator):
    name = 'dummy'

    def evaluate(self, submission):
        return {"correct": True, "score": 1, "msg": "<p>Nice Work!</p>"}
