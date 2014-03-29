# Invalid configuration
#
# Loads an evaluator module with no BaseEvaluator subclasses defined

XQUEUE_INTERFACE = {
    "queue": "test_queue",
    "host": "localhost",
    "port": 18040,
    "username": "lms",
    "password": "password",
    "timeout": 10
}

ignoreme = "test"

EVALUATOR_MODULES = {
    'evaluators.empty'
}
