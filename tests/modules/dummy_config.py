# Valid configuration
#
# Loads the DummyEvaluator class

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
    'evaluators.dummy'
}
