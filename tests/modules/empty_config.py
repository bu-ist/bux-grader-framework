# Invalid configuration
#
# Loads an evaluator module with no BaseEvaluator subclasses defined

XQUEUE_QUEUE = "test_queue"
XQUEUE_URL = "http://localhost:18040"
XQUEUE_USER = "lms"
XQUEUE_PASSWORD = "password"
XQUEUE_TIMEOUT = 10

RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "/"

WORKER_COUNT = 2

MONITOR_INTERVAL = 1

EVALUATOR_MODULES = {
    'evaluators.empty'
}
