
class WorkQueue(object):
    """ Internal submission work queue.

    Thin abstraction around RabbitMQ.

    """
    def __init__(self, host, port, username, password, virtual_host):
        pass

    def connect(self):
        """ Establish connection to message broker """
        pass

    def get(self, queue_name):
        """ Pop a submission off the work queue """
        pass

    def put(self, queue_name, msg):
        """ Push a submission on to the work queue """
        pass

    def consume(self, queue_name):
        """ Poll a particular queue for submissions """
        pass
