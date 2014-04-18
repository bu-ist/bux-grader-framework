"""
    bux_grader_framework.queues
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines work queues utilized by the evalutator workers.
"""

import functools
import json
import logging
import time
import multiprocessing
import Queue
import urllib

import rabbitpy
import pika

from statsd import statsd

log = logging.getLogger(__name__)


class WorkQueue(object):
    """ Internal submission work queue.

    First pass is a thin wrapper around multiprocessing.Queue.

    Note that the "queue_name" parameter that will be used to route submissions
    to specific evaluators in the final version is ignored. (A single shared
    queue is used for simplicity).

    """
    queue = multiprocessing.Queue()

    def get(self, queue_name):
        """ Pop a submission off the work queue """
        try:
            submission = self.queue.get(False)
        except Queue.Empty:
            return None
        else:
            log.info(" < Popped submission #%d off of '%s' queue",
                     submission["xqueue_header"]["submission_id"], queue_name)
            return submission

    def put(self, queue_name, submission):
        """ Push a submission on to the work queue """
        log.info(" > Put result for submisson #%d to '%s' queue",
                 submission["xqueue_header"]["submission_id"], queue_name)
        self.queue.put(submission)

    def consume(self, queue_name, handler):
        """ Poll a particular queue for submissions """
        while True:
            submission = self.get(queue_name)
            if submission:
                handler(submission)
            else:
                time.sleep(1)


class RabbitMQueue(object):
    """ Internal submission work queue. Backed by RabbitMQ.

    Uses pika BlockingConnection.

    """
    def __init__(self, username='guest', password='guest', host='localhost',
                 port=5672, virtual_host='/'):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.virtual_host = virtual_host

        credentials = pika.PlainCredentials(self.username,
                                            self.password)
        self.params = pika.ConnectionParameters(host=self.host,
                                                port=self.port,
                                                virtual_host=self.virtual_host,
                                                credentials=credentials)
        self._connection = None
        self._channel = None

    def connect(self):
        """ Establish a connection to RabbitMQ """
        self._connection = pika.BlockingConnection(self.params)

    def get_channel(self):
        """ Returns a connection channel for RabbitMQ """
        if not self._connection or self._connection.is_closed:
            self.connect()

        if not self._channel or self._channel.is_closed:
            self._channel = self._connection.channel()

        return self._channel

    def put(self, queue_name, submission):
        """ Push a submission on to the work queue """
        submission_id = submission["submission"]["xqueue_header"]["submission_id"]
        log.info(" >> Enqueueing submisson #%d to '%s' queue",
                 submission_id, queue_name)

        channel = self.get_channel()
        channel.queue_declare(queue=queue_name, durable=True)

        properties = pika.BasicProperties(content_type='application/json',
                                          delivery_mode=2)

        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=json.dumps(submission),
                              properties=properties)
        statsd.incr('bux_grader_framework.submissions.put')

    def consume(self, queue_name, eval_callback):
        """ Poll a particular queue for submissions

            :param str queue_name: queue to consume requests from
            :param callable eval_callback: called when a submission is received

        """

        channel = self.get_channel()
        channel.queue_declare(queue=queue_name, durable=True)

        def on_message_received(ch, method, header, body):
            """ Hides the RabbitMQ mechanics from the evaluator workers """
            tag = method.delivery_tag
            log.info(" << Message %d consumed", tag)

            submission = json.loads(body)
            response = eval_callback(submission)
            if response:
                log.info(" * Message %d acknowledged!", tag)
                ch.basic_ack(delivery_tag=tag)
                statsd.incr('bux_grader_framework.submissions.success')
            else:
                log.error(" !! Message %d could not be evaluated: %s",
                          tag, submission)

                # TODO: Establish a procedure for recovering failed submissions
                ch.basic_nack(delivery_tag=tag, requeue=False)
                statsd.incr('bux_grader_framework.submissions.failure')

        channel.basic_consume(on_message_received, queue_name)

        try:
            channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            channel.stop_consuming()
            raise

    def sleep(self, duration):
        """ A wrapper around BlockingConnection.sleep()

        Use this instead of time.sleep() to prevent heartbeat_interval
        related timeouts.

        """
        self._connection.sleep(duration)

    def close(self):
        """ Close the RabbitMQ connection """
        if self._connection:
            self._connection.close()


class RabbitPyQueue(object):
    """ Internal submission work queue. Backed by RabbitMQ.

    Uses "rabbitpy" client.

    """
    def __init__(self, username='guest', password='guest', host='localhost',
                 port=5672, virtual_host='/'):
        virtual_host = urllib.quote(virtual_host, '')

        self.url = "amqp://{user}:{password}@{host}:{port}/{vhost}".format(
                   user=username,
                   password=password,
                   host=host,
                   port=port,
                   vhost=virtual_host)
        self._connection = None
        self._channel = None

    def connect(self):
        """ Establish a connection to RabbitMQ """
        self._connection = rabbitpy.Connection(self.url)
        self._channel = self._connection.channel()
        # self._channel.on_remote_close = self.on_remote_close

    def close(self):
        """ Close the RabbitMQ connection """
        self._connection.close()

    def on_remote_close(self, method):
        log.warning("RabbitMQ closed: %s", method)
        # Reconnect?
        # self.connect()

    def sleep(self, duration):
        time.sleep(duration)

    def put(self, queue_name, submission):
        """ Push a submission on to the work queue """
        if self._channel is None:
            self.connect()

        props = {'content_type': 'application/json'}
        message = rabbitpy.Message(self._channel, json.dumps(submission), props)
        message.publish('', queue_name)

    def consume(self, queue_name, eval_callback, prefetch):
        """ Poll a particular queue for submissions

            :param str queue_name: queue to consume requests from
            :param callable eval_callback: called when a submission is received
            :param int prefetch: queue prefetch count

        """
        with rabbitpy.Connection(self.url) as conn:
            with conn.channel() as channel:
                channel.prefetch_count(prefetch)
                queue = rabbitpy.Queue(channel, queue_name)
                queue.durable = True
                queue.declare()

                # Consume the message
                for message in queue.consume_messages():
                    tag = message.delivery_tag
                    log.info(" << Message %d consumed", tag)

                    submission = message.json()
                    callback = functools.partial(self.on_complete, message)
                    eval_callback(submission, callback)

    def on_complete(self, message, success):
        """ Responds to RabbitMQ after a submission is handled.

        This method is called to the evaluator thread, which will pass in
        the result of the submission handling.

        """
        if success:
            log.info(" >> Message %d ack'd", message.delivery_tag)
            message.ack()
            statsd.incr('bux_grader_framework.submissions.success')
        else:
            log.error(" >> Message %d nack'd", message.delivery_tag)
            message.nack()
            statsd.incr('bux_grader_framework.submissions.failure')
