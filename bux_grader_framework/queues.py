"""
    bux_grader_framework.queues
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module defines work queues utilized by the evalutator workers.
"""

import functools
import json
import logging
import multiprocessing
import time
import threading
import Queue

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


class SubmissionProducer(object):
    """ Internal submission work queue. Backed by RabbitMQ. """
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


class SubmissionConsumer(object):
    """ Asynchronous consumer interface.

    Uses the Pika SelectConnection with threads for submission evaluation.

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

        # Callback method for submission handling
        self.submission_handler = None

        # Generate by pika on Basic.Consume
        self.consumer_tag = None

        self._connection = None
        self._channel = None
        self._closing = False

    def connect(self):
        """ Establish a RabbitMQ connection. """
        return pika.SelectConnection(self.params, self._on_connected)

    def _on_connected(self, connection):
        """ Called by Pika when the connection has been established.

        Establishes a RabbitMQ channel for consuming, attaches a
        callback to be fired when connection closes.

        """
        # Register callback invoked when the connection is lost unexpectedly
        self._connection.add_on_close_callback(self._on_connection_closed)

        # Open a new channel
        self._connection.channel(self._on_channel_open)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        """ Invoked when the connection is closed.

        If the close is expected, stop the ioloop.
        Otherwise raise an exception to force grader restart.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            # Connection was closed unexpectedly -- throw an exception to
            # force grader restart of evaluator worker.
            raise Exception("Pika connection closed unexpectedly: %s %s" % (
                            reply_code, reply_text))

    def _on_channel_open(self, channel):
        """ Called by Pika when the channel has been established.

        Sets prefetch limit and declares our consumer queue.

        """
        self._channel = channel

        channel.basic_qos(prefetch_count=self.prefetch_count)

        channel.queue_declare(queue=self.queue_name,
                              durable=True,
                              callback=self._on_queue_declared)

    def _on_queue_declared(self, frame):
        self.consumer_tag = self._channel.basic_consume(self._on_message_received,
                                                       queue=self.queue_name)

    def _on_message_received(self, channel, method, properties, body):
        """ Called by Pika when a queue message is received.

        Calls the submission handler in a thread to avoid blocking of the
        Pika ioloop.

        """
        tag = method.delivery_tag
        log.info(" << Message %d consumed", tag)
        frame = json.loads(body)

        # A function to be called by the submission handler when the
        # submission has been processed.
        on_complete = functools.partial(self._on_complete, channel, tag)

        # Handle submission in a separate thread to avoid blocking of ioloop.
        thread = threading.Thread(target=self.submission_handler,
                                  args=(frame, on_complete))
        thread.daemon = True
        thread.start()

    def _on_complete(self, channel, tag, success):
        """ Called by the submission handler when the submission has been handled.

        The submission handler indicates success or failure by passing a bool
        flag to this function.

        Note that the channel and tag parameters are filled in using
        ``functools.partial`` in the ``_on_message_received`` method.

        """
        if success:
            log.info(" >> Message %d ack'd", tag)
            ack = lambda: channel.basic_ack(delivery_tag=tag)
            self._connection.add_timeout(0, ack)
            statsd.incr('bux_grader_framework.submissions.success')
        else:
            log.info(" >> Message %d nack'd", tag)
            nack = lambda: channel.basic_nack(delivery_tag=tag, requeue=False)
            self._connection.add_timeout(0, nack)
            statsd.incr('bux_grader_framework.submissions.failure')

    def consume(self, queue_name, submission_handler, prefetch_count):
        """ Start consuming from the designated queue.

            :param str queue_name: queue to consume from
            :param callable submission_handler: submission handling callback
            :param int prefetch_count: maximum number of submissions allowed
                                       in queue.

        """
        # Queue to consume submissions from
        self.queue_name = queue_name

        # Callback method for handling submissions
        self.submission_handler = submission_handler

        # Blocks incoming submissions if more than this
        # amount is present in the queue.
        self.prefetch_count = prefetch_count

        log.info("Starting consumer for queue '{queue}'".format(
            queue=self.queue_name,
        ))

        try:
            self._connection = self.connect()
            self._connection.ioloop.start()
        except pika.exceptions.AMQPConnectionError:
            log.exception("Consumer for queue '{queue}' connection error".format(
                          queue=self.queue_name))
            raise
        else:
            # Log that the worker exited without an exception
            log.info("Consumer for queue '{queue}' is exiting normally...".format(
                     queue=self.queue_name))
        finally:
            # Log that the worker stopped
            log.info("Consumer for queue '{queue}' stopped".format(
                     queue=self.queue_name))

    def stop(self):
        """ Cancels queue consumer and closes the RabbitMQ connection. """

        # No connection, nothing to close
        if not self._connection:
            return

        self._closing = True
        if self._channel:
            # Cancel consumer and restart ioloop so it can receive
            # the cancel callback.
            self._channel.basic_cancel(self._on_cancelok, self.consumer_tag)
        self._connection.ioloop.start()

    def _on_cancelok(self, frame):
        """ Called by Pika when the consumer has been cancelled.

            Used to close the connection.

        """
        self._connection.close()
