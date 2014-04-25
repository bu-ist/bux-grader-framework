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


class RabbitMQueue(object):
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


class AsyncConsumer(object):
    """ Internal submission work queue. Backed by RabbitMQ. """

    MAX_THREADS = 12

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
        self.connection = None
        self.channel = None
        self.submission_handler = None

    def connect(self):
        return pika.SelectConnection(self.params, self.on_connected)

    def on_connected(self, connection):
        # Register callback invoked when the connection is lost unexpectedly
        self.connection.add_on_close_callback(self.on_connection_closed)

        self.connection.channel(self.on_channel_open)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Invoked when the connection is closed unexpectedly."""

        log.warning('Connection closed, trying to reconnect: ({0}) {1}'.format(
            reply_code,
            reply_text,
        ))

        # Reconnect logic is odd with pika, and a simple self.connect() doesn't
        # seem to work here. So instead of that, we'll just throw an exception,
        # causing this worker process to end and the monitor will then spawn a
        # new process.
        raise Exception(
            "Reply code: {}; Reply text: {}".format(reply_code, reply_text)
        )

    def on_channel_open(self, channel):
        self.channel = channel

        # The prefetch count determines how many messages will be
        # fetched by pika for processing. New messages will only be
        # fetched after the ones that are being processed have been
        # acknowledged. Because we are using a thread to process each
        # message, the prefecth count also determines the maximum
        # number of processing threads running at the same time.
        channel.basic_qos(prefetch_count=self.prefetch_count)

        channel.queue_declare(queue=self.queue_name,
                              durable=True,
                              callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        self.channel.basic_consume(self.on_message_received, queue=self.queue_name)

    def consume(self, queue_name, submission_handler, prefetch_count):
        """ Main consumer method """
        # Queue to consume submissions from
        self.queue_name = queue_name

        # Callback method for handling submissions
        self.submission_handler = submission_handler

        # Limit incoming messages to this amount
        self.prefetch_count = prefetch_count

        log.info("Starting consumer for queue {queue}".format(
            queue=self.queue_name,
        ))

        try:
            self.connection = self.connect()
            self.connection.ioloop.start()
        except pika.exceptions.AMQPConnectionError:
            log.exception("Consumer for queue {queue} connection error".format(
                          queue=self.queue_name))
            raise
        else:
            # Log that the worker exited without an exception
            log.info("Consumer for queue {queue} is exiting normally...".format(
                     queue=self.queue_name))
        finally:
            # Log that the worker stopped
            log.info("Consumer for queue {queue} stopped".format(
                     queue=self.queue_name))

    def stop(self):
        """ Stop the worker from processing messages """
        self.connection.close()

    def on_message_received(self, channel, method, properties, body):
        tag = method.delivery_tag
        log.info(" << Message %d consumed", tag)
        frame = json.loads(body)
        on_complete = functools.partial(self.on_complete, channel, tag)

        # process the submission in a different thread to avoid
        # blocking pika's ioloop, which can cause disconnects and
        # other errors
        thread = threading.Thread(target=self.submission_handler,
                                  args=(frame, on_complete))
        thread.daemon = True
        thread.start()

    def on_complete(self, channel, tag, success):
        if success:
            log.info(" >> Message %d ack'd", tag)
            ack = lambda: channel.basic_ack(delivery_tag=tag)
            self.connection.add_timeout(0, ack)
            statsd.incr('bux_grader_framework.submissions.success')
        else:
            log.info(" >> Message %d nack'd", tag)
            nack = lambda: channel.basic_ack(delivery_tag=tag)
            self.connection.add_timeout(0, nack)
            statsd.incr('bux_grader_framework.submissions.failure')
