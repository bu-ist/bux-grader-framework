.. _api:

Developer Interface
===================

.. module:: bux_grader_framework

Covers all the interfaces of the external grader framework.

Grader
------
.. autoclass:: bux_grader_framework.Grader
   :members:

Configuration
-------------
.. autoclass:: bux_grader_framework.Config
   :members:

Evaluator
---------
.. autoclass:: bux_grader_framework.BaseEvaluator
   :members:

Workers
-------
.. autoclass:: bux_grader_framework.XQueueWorker
   :members:
.. autoclass:: bux_grader_framework.EvaluatorWorker
   :members:

XQueue
------

.. autoclass:: bux_grader_framework.XQueueClient
   :members:

Exceptions
----------

XQueue
^^^^^^

.. autoexception:: bux_grader_framework.exceptions.XQueueException
.. autoexception:: bux_grader_framework.exceptions.ConnectionTimeout
.. autoexception:: bux_grader_framework.exceptions.BadQueueName
.. autoexception:: bux_grader_framework.exceptions.BadCredentials
.. autoexception:: bux_grader_framework.exceptions.InvalidReply
