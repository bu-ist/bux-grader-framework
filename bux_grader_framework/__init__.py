"""
    bux_grader_framework
    ~~~~~~~~~~~~~~~~~~~~

    A framework for bootstraping of external graders for your edX course.

    :copyright: 2014 Boston University
    :license: GNU Affero General Public License
"""

__version__ = '0.1.0'

from .conf import Config
from .evaluators import BaseEvaluator
from .grader import Grader
from .workers import EvaluatorWorker, XQueueWorker
from .xqueue import XQueueClient
