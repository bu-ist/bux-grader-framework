"""
    bux_grader_framework
    ~~~~~~~~~~~~~~~~~~~~

    A framework for bootstraping of external graders for your edX course.

    :copyright: 2014 Boston University
    :license: GNU Affero General Public License
"""

__version__ = '0.4.1'

DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s - %(levelname)s - %(processName)s - %(name)s  - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}

import os
os.environ['STATSD_HOST'] = os.environ.get('STATSD_HOST', '127.0.0.1')
os.environ['STATSD_PORT'] = os.environ.get('STATSD_PORT', '8125')

from .conf import Config
from .evaluators import registered_evaluators, BaseEvaluator
from .grader import Grader
from .workers import EvaluatorWorker, XQueueWorker
from .xqueue import XQueueClient
