"""
    bux_grader_framework
    ~~~~~~~~~~~~~~~~~~~~

    A framework for bootstraping of external graders for your edX course.

    :copyright: 2014 Boston University
    :license: GNU Affero General Public License
"""

__version__ = '0.1.0'

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

from .conf import Config
from .evaluators import registered_evaluators, BaseEvaluator
from .grader import Grader
from .workers import EvaluatorWorker, XQueueWorker
from .xqueue import XQueueClient

import statsd
statsd.Connection.set_defaults(host='10.0.2.2', port=8125, sample_rate=1, disabled=False)
