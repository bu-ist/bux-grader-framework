import logging
import os
import sys

logging.basicConfig(level=logging.INFO)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

# Add modules dir to path
MODULES_DIR = 'modules'
sys.path.insert(0, os.path.join(TESTS_DIR, MODULES_DIR))
