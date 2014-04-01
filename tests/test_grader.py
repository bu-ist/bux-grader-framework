import unittest

from bux_grader_framework.grader import Grader
from bux_grader_framework.conf import Config
from bux_grader_framework.exceptions import ImproperlyConfiguredGrader


class TestGrader(unittest.TestCase):

    def setUp(self):
        self.grader = Grader()

    def test_init(self):
        self.assertIsInstance(self.grader, Grader)

    def test_grader_config(self):

        # Assert default config class
        self.assertTrue(hasattr(self.grader, 'config_class'))
        self.assertIs(self.grader.config_class, Config)

        # Assert config instantiated on property accesss
        self.assertTrue(hasattr(self.grader, 'config'))
        self.assertIsInstance(self.grader.config, self.grader.config_class)

    def test_grader_config_from_module(self):
        result = self.grader.config_from_module('dummy_config')
        self.assertTrue(result)
        self.assertIn('XQUEUE_QUEUE', self.grader.config)

    def test_evaluators(self):
        self.grader.config_from_module('dummy_config')
        self.assertEquals(len(self.grader.evaluators), 1)

    def test_load_evaluators_missing(self):
        self.grader.config_from_module('missing_config')
        self.assertRaises(ImproperlyConfiguredGrader,
                          self.grader._load_evaluators)

    def test_load_evaluators_empty(self):
        self.grader.config_from_module('empty_config')
        self.assertRaises(ImproperlyConfiguredGrader,
                          self.grader._load_evaluators)

    def test_load_evaluators_invalid(self):
        self.grader.config_from_module('invalid_config')
        self.assertRaises(ImproperlyConfiguredGrader,
                          self.grader._load_evaluators)

if __name__ == '__main__':
    unittest.main()
