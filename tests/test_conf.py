import unittest

import bux_grader_framework.conf as conf


class TestConfig(unittest.TestCase):

    def setUp(self):
        self.config = conf.Config()

    def test_from_module(self):
        result = self.config.from_module('dummy_config')

        self.assertTrue(result)
        self.assertIn('XQUEUE_QUEUE', self.config)
        self.assertEqual(self.config['XQUEUE_QUEUE'], 'test_queue')
        self.assertNotIn('ignoreme', self.config.keys())

    def test_from_module_raises_exception(self):
        self.assertRaises(ImportError, self.config.from_module, ('bad_config'))


if __name__ == '__main__':
    unittest.main()
