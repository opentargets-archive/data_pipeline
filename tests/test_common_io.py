import unittest
from mrtarget.common.IO import check_to_open


class IOTests(unittest.TestCase):
    def test_check_to_open_false(self):
        filename = '/false/file'
        self.assertFalse(check_to_open(filename),'file does not exist so it must return false')

    def test_check_to_open_true(self):
        filename = 'https://www.google.com/robots.txt'
        self.assertTrue(check_to_open(filename),'google robots url must exist')
