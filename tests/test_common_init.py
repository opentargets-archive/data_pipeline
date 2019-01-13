
import unittest

import mrtarget.common as common
from toolz import curry, take
from tempfile import NamedTemporaryFile
import os
from mrtarget.common import URLZSource
from toolz.functoolz import compose
from string import rstrip


class DataStructureTests(unittest.TestCase):

    def test_urlzsource(self):
        lines4 = []
        with URLZSource('http://www.google.com/robots.txt').open() as f:
            take_and_rstrip = compose(curry(map,
                                            lambda l: rstrip(l, '\n')),
                                      curry(take, 4))
            lines4 = list(take_and_rstrip(f))

        print(str(lines4))
        self.assertGreaterEqual(len(lines4), 1,
                              "Failed to get more than 0 lines")
