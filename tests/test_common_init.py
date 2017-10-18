import json
import unittest

import mrtarget.common as common
from toolz import curry, take
from trace import find_lines_from_code
from tempfile import gettempdir, NamedTemporaryFile
import os
from mrtarget.common import URLZSource
from toolz.functoolz import compose
from string import rstrip


class DataStructureTests(unittest.TestCase):
    def test_url_to_stream(self):
        lines = ["User-agent: *",
                 "Disallow: /search",
                 "Allow: /search/about",
                 "Allow: /search/howsearchworks"]

        lines4 = \
            list(take(4, common.url_to_stream("http://www.google.com/robots.txt")))

        self.assertItemsEqual(lines, lines4,
                              "Failed to get the first 4 lines")

    def test_url_to_tempfile(self):
        file_exists = False

        with common.url_to_tmpfile("http://www.google.com/robots.txt") as f:
            file_exists = os.path.exists(f.name)

        self.assertTrue(file_exists == True,
                        'Failed to create a temporal file from http://')

        file_name = None
        with NamedTemporaryFile(mode='r+w+b', delete=False) as fd:
            file_name = fd.name
            fd.write('content')

        with common.url_to_tmpfile("file://" + file_name) as f:
            line = f.readline()

        self.assertTrue(line == 'content',
                        'Failed to create a temporal file from file://')

        os.remove(file_name)

    def test_urlzsource(self):
        lines4 = []
        with URLZSource('http://www.google.com/robots.txt').open() as f:
            take_and_rstrip = compose(curry(map,
                                            lambda l: rstrip(l, '\n')),
                                      curry(take, 4))
            lines4 = list(take_and_rstrip(f))

        lines = ["User-agent: *",
                 "Disallow: /search",
                 "Allow: /search/about",
                 "Allow: /search/howsearchworks"]

        print(str(lines4))
        self.assertItemsEqual(lines, lines4,
                              "Failed to get the first 4 lines")
