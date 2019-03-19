from __future__ import absolute_import, print_function

import functools
from contextlib import contextmanager
import gzip
import zipfile
import logging
import tempfile as tmp
import requests as r
import requests_file
import os

_l = logging.getLogger(__name__)


def urllify(string_name):
    """return a file:// urlified simple path to a file:// is :// is not contained in it"""
    if '://' in string_name:
        return string_name
    else:
        return 'file://'+os.path.abspath(string_name)



def require_all(*predicates):
    r_all = all(predicates)
    if not r_all:
        print('ERROR require_all failed checking all predicates true')
        _l.error('require_all failed checking all predicates true')
