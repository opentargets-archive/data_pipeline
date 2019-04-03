from __future__ import absolute_import, print_function

import os


def urllify(string_name):
    """return a file:// urlified simple path to a file:// is :// is not contained in it"""
    if '://' in string_name:
        return string_name
    else:
        return 'file://'+os.path.abspath(string_name)

