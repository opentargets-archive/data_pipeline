import os
import random
import logging
import gzip

import functools
import itertools
import addict

_l = logging.getLogger(__name__)


class ProcessContext(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = addict.Dict(kwargs)
        self.logger = logging.getLogger(__name__ + '_' + str(os.getpid()))


def to_source_for_writing(filename):
    '''open a filename checking if .gz or not at the end of the filename'''
    return gzip.open(filename, mode='wb') \
            if filename.endswith('.gz') else open(filename, mode='w')


def from_source_for_reading(filename):
    '''return an iterator from izip (filename, (enumerate(file_handle))'''
    _l.debug('generate an iterator of (filename,enumerate) for filename %s', filename)
    if filename.endswith('.gz'):
        open_f = functools.partial(gzip.open, mode='rb')
    else:
        open_f = functools.partial(open, mode='r')

    f_handle = open_f(filename)
    return itertools.izip(itertools.cycle([filename]),enumerate(f_handle, start=1))


def reduce_tuple_with_sum(iterable):
    it = iter(iterable)
    return functools.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), it, (0, 0))
