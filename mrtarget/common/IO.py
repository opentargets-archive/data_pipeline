import functools
import more_itertools
import itertools
import gzip
import logging
import os.path
import requests as r
import requests_file

from opentargets_urlzsource import URLZSource
from mrtarget.common import urllify


_l = logging.getLogger(__name__)


def check_to_open(filename):
    """check if `filename` is a fetchable uri and returns True in the case is true False otherwise"""
    url_name = urllify(filename)
    with r.Session() as r_session:
        r_session.mount('file://', requests_file.FileAdapter())

        f = r_session.get(url_name, stream=True)
        is_ok = True

        #logging in child processess can lead to hung threads
        # see https://codewithoutrules.com/2018/09/04/python-multiprocessing/
        #_l.debug("check to open uri %s", url_name)
        try:
            f.raise_for_status()
        except Exception as e:
            _l.exception(e)
            is_ok = False
        finally:
            f.close()
            return is_ok


def open_to_write(filename):
    """open a filename checking if .gz or not at the end of the filename"""
    if filename.endswith('.gz'):
        return gzip.open(filename, 'wb')
    else:
        return open(filename,'w')


def open_to_read(filename):
    """return an iterator from izip (filename, (enumerate(file_handle, start=1))"""
    #logging in child processess can lead to hung threads
    # see https://codewithoutrules.com/2018/09/04/python-multiprocessing/
    #_l.debug('generate an iterator of (filename,enumerate) for filename %s', filename)
    it = more_itertools.with_iter(URLZSource(filename).open())
    return itertools.izip(itertools.cycle([filename]), enumerate(it, start=1))


def make_iter_lines(iterable_of_filenames, first_n=0):
    """return an iterator of lines for all filenames in `iterable_of_filenames`. It returns
    each element from the iterator is  in the shape of (filenae, (line_n, line)) starting
    from line_n = 1. If `first_n` is > 0 then only first n lines will be taken from the iter.
    """
    it = iter(iterable_of_filenames)

    in_handles = itertools.imap(open_to_read, it)

    _l.debug('create a iterable of lines from all file handles')
    it_lines = itertools.chain.from_iterable(itertools.ifilter(lambda e: e is not None, in_handles))

    return more_itertools.take(first_n, it_lines) \
        if first_n > 0 else it_lines
