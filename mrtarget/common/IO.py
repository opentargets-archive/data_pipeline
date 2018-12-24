import functools
import more_itertools
import itertools
import gzip
import logging
import os.path
import glob
import requests as r
import requests_file

from mrtarget.common import URLZSource, urllify


_l = logging.getLogger(__name__)


def check_to_open(filename):
    """check if `filename` is a fetchable uri and returns True in the case is true False otherwise"""
    url_name = urllify(filename)
    r_session = r.Session()
    r_session.mount('file://', requests_file.FileAdapter())

    f = r_session.get(url_name, stream=True)
    is_ok = True

    _l.debug("check to open uri %s", url_name)
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
    open_f = functools.partial(gzip.open, filename, 'wb') if filename.endswith('.gz') \
        else functools.partial(open, filename, 'w')

    return open_f()


def open_to_read(filename):
    """return an iterator from izip (filename, (enumerate(file_handle, start=1))"""
    _l.debug('generate an iterator of (filename,enumerate) for filename %s', filename)
    it = more_itertools.with_iter(URLZSource(filename).open())
    return itertools.izip(itertools.cycle([filename]), enumerate(it, start=1))


def get_filenames_by_glob(path_str):
    filenames = []
    try:
        filenames = glob.glob(path_str)
    except Exception as e:
        _l.exception(e)
    finally:
        return filenames


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
