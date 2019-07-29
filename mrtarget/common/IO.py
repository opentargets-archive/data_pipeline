import functools
import more_itertools
import itertools
import gzip
import logging
import os.path
import requests as r
import requests_file
import mrtarget
import pkg_resources as res
from opentargets_urlzsource import URLZSource



def urllify(string_name):
    """return a file:// urlified simple path to a file:// is :// is not contained in it"""
    if '://' in string_name:
        return string_name
    else:
        return 'file://'+os.path.abspath(string_name)

def check_to_open(filename):
    """check if `filename` is a fetchable uri and returns True in the case is true False otherwise"""
    url_name = urllify(filename)
    with r.Session() as r_session:
        r_session.mount('file://', requests_file.FileAdapter())

        f = r_session.get(url_name, stream=True)
        is_ok = True

        try:
            f.raise_for_status()
        except Exception as e:
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
    it = more_itertools.with_iter(URLZSource(filename).open())
    return zip(itertools.cycle([filename]), enumerate(it, start=1))


def make_iter_lines(iterable_of_filenames, first_n=0):
    """return an iterator of lines for all filenames in `iterable_of_filenames`. It returns
    each element from the iterator is  in the shape of (filenae, (line_n, line)) starting
    from line_n = 1. If `first_n` is > 0 then only first n lines will be taken from the iter.
    """
    it = iter(iterable_of_filenames)

    in_handles = map(open_to_read, it)

    it_lines = itertools.chain.from_iterable(filter(lambda e: e is not None, in_handles))

    return more_itertools.take(first_n, it_lines) \
        if first_n > 0 else it_lines


def file_or_resource(fname):
    '''get filename and check if in getcwd then get from
    the package resources folder
    '''
    filename = os.path.expanduser(fname)

    resource_package = mrtarget.__name__
    resource_path = '/'.join(('resources', filename))

    if filename is not None:
        abs_filename = os.path.join(os.path.abspath(os.getcwd()), filename) \
                       if not os.path.isabs(filename) else filename

        return abs_filename if os.path.isfile(abs_filename) \
            else res.resource_filename(resource_package, resource_path)