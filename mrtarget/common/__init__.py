from __future__ import absolute_import, print_function

import functools
from contextlib import contextmanager
import gzip
import zipfile
import logging
import tempfile as tmp
import requests as r
import requests_file

_l = logging.getLogger(__name__)


def urllify(string_name):
    """return a file:// urlified simple path to a file:// is :// is not contained in it"""
    return string_name if '://' in string_name else 'file://' + string_name


def url_to_stream(url, *args, **kwargs):
    """request a url using requests pkg and pass *args and **kwargs to
    requests.get function (useful for proxies) and returns the filled file
    descriptor from a tempfile.NamedTemporaryFile

    If you want to stream a raw uri (and not compressed) use the parameter
    `enable_stream=True`
    """
    url_name = urllify(url)
    r_session = r.Session()
    r_session.mount('file://', requests_file.FileAdapter())

    f = r_session.get(url, *args, stream=True, **kwargs)
    f.raise_for_status()

    if f.encoding is None:
        f.encoding = 'utf-8'

    for line in f.iter_lines(decode_unicode=True):
            yield line

    f.close()


class URLZSource(object):
    def __init__(self, filename, *args, **kwargs):
        """Easy way to open multiple types of URL protocol (e.g. http:// and file://)
        as well as handling compressed content (e.g. .gz or .zip) if appropriate.

        Just in case you need to use proxies for url use it as normal
        named arguments to requests.

        >>> # proxies = {}
        >>> # if Config.HAS_PROXY:
        ...    # self.proxies = {"http": Config.PROXY,
        ...                      # "https": Config.PROXY}
        >>> # with URLZSource('http://var.foo/noname.csv',proxies=proxies).open() as f:

        """
        self._log = logging.getLogger(__name__)
        self.filename = urllify(filename)
        self.args = args
        self.kwargs = kwargs
        self.proxies = None
        self.r_session = r.Session()
        self.r_session.mount('file://', requests_file.FileAdapter())

    @contextmanager
    def _open_local(self, filename, mode):
        """
        This is an internal function to handle opening the temporary file 
        that the URL has been downloaded to, including handling compression
        if appropriate
        """
        open_f = None

        if filename.endswith('.gz'):
            open_f = functools.partial(gzip.open, mode='rb')

        elif filename.endswith('.zip'):
            zipped_data = zipfile.ZipFile(filename)
            info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)

            filename = info
            open_f = functools.partial(zipped_data.open)
        else:
            open_f = functools.partial(open, mode='r')

        with open_f(filename) as fd:
            yield fd

    @contextmanager
    def open(self, mode='r'):
        """
        This downloads the URL to a temporary file, naming the file
        based on the URL. 
        """

        if self.filename.startswith('ftp://'):
            self._log.error('Not implemented ftp protocol')
            NotImplementedError('finish ftp')

        else:
            local_filename = self.filename.split('://')[-1].split('/')[-1]
            f = self.r_session.get(url=self.filename, stream=True, **self.kwargs)
            f.raise_for_status()
            file_to_open = None
            #this has to be "delete=false" so that it can be re-opened with the same filename
            #to be read out again
            with tmp.NamedTemporaryFile(mode='wb', suffix=local_filename, delete=False) as fd:
                # write data into file in streaming fashion
                file_to_open = fd.name
                for block in f.iter_content(1024):
                    fd.write(block)

            with self._open_local(file_to_open, mode) as fd:
                yield fd


class LogAccum(object):
    def __init__(self, logger_o, elem_limit=1024):
        if not logger_o:
            raise TypeError('logger_o cannot have None value')

        self._logger = logger_o
        self._accum = {'counter': 0}
        self._limit = elem_limit

    def _flush(self, force=False):
        if force or self._accum['counter'] >= self._limit:
            keys = set(self._accum.iterkeys()) - set(['counter'])

            for k in keys:
                for msg in self._accum[k]:
                    self._logger.log(k, msg[0], *msg[1])

                # python indentation playing truth or dare
                del self._accum[k][:]

            # reset the accum
            del(self._accum)
            self._accum = {'counter': 0}

    def flush(self, force=True):
        self._flush(force)

    def log(self, level, message, *args):
        if level in self._accum:
            self._accum[level].append((message, args))
        else:
            self._accum[level] = [(message, args)]

        self._accum['counter'] += 1
        self._flush()

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush(True)


def require_all(*predicates):
    r_all = all(predicates)
    if not r_all:
        print('ERROR require_all failed checking all predicates true')
        _l.error('require_all failed checking all predicates true')


def require_any(*predicates):
    r_any = any(predicates)
    if not r_any:
        _l.error('requre_any failed checking at least one predicate true')

