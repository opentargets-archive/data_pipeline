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

def url_to_stream(url, *args, **kwargs):
    '''request a url using requests pkg and pass *args and **kwargs to
    requests.get function (useful for proxies) and returns the filled file
    descriptor from a tempfile.NamedTemporaryFile

    If you want to stream a raw uri (and not compressed) use the parameter
    `enable_stream=True`
    '''
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
        """A source extension for petl python package
        Just in case you need to use proxies for url use it as normal
        named arguments

        >>> # proxies = {}
        >>> # if Config.HAS_PROXY:
        ...    # self.proxies = {"http": Config.PROXY,
        ...                      # "https": Config.PROXY}
        >>> # with URLZSource('http://var.foo/noname.csv',proxies=proxies).open() as f:
        >>> from __future__ import absolute_import, print_function
        >>> import petl as p
        >>> t = p.fromcsv(mpetl.URLZSource('https://raw.githubusercontent.com/opentargets/mappings/master/expression_uberon_mapping.csv'), delimiter='|')
        >>> t.look()

        +------------------------+-------------------+
        | label                  | uberon_code       |
        +========================+===================+
        | u'stomach 2'           | u'UBERON_0000945' |
        +------------------------+-------------------+
        | u'stomach 1'           | u'UBERON_0000945' |
        +------------------------+-------------------+
        | u'mucosa of esophagus' | u'UBERON_0002469' |
        +------------------------+-------------------+
        | u'transverse colon'    | u'UBERON_0001157' |
        +------------------------+-------------------+
        | u'brain'               | u'UBERON_0000955' |
        +------------------------+-------------------+
        ...

        """
        self.filename = filename
        self.args = args
        self.kwargs = kwargs
        self.proxies = None
        self.r_session = r.Session()
        self.r_session.mount('file://', requests_file.FileAdapter())

    @contextmanager
    def _open_local(self, filename, mode):
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
        if self.filename.startswith('ftp://'):
            raise NotImplementedError('finish ftp')

        else:
            local_filename = self.filename.split('://')[-1].split('/')[-1]
            f = self.r_session.get(url=self.filename, stream=True, **self.kwargs)
            f.raise_for_status()
            file_to_open = None
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

