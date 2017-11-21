from __future__ import absolute_import

from contextlib import contextmanager
import gzip
import zipfile
import logging
import petl as p
import io
import tempfile as tmp
import requests as r
import jsonschema as jss
import json
# import urllib2 as u2

_l = logging.getLogger(__name__)

def url_to_stream(url, *args, **kwargs):
    '''request a url using requests pkg and pass *args and **kwargs to
    requests.get function (useful for proxies) and returns the filled file
    descriptor from a tempfile.NamedTemporaryFile

    If you want to stream a raw uri (and not compressed) use the parameter
    `enable_stream=True`
    '''
    f = r.get(url, *args, stream=True, **kwargs)
    f.raise_for_status()

    if f.encoding is None:
        f.encoding = 'utf-8'

    for line in f.iter_lines(decode_unicode=True):
            yield line

    f.close()


@contextmanager
def url_to_tmpfile(url, delete=True, *args, **kwargs):
    '''request a url using requests pkg and pass *args and **kwargs to
    requests.get function (useful for proxies) and returns the filled file
    descriptor from a tempfile.NamedTemporaryFile
    '''
    f = None

    if url.startswith('ftp://'):
        raise NotImplementedError('finish ftp')

    elif url.startswith('file://') or ('://' not in url):
        filename = url[len('file://'):] if '://' in url else url
        with open(filename, mode="r+b") as f:
            yield f

    else:
        f = r.get(url, *args, stream=True, **kwargs)
        f.raise_for_status()

        with tmp.NamedTemporaryFile(mode='r+w+b', delete=delete) as fd:
            # write data into file in streaming fashion
            for block in f.iter_content(1024):
                fd.write(block)

            fd.seek(0)
            yield fd

        f.close()


class URLZSource(object):
    def __init__(self, *args, **kwargs):
        '''A source extension for petl python package
        Just in case you need to use proxies for url use it as normal
        named arguments

        >>> # proxies = {}
        >>> # if Config.HAS_PROXY:
        ...    # self.proxies = {"http": Config.PROXY,
        ...                      # "https": Config.PROXY}
        >>> # with URLZSource('http://var.foo/noname.csv',proxies=proxies).open() as f:
        >>> from __futures__ import absolute_import
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

        '''
        self.args = args
        self.kwargs = kwargs
        self.proxies = None

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise p.errors.ArgumentError('source is read-only')

        zf = None

        with url_to_tmpfile(*self.args, **self.kwargs) as f:
            buf = f

            if self.args[0].endswith('.gz'):
                zf = gzip.GzipFile(fileobj=buf)
            elif self.args[0].endswith('.zip'):
                zipped_data = zipfile.ZipFile(buf)
                info = zipped_data.getinfo(
                    zipped_data.filelist[0].orig_filename)
                zf = zipped_data.open(info)
            else:
                zf = buf

            yield zf
        zf.close()


def generate_validator_from_schema(schema_uri):
    '''load a uri, build and return a jsonschema validator'''
    with URLZSource(schema_uri).open() as r_file:
        js_schema = json.load(r_file)

    validator = jss.validators.validator_for(js_schema)
    return validator(schema=js_schema)


def generate_validators_from_schemas(schemas_map):
    '''return a dict of schema names and validators using the function
    `generate_validator_from_schema`'''
    validators = {}
    for schema_name, schema_uri in schemas_map.iteritems():
        # per kv we create the validator and instantiate it
        _l.info('generate_validator_from_schema %s using the uri %s',
                schema_name, schema_uri)
        validators[schema_name] = generate_validator_from_schema(schema_uri)

    return validators


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
        print 'ERROR require_all failed checking all predicates true'
        _l.error('require_all failed checking all predicates true')

def require_any(*predicates):
    r_any = any(predicates)
    if not r_any:
        _l.error('requre_any failed checking at least one predicate true')


class Actions():
    ALL = 'all'


# with thanks to: https://stackoverflow.com/questions/14897756/python-progress-bar-through-logging-module/41224909#41224909
class TqdmToLogger(io.StringIO):
    """
        Output stream for TQDM which will output to logger module instead of
        the StdOut.
    """
    logger = None
    level = None
    buf = ''
    def __init__(self,logger,level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO
    def write(self,buf):
        self.buf = buf.strip('\r\n\t ')
    def flush(self):
        self.logger.log(self.level, self.buf)
