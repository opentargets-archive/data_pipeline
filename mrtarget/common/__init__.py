from __future__ import absolute_import

from contextlib import contextmanager
import gzip
import zipfile
import petl as p
import tempfile as tmp
import requests as r
import jsonschema as jss
import json
# import urllib2 as u2


def url_to_stream(url, *args, **kwargs):
    '''request a url using requests pkg and pass *args and
    **kwargs to requests.get function (useful for proxies)
    and returns the filled file descriptor from a
    tempfile.NamedTemporaryFile

    If you want to stream a raw uri (and not compressed) use the
    parameter `enable_stream=True`
    '''
    f = r.get(url, *args, stream=True, **kwargs)
    f.raise_for_status()

    if f.encoding is None:
        f.encoding = 'utf-8'

    for line in f.iter_lines(decode_unicode=True):
            yield line

    f.close()


@contextmanager
def url_to_tmpfile(url, *args, **kwargs):
    '''request a url using requests pkg and pass *args and
    **kwargs to requests.get function (useful for proxies)
    and returns the filled file descriptor from a
    tempfile.NamedTemporaryFile
    '''
    f = None

    if url.startswith('ftp'):
        # TODO unfinished but not necessary at the moment
        # proxy_support = u2.ProxyHandler({})
        # opener = u2.build_opener(proxy_support)
        # req = u2.Request(url, *args, **kwargs)
        # f = opener.open(req)
        raise NotImplemented('finish ftp')
    else:
        f = r.get(url, *args, stream=True, **kwargs)
        f.raise_for_status()

    with tmp.NamedTemporaryFile(mode='r+w+b', delete=True) as fd:
        # write data into file in streaming fashion
        for block in f.iter_content(1024):
            fd.write(block)

        fd.seek(0)
        try:
            yield fd
        finally:
            fd.close()
            f.close()


class URLZSource():
    def __init__(self, *args, **kwargs):
        '''A source extension for petl python package
        Just in case you need to use proxies for url use it as normal
        named arguments

        >>> # proxies = {}
        >>> # if Config.HAS_PROXY:
        ...    # self.proxies = {"http": Config.PROXY,
        ...                      # "https": Config.PROXY}
        >>> # with URLZSource('http://var.foo/noname.csv',proxies=proxies) as f:
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


def generate_validator_from_schema(schema):
    return jss.validators.validator_for(schema=schema)


def generate_validators_from_schemas(schemas_map):
    validators = {}
    for k, v in schemas_map.iteritems():
        # per kv we create the validator and instantiate it
        with URLZSource(v).open() as r_file:
            js_schema = json.load(r_file)

        validator = jss.validators.validator_for(js_schema)
        validators[k] = validator(schema=js_schema)

    return validators


class Actions():
    ALL = 'all'
