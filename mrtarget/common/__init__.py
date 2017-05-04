import gzip
import urllib2
from io import StringIO, BytesIO
import zipfile
import requests

from contextlib import contextmanager
from petl.errors import ArgumentError


class Actions():
    ALL='all'


class URLGZSource():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')
        request = urllib2.Request(*self.args, **self.kwargs)
        request.add_header('Accept-encoding', 'gzip, deflate')
        f = urllib2.urlopen(request)
        buf = BytesIO(f.read())
        zf = gzip.GzipFile(fileobj=buf)
        try:
            yield zf
        finally:
            zf.close()
            f.close()


class URLZSource():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')

        request = urllib2.Request(*self.args, **self.kwargs)
        f = urllib2.urlopen(request)
        buf = BytesIO(f.read())
        zipped_data = zipfile.ZipFile(buf)
        info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)
        zf = zipped_data.open(info)
        try:
            yield zf
        finally:
            zf.close()
