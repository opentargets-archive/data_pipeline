import gzip
import urllib2
from io import BytesIO
import zipfile

from contextlib import contextmanager
from petl.errors import ArgumentError


class Actions():
    ALL='all'


class URLZSource():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')

        zf = None
        request = urllib2.Request(*self.args, **self.kwargs)
        f = urllib2.urlopen(request)
        buf = BytesIO(f.read())

        if self.args[0].endswith('.gz'):
            zf = gzip.GzipFile(fileobj=buf)
        elif self.args[0].endswith('.zip'):
            zipped_data = zipfile.ZipFile(buf)
            info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)
            zf = zipped_data.open(info)
        else:
            zf = buf

        try:
            yield zf
        finally:
            zf.close()
            f.close()
