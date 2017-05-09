import gzip
import urllib2
from io import BytesIO
import zipfile
import requests as r

from contextlib import contextmanager
from petl.errors import ArgumentError

from mrtarget.Settings import Config


class URLZSource():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.proxies = None
        if Config.HAS_PROXY:
            self.proxies = {"http": Config.PROXY,
                            "https": Config.PROXY}

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')

        zf = None
        f = None

        if self.proxies:
            f = r.get(*self.args,
                      proxies=self.proxies,
                      stream=True, **self.kwargs)
        else:
            f = r.get(*self.args,
                      stream=True, **self.kwargs)

        f.raise_for_status()

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
