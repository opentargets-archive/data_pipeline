from __future__ import absolute_import

import tempfile as tmp
import requests as r
from contextlib import contextmanager


@contextmanager
def url_to_tmpfile(url, *args, **kwargs):
    '''request a url using requests pkg and pass *args and
    **kwargs to requests.get function (useful for proxies)
    and returns the filled file descriptor from a
    tempfile.NamedTemporaryFile
    '''
    f = None

    f = r.get(url, *args, stream=True, **kwargs)
    f.raise_for_status()

    with tmp.NamedTemporaryFile(mode='rw+b', delete=True) as fd:
        # write data into file in streaming fashion
        for block in f.iter_content(1024):
            fd.write(block)

        fd.seek(0)
        try:
            yield fd
        finally:
            fd.close()
            f.close()
