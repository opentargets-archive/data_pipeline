ftputil
=======

Purpose
-------

ftputil is a high-level FTP client library for the Python programming
language. ftputil implements a virtual file system for accessing FTP
servers, that is, it can generate file-like objects for remote files.
The library supports many functions similar to those in the os,
os.path and shutil modules. ftputil has convenience functions for
conditional uploads and downloads, and handles FTP clients and servers
in different timezones.

What's new?
-----------

Since version 3.3 the following changed:

- Fixed a bug where a 226 reply after a remote file close would only
  show up later when doing a `pwd` call on the session. [1] This
  resulted in an `ftplib.error_reply` exception when opening a remote
  file.

Note that ftputil 3.0 broke backward compatibility with ftputil 2.8
and before. The differences are described here:
http://ftputil.sschwarzer.net/trac/wiki/WhatsNewInFtputil3.0

Documentation
-------------

The documentation for ftputil can be found in the file ftputil.txt
(reStructuredText format) or ftputil.html (recommended, generated from
ftputil.txt).

Prerequisites
-------------

To use ftputil, you need Python, at least version 2.6. Python 3.x
versions work as well. Python is a programming language, available
from http://www.python.org for free.

Installation
------------

*If you have an older version of ftputil installed, delete it or
move it somewhere else, so that it doesn't conflict with the new
version!*

If you have pip or easy_install available, you can install the current
version of ftputil directly from the Python Package Index (PyPI)
without downloading the package explicitly. You'll still need an
internet connection, of course.

- Just type

    pip install ftputil
  
  or
  
    easy_install ftputil
  
  on the command line, respectively. Unless you're installing ftputil
  in a virtual environment, you'll probably need root/administrator
  privileges to do that.
  
  Done. :-)

If you don't have pip or easy_install, you need to download a tarball
from the Python Package Index or from the ftputil website and install
it:

- Unpack the archive file containing the distribution files. If you
  had an ftputil version 2.8, you would type at the shell prompt:

    tar xzf ftputil-2.8.tar.gz

- Make the directory to where the files were unpacked your current
  directory. Assume that after unpacking, you have a directory
  `ftputil-2.8`. Make it the current directory with

    cd ftputil-2.8

- Type

    python setup.py install

  at the shell prompt. On Unix/Linux, you have to be root to perform
  the installation. Likewise, you have to be logged in as
  administrator if you install on Windows.

  If you want to customize the installation paths, please read
  http://docs.python.org/inst/inst.html .

License
-------

ftputil is open source software. It is distributed under the
new/modified/revised BSD license (see
http://opensource.org/licenses/BSD-3-Clause ).

Authors
-------

Stefan Schwarzer <sschwarzer@sschwarzer.net>

Evan Prodromou <evan@bad.dynu.ca> (lrucache module)

(See also the file `doc/contributors.txt`.)

Please provide feedback! It's certainly appreciated. :-)

[1] http://ftputil.sschwarzer.net/trac/ticket/102
