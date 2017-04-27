"""
Copyright 2014-2016 EMBL - European Bioinformatics Institute, Wellcome
Trust Sanger Institute, GlaxoSmithKline and Biogen

This software was developed as part of Open Targets. For more information please see:

	http://targetvalidation.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

.. module:: Downloader
    :platform: Unix, Linux
    :synopsis: A data pipeline module to download data.
.. moduleauthor:: Gautier Koscielny <gautierk@opentargets.org>
"""
import logging
import socket

import requests
import re
import os
import socks
import ftplib
from mrtarget.Settings import Config


__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

class Downloader(object):

    def __init__(self):
        self.opener = None
        self.proxies = None
        if Config.HAS_PROXY:
            self.proxies = { "http": Config.PROXY, "https" : Config.PROXY }

    def get_resource(self, url, directory, filename=None):

        debug_level = 2
        txt = None
        print re.match("^([^:]+)://([^/]+)/(.+)$", url).groups()[0]
        (protocol, server_site, path) = re.match("^([^:]+)://([^/]+)(.+)$", url).groups()
        #print "%s %s %s"%(protocol, server_site, path)
        try:
            if protocol == 'ftp':
                connection = None
                ftp_user = 'anonymous'
                ftp_password = 'ot@targetvalidation.org'
                (ftp_path, ftp_file) = re.match("(.+)/([^/]+)$", path).groups()
                if self.proxies:
                    # http://socksipy.sourceforge.net/readme.txt
                    # PySocks or SocksiPy-branch==1.01
                    #https://gist.github.com/Motoma/594590
                    # -e https://sourceforge.net/projects/socksipy/files/socksipy/SocksiPy%201.00/SocksiPy.zip/download#eggSocksipy
                    # http://stackoverflow.com/questions/15160922/how-can-i-unwrap-a-module-with-socksipy
                    socks.setdefaultproxy(
                        socks.PROXY_TYPE_HTTP,
                        Config.PROXY_HOST,
                        Config.PROXY_PORT,
                        True,
                        Config.PROXY_USERNAME,
                        Config.PROXY_PASSWORD)
                    #socket.socket = socks.socksocket
                    socks.wrapmodule(ftplib)
                    connection = ftplib.FTP(server_site)
                    connection.set_debuglevel( debug_level )
                    #connection.login( Config.PROXY_USERNAME, Config.PROXY_PASSWORD )
                    connection.login( ftp_user, ftp_password )
                else:
                    connection = ftplib.FTP(server_site)
                    connection.set_debuglevel( debug_level )
                    connection.login( ftp_user, ftp_password )

                local_file = open(os.path.join(directory, filename), "wb")
                connection.cwd(ftp_path)
                connection.retrbinary('RETR %s'%ftp_file, local_file.write)
                local_file.close()
                connection.quit()

            else:
                if self.proxies:
                    response = requests.get(url, proxies=self.proxies, stream=True)
                else:
                    response = requests.get(url, stream=True)

                response.raise_for_status()

                if filename is not None:
                    # Open our local file for writing
                    local_file = open(os.path.join(directory, filename), "wb")
                    #Write to our loca l file
                    for block in response.iter_content(1024):
                        local_file.write(block)
                    local_file.close()
                    logging.info("downloaded %s"%filename)

                else:
                    txt = response.text
                return txt
                #handle errors
        except requests.exceptions.RequestException as e:
            print "Request Error:",e.message

    # Install a proxy, by changing the method socket.socket()
    def setup_http_proxy(proxy_host, proxy_port) :

        # New socket constructor that returns a ProxySock, wrapping a real socket
        def socket_proxy(af, socktype, proto) :

            # Create a socket, old school :
            sock = socket.socket_formal(af, socktype, proto)

            # Wrap it within a proxy socket
            return ProxySock(
                    sock,
                    proxy_host,
                    proxy_port)

        # Replace the "socket" method by our custom one
        socket.socket_formal = socket.socket
        socket.socket = socket_proxy
