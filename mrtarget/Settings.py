import uuid
from collections import defaultdict
import os
import re
import json
import ConfigParser
import pkg_resources as res
from envparse import env, ConfigurationError
import mrtarget
import multiprocessing as mp
import logging

logger = logging.getLogger(__name__)


def ini_from_file_or_resource(*filenames):
    '''load the ini files using file_or_resource an
    return the configuration object or None
    '''
    f = [file_or_resource(fname) for fname in filenames if fname]
    cfg = ConfigParser.ConfigParser()
    if cfg.read(f):
        # read() returns list of successfully parsed filenames
        return cfg
    else:
        # the function return none in case no file was found
        return None


def file_or_resource(fname=None):
    '''get filename and check if in getcwd then get from
    the package resources folder
    '''
    filename = os.path.expanduser(fname)

    resource_package = mrtarget.__name__
    resource_path = '/'.join(('resources', filename))

    if filename is not None:
        abs_filename = os.path.join(os.path.abspath(os.getcwd()), filename) \
                       if not os.path.isabs(filename) else filename

        return abs_filename if os.path.isfile(abs_filename) \
            else res.resource_filename(resource_package, resource_path)


# loading all ini files into the same configuration
ini = ini_from_file_or_resource('db.ini',
                                'es_custom_idxs.ini')


def read_option(option, cast=None, ini=ini, section='dev',
                **kwargs):
    '''helper method to read value from environmental variable and ini files, in
    that order. Relies on envparse and accepts its parameters.
    The goal is to have ENV var > ini files > defaults

    Lists and dict in the ini file are parsed as JSON strings.
    '''
    # if passing 'default' as parameter, we don't want envparse to return
    # succesfully without first check if there is anything in the ini file
    try:
        default_value = kwargs.pop('default')
    except KeyError:
        default_value = None

    try:
        # reading the environment variable with envparse
        return env(option, cast=cast, **kwargs)
    except ConfigurationError:
        if not ini:
            return default_value

        try:
            # TODO: go through all sections available
            if cast is bool:
                return ini.getboolean(section, option)
            elif cast is int:
                return ini.getint(section, option)
            elif cast is float:
                return ini.getint(section, option)
            elif cast is dict or cast is list:
                # if you want list and dict variables in the ini file,
                # this function will accept json formatted lists.
                return json.loads(ini.get(section, option))
            else:
                return ini.get(section, option)

        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default_value

class Config():

    RELEASE_VERSION = read_option('CTTV_DATA_VERSION', default='')    

    # This config file is like this and no prefixes or version will be
    # appended
    #
    # [indexes]
    # gene-data=new-gene-data-index-name
    # ...
    #
    # if no index field or config file is found then a default
    # composed index name will be returned
    ES_CUSTOM_IDXS = read_option('CTTV_ES_CUSTOM_IDXS',
                                 default=False, cast=bool)
    ES_CUSTOM_IDXS_INI = ini if ES_CUSTOM_IDXS else None
    