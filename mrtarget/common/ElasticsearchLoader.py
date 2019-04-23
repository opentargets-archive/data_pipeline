import random
from collections import defaultdict
import time
import logging
import tempfile
import json
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.Settings import Config



class Loader():
    """
    Loads data to elasticsearch
    """

    def __init__(self,
                 es,
                 dry_run):

        self.logger = logging.getLogger(__name__)

        self.es = es
        self.dry_run = dry_run

    @staticmethod
    def get_versioned_index(index_name, check_custom_idxs=False):
        '''get a composed real name of the index

        If check_custom_idxs is set to True then it tries to get
        from ES_CUSTOM_IDXS_FILENAME config file. This config file
        is like this and no prefixes or versions will be appended

        [indexes]
        gene-data=new-gene-data-index-name

        if no index field or config file is found then a default
        composed index name will be returned
        '''
        if index_name.startswith(Config.RELEASE_VERSION+'_'):
            raise ValueError('Cannot add %s twice to index %s'
                             % (Config.RELEASE_VERSION, index_name))
        if index_name.startswith('!'):
            return index_name

        # quite tricky, isn't it? we do code HYPERfunctions
        # not mere functions you need to be reading this whole code
        # for a 5-dimensions space to get it in its full
        # why an asterisk? because the index name is really a string
        # to be parsed by elasticsearch as a multiindex shiny thing
        suffix = '*' if index_name.endswith('*') else ''
        raw_name = index_name[:-len(suffix)] if len(suffix) > 0 else index_name

        idx_name = Config.ES_CUSTOM_IDXS_INI.get('indexes', raw_name) \
            if check_custom_idxs and \
            Config.ES_CUSTOM_IDXS and \
            Config.ES_CUSTOM_IDXS_INI and \
            Config.ES_CUSTOM_IDXS_INI.has_option('indexes', raw_name) \
            else Config.RELEASE_VERSION + '_' + index_name

        return idx_name + suffix
