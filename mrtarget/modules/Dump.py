import json
from tqdm import tqdm

from mrtarget.common import Actions
import gzip, time, logging

from settings import Config
from opentargets import OpenTargetsClient

class DumpActions(Actions):
    DUMP='dump'


class DumpGenerator(object):

    def __init__(self):

        self.client = OpenTargetsClient(host=Config.DUMP_REMOTE_API,
                                        port=Config.DUMP_REMOTE_API_PORT,
                                        auth_app_name=Config.DUMP_REMOTE_API_APPNAME,
                                        auth_secret=Config.DUMP_REMOTE_API_SECRET)


    def dump(self):

        '''dump evidence data'''
        logging.info('Dumping association data')
        a = self.client.filter_associations(size=10000, )
        a.to_file(Config.DUMP_FILE_ASSOCIATION, progress_bar=True)
        logging.info('Dumping evidence data')
        ev = self.client.filter_evidence(size=10000)
        ev.to_file(Config.DUMP_FILE_EVIDENCE, progress_bar=True)
