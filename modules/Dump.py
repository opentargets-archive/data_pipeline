import json
from tqdm import tqdm

from common import Actions
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
        logging.info('Dumping evidence data')
        all_evidence = self.client.filter_evidence(size=10000)
        with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as evidence_dump_file:
            for evidence in tqdm(all_evidence,
                                desc = 'Dumping evidence data'):
                evidence_dump_file.write(json.dumps(evidence, separators=(',', ':')) + '\n')

        '''dump association data'''
        logging.info('Dumping association data')
        all_association = self.client.filter_association(size=10000)
        with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as association_dump_file:
            for association in tqdm(all_association,
                                 desc='Dumping association data'):
                association_dump_file.write(json.dumps(association, separators=(',', ':')) + '\n')



