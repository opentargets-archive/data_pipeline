import json
from tqdm import tqdm

from common import Actions
import gzip, time, logging

from settings import Config
from opentargets import OpenTargetsClient

class DumpActions(Actions):
    DUMP='dump'


class DumpGenerator(object):

    def __init__(self, api_url = Config.DUMP_REMOTE_API):

        self.api_url = api_url
        self.client = OpenTargetsClient(auth_app_name=Config.DUMP_REMOTE_API_APPNAME, auth_secret=Config.DUMP_REMOTE_API_SECRET)


    def dump(self):


        '''dump evidence data'''
        logging.info('Dumping evidence data')
        all_evidence = ot.filter_evidence()
        with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as evidence_dump_file:
            for evidence in tqdm(all_evidence,
                                desc = 'Dumping evidence data'):
                evidence_dump_file.write(json.dumps(evidence, separators=(',', ':')) + '\n')

        '''dump association data'''
        logging.info('Dumping association data')
        all_association = ot.filter_association()
        with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as association_dump_file:
            for association in tqdm(all_association,
                                 desc='Dumping association data'):
                association_dump_file.write(json.dumps(association, separators=(',', ':')) + '\n')



