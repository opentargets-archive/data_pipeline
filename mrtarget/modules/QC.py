'''Runs quality control queries on the database'''
import logging
from collections import Counter
from pprint import pprint

from elasticsearch import helpers
from tqdm import tqdm
from mrtarget.common import TqdmToLogger

from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)

class QCRunner(object):

    def __init__(self, es):

        self.es = es
        self.esquery = ESQuery(es)
        self._logger = logging.getLogger(__name__)

    def run_associationQC(self):
        self.run_evidence2associationQC()

    def run_evidence2associationQC(self):
        computed_assocations_ids = set(self.esquery.get_all_associations_ids())
        missing_assocations_ids = set()
        total_evidence = self.esquery.count_elements_in_index(Config.ELASTICSEARCH_DATA_INDEX_NAME+'*')
        logger.info('Starting to analyse %i evidence'%total_evidence)
        for as_id in tqdm(self.esquery.get_all_target_disease_pair_from_evidence(),
                           desc = 'checking t-d pairs in evidence data',
                           unit=' t-d pairs',
                           file=tqdm_out,
                           unit_scale=True,
                           total= total_evidence*5,# estimate t-d pairs
                           leave=True,
                           ):
            if as_id not in computed_assocations_ids:
                logger.error('Association id %s was not computed or stored'%as_id)
                missing_assocations_ids.add(as_id)

        if missing_assocations_ids:
            logger.error('%i associations not found'%len(missing_assocations_ids))
            logger.error('\n'.join(list(missing_assocations_ids)))
        else:
            logger.info('no missing annotation found')






