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


def defauldict(param):
    pass


class QCRunner(object):

    def __init__(self, es):

        self.es = es
        self.esquery = ESQuery(es)
        self._logger = logging.getLogger(__name__)
        tqdm_out = TqdmToLogger(self._logger,level=logging.INFO)

        # self.run_associationQC()

    def run_associationQC(self):
        # self.run_association2evidenceQC()
        self.run_evidence2associationQC()
        # self.check_association_gene_info()
        # self.check_evidence_gene_info()

    def run_association2evidenceQC(self):
        c=0
        e=0
        for ass in tqdm(self.esquery.get_all_associations(),
                           desc = 'checking associations are computed on all the evidence',
                           unit=' associations',
                           file=tqdm_out,
                           unit_scale=True,
                           total= self.esquery.count_elements_in_index(Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                           leave=True,
                           ):

            c+=1
            if c %1000 == 0:
                logger.info('%i association processed, %i errors found'%(c, e))

            ass_ev_counts = ass['evidence_count']['datasources']
            target, disease = ass['target']['id'], ass['disease']['id']
            db_ev_counts = self.esquery.count_evidence_sourceIDs(target, disease )

            for ds in ass_ev_counts:
                correct = ass_ev_counts[ds]==db_ev_counts[ds]
                if not correct:
                    logger.error("evidence count mismatch for association %s for datasource %s. association object:%i, evidence objects available:%i"%(ass['id'],ds, ass_ev_counts[ds], db_ev_counts[ds]))
                    e+=1
                    break
        logger.info('%i association processed, %i errors found' % (c, e))
        logger.info('DONE')



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

    def check_association_gene_info(self):
        logger.info('checking target info in associations')

        c = 0
        e = 0
        id2symbol = dict()
        for ass in tqdm(self.esquery.get_all_associations(),
                        desc='checking target info is correct in all associations',
                        unit=' associations',
                        unit_scale=True,
                        file=tqdm_out,
                        total=self.esquery.count_elements_in_index(
                            Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME),
                        leave=True,
                        ):

            c += 1
            if c % 1000 == 0:
                logger.info('%i association processed, %i errors found' % (c, e))

            target, disease = ass['target']['id'], ass['disease']['id']
            target_symbol = ass['target']['gene_info']['symbol']

            if target not in id2symbol:
                id2symbol[target]=target_symbol
            else:
                if target_symbol != id2symbol[target]:
                    e+=1

        logger.info('%i association processed, %i errors found' % (c, e))
        logger.info('DONE')

    def check_evidence_gene_info(self):
        logger.info('checking target info in associations')

        c = 0
        e = 0
        id2symbol = dict()
        for ass in tqdm(self.esquery.get_all_evidence_for_datasource(),
                        desc='checking target info is correct in all evidence',
                        unit=' evidence',
                        unit_scale=True,
                        file=tqdm_out,
                        total=self.esquery.count_elements_in_index(
                            Config.ELASTICSEARCH_DATA_INDEX_NAME+'*'),
                        leave=True,
                        ):

            c += 1
            if c % 1000 == 0:
                logger.info('%i evidence processed, %i errors found' % (c, e))

            target, disease = ass['target']['id'], ass['disease']['id']
            target_symbol = ass['target']['gene_info']['symbol']

            if target not in id2symbol:
                id2symbol[target] = target_symbol
            else:
                if target_symbol != id2symbol[target]:
                    e += 1

        logger.info('%i evidence processed, %i errors found' % (c, e))
        logger.info('DONE')


    # def analyse_cancer_gene_census(self):
    #     c=0
    #     o=0
    #     for ev_hit in helpers.scan(client=self.es,
    #                                query={"query": {
    #                                    "filtered": {
    #                                        "filter": {
    #                                            "bool": {
    #                                                "must": [
    #                                                    {"terms": {"sourceID": ['cancer_gene_census']}},
    #                                                ]
    #                                            }
    #                                        }
    #                                    }
    #                                },
    #                                    '_source': True,
    #                                    'size': 100,
    #                                },
    #                                scroll='1h',
    #                                index=Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_INDEX_NAME + '*'),
    #                                timeout='10m',
    #                                ):
    #         c+=1
    #         if len(ev_hit['_source']['evidence']['known_mutations']) >1:
    #             o+=1
    #         else:
    #             data = ev_hit['_source']
    #             tot, mut =  float(data['evidence']['known_mutations'][0]['number_mutated_samples']), float(data['evidence']['known_mutations'][0]['number_samples_with_mutation_type'])
    #             print '%s\t%s\t%i\t%i\t%.3f'%(data['target']['gene_info']['symbol'],
    #                                           data['disease']['efo_info']['label'],
    #                                           tot,
    #                                           mut,
    #                                           mut/tot)
    #
    #     print c,o
    #
    #     return






