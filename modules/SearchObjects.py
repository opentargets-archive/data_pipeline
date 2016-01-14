import logging
import random
import time
from redislite import Redis

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue, RedisQueueStatusReporter
from  multiprocessing import Process
from elasticsearch import Elasticsearch, helpers

from settings import Config

""" TEST RedisQueue

import time
from redislite import Redis
from common.Redis import RedisQueue

r_server = Redis()

q = RedisQueue(r_server=r_server, max_size=100)

print q.get_status()

'''submit jobs'''
for i in range(10):
    q.put(i)
print q.get_status()
q.set_submission_finished()

'''get a job'''
key, value = q.get()
q.done(key)
print q.get_status(), value, 'done'

'''get a job and signal as error'''
key, value = q.get()
q.done(key, error=True)
print q.get_status(), value, 'done'

'''get a job, timeout, and put it back '''
key, value = q.get()
time.sleep(7)
print q.get_status(), value, 'timed out'
q.put_back_timedout_jobs()
print q.get_status()

'''get more jobs than available'''
for i in range(10):
    data = q.get(timeout=5)
    if data:
        key, value = data
        q.done(key)
        print value, 'done'

print q.get_status()
print 'QUEUE IS DONE:', q.is_done()
q.close()
print q.get_status()
"""

class SearchObjectActions(Actions):
    PROCESS = 'process'

class SearchObjectTypes(object):
    TARGET = 'target'
    DISEASE = 'disease'
    GO_TERM = 'go'
    PROTEIN_FEATURE = 'protein_feature'
    PUBLICATION = 'pub'
    SNP = 'snp'
    GENERIC = 'generic'

class SearchObject(JSONSerializable):
    """ Base class for search objects
    """
    def __init__(self,
                 id,
                 title,
                 description,
                 ):
        self.id = id
        self.title = title
        self.description = description
        self.type = SearchObjectTypes.GENERIC
        self.private ={}
        self._create_suggestions()

    def set_associations(self, top_associations = [], total_associations = 0):
        self.top_associations = top_associations
        self.total_associations = total_associations

    def _create_suggestions(self):
        '''reimplement in subclasses to allow a better autocompletion'''

        field_order = [self.id,
                       self.title,
                       self.description,
                       ]

        self.private['suggestions'] = dict(input = [],
                                              output = self.title,
                                              payload = dict(id = self.id,
                                                             title = self.title,
                                                             description = self.description),
                                              )


        for field in field_order:
            if isinstance(field, list):
                self.private['suggestions']['input'].extend(field)
            else:
                self.private['suggestions']['input'].append(field)

        self.private['suggestions']['input'] = [x.lower() for x in self.private['suggestions']['input']]



class SearchObjectTarget(JSONSerializable):
    """
    Target search object
    """
    def __init__(self,
                 id,
                 title,
                 description,
                 approved_symbol,
                 approved_name,
                 symbol_synonyms,
                 name_synonyms,
                 biotype,
                 gene_family_description,
                 uniprot_accessions,
                 hgnc_id,
                 ensembl_gene_id,
                 ):
        super(SearchObjectTarget, self).__init__()
        self.type = SearchObjectTypes.TARGET
        self.approved_symbol = approved_symbol
        self.approved_name = approved_name
        self.symbol_synonyms = symbol_synonyms
        self.name_synonyms = name_synonyms
        self.biotype = biotype
        self.gene_family_description = gene_family_description
        self.uniprot_accessions = uniprot_accessions
        self.hgnc_id = hgnc_id
        self.ensembl_gene_id = ensembl_gene_id


class SearchObjectDisease(JSONSerializable):
    """
    Target search object
    """
    def __init__(self,
                 id,
                 title,
                 description,
                 efo_code,
                 efo_url,
                 efo_label,
                 efo_definition,
                 efo_synonyms,
                 efo_path_codes,
                 efo_path_labels
                 ):
        super(SearchObjectDisease, self).__init__()
        self.type = SearchObjectTypes.DISEASE
        self.efo_code = efo_code
        self.efo_url = efo_url
        self.efo_label = efo_label
        self.efo_definition = efo_definition
        self.efo_synonyms = efo_synonyms
        self.efo_path_codes = efo_path_codes
        self.efo_path_labels = efo_path_labels


class SearchObjectAnalyserWorker(Process):
    ''' read a list of objects from the processing queue and analyse them with the available association data before
    storing them in elasticsearch
    '''

    def __init__(self):
        super(SearchObjectAnalyserWorker, self).__init__()

    def run(self):

        while True:

            '''process objects to simple search object'''

            '''count associations '''

            '''store search objects'''
            pass





class SearchObjectProcess(object):
    def __init__(self,
                 adapter,
                 loader,
                 r_server):
        self.adapter = adapter
        self.session = adapter.session
        self.loader = loader
        self.esquery = ESQuery(loader.es)
        self.r_server = r_server

    def process_all(self):
        ''' process all the objects that needs to be returned by the search method
        :return:
        '''

        queue = RedisQueue(queue_id='search_obj_processing',
                           max_size=int(1e5))

        q_reporter = RedisQueueStatusReporter([queue])
        q_reporter.start()

        '''get gene simplified objects and push them to the processing queue'''
        for i,target in enumerate(self.esquery.get_all_targets()):
            target['search_type'] = SearchObjectTypes.TARGET
            queue.put(target, self.r_server)

        for i,disease in enumerate(self.esquery.get_all_diseases()):
            disease['search_type'] = SearchObjectTypes.DISEASE
            queue.put(disease, self.r_server)



        '''get disease objects  and push them to the processing queue'''

        while not queue.is_done(r_server=self.r_server):
            data = queue.get()
            if data is not None:
                key, value = data
                error = random.random()>0.92
                queue.done(key, error=error, r_server=self.r_server)

        logging.critical('ALL DONE!')