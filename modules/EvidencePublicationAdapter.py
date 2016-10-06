import multiprocessing
import time
import logging
from tqdm import tqdm
from settings import Config

from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue, RedisQueueWorkerProcess

from modules.Literature import PublicationFetcher, PublicationAnalyserSpacy

logger = logging.getLogger(__name__)
MAX_PUBLICATION_CHUNKS =1000


class EvidenceStringPublicationAdapter():
    def __init__(self,es,
                 loader,
                 r_server=None):
        self.es = es
        self.es_query = ESQuery(es)
        self.loader = loader
        self.r_server = r_server

    def process_evidence_string(self):
        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)
        pub_analyser = PublicationAnalyserSpacy(pub_fetcher, self.es, self.loader)

        # Literature Queue
        literature_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_analyzer_q',
                                  max_size=MAX_PUBLICATION_CHUNKS,
                                  job_timeout=120)

        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()
        logging.info("No of workers {} ".format(no_of_workers))

        # Start literature-analyser-worker processes
        processors = [EvidencePublicationProcess(literature_q,
                                               self.r_server.db,
                                               pub_fetcher,
                                               pub_analyser,
                                               self.loader,
                                               self.es,
                                               ) for i in range(no_of_workers)]

        for p in processors:
            p.start()

        get_evidence_page_size = 5000
        for row in tqdm(self.get_evidence(page_size = get_evidence_page_size, datasources='europepmc'),
                        desc='Reading available evidence_strings',
                        total = self.es_query.count_validated_evidence_strings(datasources= 'europepmc'),
                        unit=' evidence',
                        unit_scale=True):

            literature_q.put(row)

        for p in processors:
                p.join()

        logging.info('flushing data to index')

        self.loader.es.indices.flush(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                        wait_if_ongoing=True)
        self.loader.es.indices.flush(Config.ELASTICSEARCH_DATA_INDEX_NAME,
                                        wait_if_ongoing=True)
        logging.info("DONE")

    def get_evidence(self, page_size = 5000, datasources = []):

        c = 0
        for row in self.es_query.get_validated_evidence_strings(size=page_size, datasources = datasources):
            c += 1
            if c % page_size == 0:
                logger.info("loaded %i ev from db to process" % c)
            yield row
        logger.info("loaded %i ev from db to process"%c)

class EvidencePublicationProcess(RedisQueueWorkerProcess):
            def __init__(self,
                         queue_in,
                         redis_path,
                         pub_fetcher,
                         pub_analyser,
                         loader,
                         es=None,

                         ):
                super(EvidencePublicationProcess, self).__init__(queue_in, redis_path)
                self.queue_in = queue_in
                self.redis_path = redis_path
                self.es = es
                self.pub_fetcher = pub_fetcher
                self.pub_analyser = pub_analyser
                self.loader = loader
                self.start_time = time.time()
                self.audit = list()
                self.logger = logging.getLogger(__name__)

            def process(self, data):

                logging.info("In EvidencePublicationProcess- {} ".format(self.name))
                pmid_url = data['literature']['references'][0]['lit_id']
                pmid = self.get_pub_id_from_url(pmid_url)
                publications = self.pub_fetcher.get_publication(pmid)

                for pub_id, pub in publications.items():
                    spacy_analysed_pub = self.pub_analyser.analyse_publication(pub_id=pub_id,
                                                                               pub=pub)

                    self.loader.put(index_name=Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                    doc_type=spacy_analysed_pub.get_type(),
                                    ID=pub_id,
                                    body=spacy_analysed_pub.to_json(),
                                    parent=pub_id,
                                    )
                    logging.info("Updated Publication Data For PMID ".format(pub_id))
                    data['literature']['abstract'] = pub.abstract
                    data['literature']['title'] = pub.title
                    data['literature']['year'] = pub.year
                    data['literature']['journal'] = pub.journal
                    data['literature']['abstract_lemmas'] = spacy_analysed_pub.lemmas
                    self.loader.put(index_name="evidence-data-generic",
                                    doc_type="evidencestring-europepmc",
                                    ID=data['id'],
                                    body=data,
                                    routing=data['target']['id'],
                                    create_index=False
                                    )
                    logging.info("Update evidence for id  {} ".format(data['id']))
                    logging.info("Routing - {}".format(data['target']['id']))

            def get_pub_id_from_url(self,url):
                return url.split('/')[-1]



