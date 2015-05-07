from collections import defaultdict
import logging
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import streaming_bulk
from settings import ElasticSearchConfiguration, Config

__author__ = 'andreap'


class Loader():
    """
    Loads data to elasticsearch
    """

    def __init__(self, es, chunk_size=1000):

        self.es = es
        self.cache = []
        self.results = defaultdict(list)
        self.chunk_size = chunk_size

    def put(self, index_name, doc_type, ID, body):

        self.cache.append(dict(_index = index_name,
                               _type = doc_type,
                               _id = ID,
                               _source = body))
        if (len(self.cache) % self.chunk_size) == 0:
            self.flush()

        # self.load_single(index_name, doc_type, ID, body)

    def flush(self):
        for ok, results in streaming_bulk(
            self.es,
            self.cache,
            chunk_size=self.chunk_size,
            request_timeout=120,
            ):
            action, result = results.popitem()
            self.results[result['_index']].append(result['_id'])
            doc_id = '/%s/%s' % (result['_index'], result['_id'])
            if (len(self.results[result['_index']]) % 1000) == 0:
                logging.info("%i entries processed for index %s" % (len(self.results[result['_index']]), result['_index']))
            if not ok:
                logging.error('Failed to %s document %s: %r' % (action, doc_id, result))
            else:
                pass
        self.cache = []


        # if index_name:
        #     self.cache[index_name] = []
        # else:
        #     for index_name in self.cache:
        #         self.cache[index_name] = []


    def close(self):

        self.flush()
    #
    # def load_single(self, index_name, doc_type, ID, body):
    #     self.results[index_name].append(
    #         self.es.index(index=index_name,
    #                       doc_type=doc_type,
    #                       id=ID,
    #                       body=body)
    #     )
    #     if (len(self.results[index_name]) % 1000) == 0:
    #         logging.info("%i entries processed for index %s" % (len(self.results[index_name]), index_name))

    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.flush()


    def create_new_index(self, index_name):
        try:
            self.es.indices.delete(index_name, ignore=400)
        except NotFoundError:
            pass
        if index_name == Config.ELASTICSEARCH_DATA_INDEX_NAME:
            # TODO: set evidence_chain objects as nested so they don't mess with searches with the same key due to the reverse index
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.evidence_data_mapping,
            )
        elif index_name == Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.efo_data_mapping
            )
        elif index_name == Config.ELASTICSEARCH_ECO_INDEX_NAME:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.eco_data_mapping
            )
        elif index_name == Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.gene_data_mapping
            )
        elif index_name == Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.expression_data_mapping
            )
        else:
            self.es.indices.create(index=index_name, ignore=400)
        return