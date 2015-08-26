from collections import defaultdict
from datetime import datetime
import logging
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import streaming_bulk
from sqlalchemy import and_
from common import Actions
from common.PGAdapter import ElasticsearchLoad
from settings import ElasticSearchConfiguration, Config

__author__ = 'andreap'

class ElasticsearchActions(Actions):
    RELOAD='reload'

class JSONObjectStorage():

    @staticmethod
    def delete_prev_data_in_pg(session, index_name, doc_name = None):
        if doc_name is not None:
            rows_deleted = session.query(
                ElasticsearchLoad).filter(
                and_(ElasticsearchLoad.index == index_name,
                     ElasticsearchLoad.type == doc_name)).delete()
        else:
            rows_deleted = session.query(
                ElasticsearchLoad).filter(ElasticsearchLoad.index == index_name).delete()
        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)

    @staticmethod
    def store_to_pg(session,
                    index_name,
                    doc_name,
                    data,
                    delete_prev=True,
                    autocommit=True):
        if delete_prev:
            JSONObjectStorage.delete_prev_data_in_pg(session, index_name, doc_name)
        c = 0
        for key, value in data.iteritems():
            c += 1
            session.add(ElasticsearchLoad(id=key,
                                          index=index_name,
                                          type=doc_name,
                                          data=value.to_json(),
                                          active=True,
                                          date_created=datetime.now(),
                                          date_modified=datetime.now(),
                                          ))
            if c % 1000 == 0:
                logging.info("%i rows of %s inserted to elasticsearch_load" %(c, doc_name))
                session.flush()
        if autocommit:
            session.commit()
        logging.info('inserted %i rows of %s inserted in elasticsearch_load' %(c, doc_name))

    @staticmethod
    def refresh_index_data_in_es(loader, session, index_name, doc_name=None):
        """given an index and a doc_name,
        - remove and recreate the index
        - load all the available data with that doc_name for that index
        """
        # loader.create_new_index(index_name)
        if doc_name:
            for row in session.query(ElasticsearchLoad.id, ElasticsearchLoad.data, ).filter(and_(
                            ElasticsearchLoad.index == index_name,
                            ElasticsearchLoad.type == doc_name,
                            ElasticsearchLoad.active == True)
            ).yield_per(loader.chunk_size):
                loader.put(index_name, doc_name, row.id, row.data)
        else:
            for row in session.query(ElasticsearchLoad.id, ElasticsearchLoad.type, ElasticsearchLoad.data, ).filter(and_(
                            ElasticsearchLoad.index == index_name,
                            ElasticsearchLoad.active == True)
            ).yield_per(loader.chunk_size):
                if index_name == Config.ELASTICSEARCH_DATA_INDEX_NAME:#force split in different indexes
                    loader.put(index_name+'-'+row.type, row.type, row.id, row.data)
                else:
                    loader.put(index_name, row.type, row.id, row.data)
        loader.flush()

    @staticmethod
    def refresh_all_data_in_es(loader, session):
        """push all the data stored in elasticsearch_load table to elasticsearch,
        - remove and recreate each index
        - load all the available data with any for that index
        """
        created_indexes =[]

        for row in session.query(ElasticsearchLoad).yield_per(loader.chunk_size):
            if row.index not in created_indexes:
                loader.create_new_index(row.index)
                created_indexes.append(row.index)
            loader.put(row.index, row.type, row.id, row.data)

        loader.flush()

    @staticmethod
    def get_data_from_pg(session, index_name, doc_name, objid):
        """given an index and a doc_name and an id return the json object tore in postgres
        """
        row = session.query(ElasticsearchLoad.data).filter(and_(
            ElasticsearchLoad.index == index_name,
            ElasticsearchLoad.type == doc_name,
            ElasticsearchLoad.active == True,
            ElasticsearchLoad.id == objid)
        ).first()
        if row:
            return row.data


class Loader():
    """
    Loads data to elasticsearch
    """

    def __init__(self, es, chunk_size=1000):

        self.es = es
        self.cache = []
        self.results = defaultdict(list)
        self.chunk_size = chunk_size
        self.index_created=[]

    def put(self, index_name, doc_type, ID, body):

        if not index_name in self.index_created:
            self.create_new_index(index_name)
            self.index_created.append(index_name)

        self.cache.append(dict(_index=index_name,
                               _type=doc_type,
                               _id=ID,
                               _source=body))
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
                logging.info(
                    "%i entries uploaded in elasticsearch for index %s" % (len(self.results[result['_index']]), result['_index']))
            if not ok:
                logging.error('Failed to %s document %s: %r' % (action, doc_id, result))
            else:
                pass
        self.cache = []


        # if index_name:
        # self.cache[index_name] = []
        # else:
        #     for index_name in self.cache:
        #         self.cache[index_name] = []


    def close(self):

        self.flush()

    #
    # def load_single(self, index_name, doc_type, ID, body):
    # self.results[index_name].append(
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
        if index_name.startswith(Config.ELASTICSEARCH_DATA_INDEX_NAME):
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