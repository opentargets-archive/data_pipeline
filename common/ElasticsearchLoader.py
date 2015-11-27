from collections import defaultdict
from datetime import datetime, time
import logging
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from sqlalchemy import and_
from common import Actions
from common.PGAdapter import ElasticsearchLoad
from common.processify import processify
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
                and_(ElasticsearchLoad.index.startswith(index_name),
                     ElasticsearchLoad.type == doc_name)).delete(synchronize_session=False)
        else:
            rows_deleted = session.query(
                ElasticsearchLoad).filter(ElasticsearchLoad.index.startswith(index_name)).delete(synchronize_session=False)
        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)

    @staticmethod
    def store_to_pg(session,
                    index_name,
                    doc_name,
                    data,
                    delete_prev=True,
                    autocommit=True,
                    quiet = False):
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
                logging.debug("%i rows of %s inserted to elasticsearch_load" %(c, doc_name))
                session.flush()
        session.flush()
        if autocommit:
            session.commit()
        if not quiet:
            logging.info('inserted %i rows of %s inserted in elasticsearch_load' %(c, doc_name))
        return c

    @staticmethod
    def store_to_pg_core(adapter,
                    index_name,
                    doc_name,
                    data,
                    delete_prev=True,
                    autocommit=True,
                    quiet = False):
        if delete_prev:
            JSONObjectStorage.delete_prev_data_in_pg(adapter.session, index_name, doc_name)
        rows_to_insert =[]
        for key, value in data.iteritems():
            rows_to_insert.append(dict(id=key,
                                      index=index_name,
                                      type=doc_name,
                                      data=value.to_json(),
                                      active=True,
                                      ))
        adapter.engine.execute(ElasticsearchLoad.__table__.insert(),rows_to_insert)

        # if autocommit:
        #     adapter.session.commit()
        if not quiet:
            logging.info('inserted %i rows of %s inserted in elasticsearch_load' %(len(rows_to_insert), doc_name))
        return len(rows_to_insert)


    @staticmethod
    def refresh_index_data_in_es(loader, session, index_name, doc_name=None):
        """given an index and a doc_name,
        - remove and recreate the index
        - load all the available data with that doc_name for that index
        """
        # loader.create_new_index(index_name)
        if doc_name:
            for row in session.query(ElasticsearchLoad.id, ElasticsearchLoad.index, ElasticsearchLoad.data, ).filter(and_(
                            ElasticsearchLoad.index.startswith(index_name),
                            ElasticsearchLoad.type == doc_name,
                            ElasticsearchLoad.active == True)
            ).yield_per(loader.chunk_size):
                loader.put(row.index, doc_name, row.id, row.data)
        else:
            for row in session.query(ElasticsearchLoad.id, ElasticsearchLoad.index, ElasticsearchLoad.type, ElasticsearchLoad.data, ).filter(and_(
                            ElasticsearchLoad.index.startswith(index_name),
                            ElasticsearchLoad.active == True)
            ).yield_per(loader.chunk_size):
                loader.put(row.index, row.type, row.id, row.data)
        loader.flush()
        # loader.restore_after_bulk_indexing()


    @staticmethod
    def refresh_all_data_in_es(loader, session):
        """push all the data stored in elasticsearch_load table to elasticsearch,
        - remove and recreate each index
        - load all the available data with any for that index
        """


        for row in session.query(ElasticsearchLoad).yield_per(loader.chunk_size):
            loader.put(row.index, row.type, row.id, row.data,create_index = True)


        loader.flush()
        # loader.restore_after_bulk_indexing()

    @staticmethod
    def get_data_from_pg(session, index_name, doc_name, objid):
        """given an index and a doc_name and an id return the json object tore in postgres
        """
        row = session.query(ElasticsearchLoad.data).filter(and_(
            ElasticsearchLoad.index.startswith(index_name),
            ElasticsearchLoad.type == doc_name,
            ElasticsearchLoad.active == True,
            ElasticsearchLoad.id == objid)
        ).first()
        if row:
            return row.data

    @staticmethod
    def paginated_query(q, page_size = 1000):
        offset = 0
        # @processify
        def get_results():
            return q.limit(page_size).offset(offset)
        while True:
            r = False
            for elem in get_results():
               r = True
               yield elem
            offset += page_size
            if not r:
                break

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
        logging.debug("loader chunk_size: %i"%chunk_size)

    def _get_versioned_index(self,index_name):
        return Config.RELEASE_VERSION + '_' + index_name



    def put(self, index_name, doc_type, ID, body, create_index = True):

        if  index_name not in self.index_created:
            if create_index:
                self.create_new_index(index_name)
            self.index_created.append(index_name)
            try:
                time.sleep(3)
                self.prepare_for_bulk_indexing(self._get_versioned_index(index_name))
            except:
                logging.error("cannot prepare index %s for bulk indexing"%index_name)
                pass

        self.cache.append(dict(_index=self._get_versioned_index(index_name),
                               _type=doc_type,
                               _id=ID,
                               _source=body))
        if (len(self.cache) % self.chunk_size) == 0:
            self.flush()

            # self.load_single(index_name, doc_type, ID, body)

    def flush(self):
        # for ok, results in streaming_bulk(
        for ok, results in parallel_bulk(
                self.es,
                self.cache,
                chunk_size=self.chunk_size,
                request_timeout=1200,
        ):

            action, result = results.popitem()
            self.results[result['_index']].append(result['_id'])
            doc_id = '/%s/%s' % (result['_index'], result['_id'])
            if (len(self.results[result['_index']]) % self.chunk_size) == 0:
                logging.debug(
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
        for index_name in self.index_created:
            self.restore_after_bulk_indexing(index_name)

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
        self.close()

    def prepare_for_bulk_indexing(self, index_name):
        settings = {
                    "index" : {
                        "refresh_interval" : "-1",
                        "number_of_replicas" : 0
                    }
        }
        self.es.indices.put_settings(index=index_name,
                                     body =settings)
    def restore_after_bulk_indexing(self, index_name):
        settings = {
                    "index" : {
                        "refresh_interval" : "60s",
                        "number_of_replicas" : 1
                    }
        }
        index_name = self._get_versioned_index(index_name)

        def update_settings(base_settings, specific_settings):
            for key in ["refresh_interval", "number_of_replicas"]:
                if key in specific_settings['settings']:
                    base_settings["index"][key]= specific_settings['settings'][key]
            return base_settings
        if Config.ELASTICSEARCH_DATA_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.evidence_data_mapping)
        elif Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.score_data_mapping)
        elif Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.efo_data_mapping)
        elif Config.ELASTICSEARCH_ECO_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.eco_data_mapping)
        elif Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.gene_data_mapping)
        elif Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME in index_name:
            settings=update_settings(settings,ElasticSearchConfiguration.expression_data_mapping)

        self.es.indices.put_settings(index=index_name,
                                     body =settings)

    def create_new_index(self, index_name):
        index_name = self._get_versioned_index(index_name)
        try:
            self.es.indices.delete(index_name, ignore=400)
        except NotFoundError:
            pass
        if Config.ELASTICSEARCH_DATA_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.evidence_data_mapping,
                                   )
        elif Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.score_data_mapping,
                                   )
        elif Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.efo_data_mapping
                                   )
        elif Config.ELASTICSEARCH_ECO_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.eco_data_mapping
                                   )
        elif Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.gene_data_mapping
                                   )
        elif Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME in index_name:
            self.es.indices.create(index=index_name,
                                   ignore=400,
                                   body=ElasticSearchConfiguration.expression_data_mapping
                                   )
        else:
            self.es.indices.create(index=index_name, ignore=400)

        logging.info("%s index created"%index_name)

        return

    def clear_index(self, index_name):
        self.es.indices.delete(index=index_name)

    def optimize_all(self):
        try:
            self.es.indices.optimize(index='', max_num_segments=5, wait_for_merge = False)
        except:
            logging.warn('optimisation of all indexes failed')

    def optimize_index(self, index_name):
        try:
            self.es.indices.optimize(index=self._get_versioned_index(index_name), max_num_segments=5, wait_for_merge = False)
        except:
            logging.warn('optimisation of index %s failed'%index_name)
