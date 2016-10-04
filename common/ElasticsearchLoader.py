import json
from collections import defaultdict
from datetime import datetime
import time
import logging
from difflib import Differ
from pprint import pprint
from unittest import TestCase

import collections

import sys
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import parallel_bulk
from sqlalchemy import and_
from common import Actions
from common.DataStructure import JSONSerializable
from common.EvidenceJsonUtils import assertJSONEqual
from common.PGAdapter import ElasticsearchLoad
from common.processify import processify
from settings import Config
from elasticsearch_config import ElasticSearchConfiguration

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

    def __init__(self,
                 es,
                 chunk_size=1000,
                 dry_run = False,
                 max_flush_interval = 10):

        self.es = es
        self.cache = []
        self.results = defaultdict(list)
        self.chunk_size = chunk_size
        self.indexes_created=[]
        self.indexes_optimised = {}
        self.dry_run = dry_run
        self.max_flush_interval = max_flush_interval
        self._last_flush_time = time.time()

    @staticmethod
    def get_versioned_index(index_name):
        if index_name.startswith(Config.RELEASE_VERSION+'_'):
            raise ValueError('Cannot add %s twice to index %s'%(Config.RELEASE_VERSION, index_name))
        if index_name.startswith('!'):
            return index_name
        return Config.RELEASE_VERSION + '_' + index_name



    def put(self, index_name, doc_type, ID, body, create_index = True, routing = None, parent = None):

        if index_name not in self.indexes_created:
            if create_index:
                self.create_new_index(index_name)
            self.indexes_created.append(index_name)
        versioned_index_name = self.get_versioned_index(index_name)
        if versioned_index_name not in self.indexes_optimised:
            self.prepare_for_bulk_indexing(versioned_index_name)
        if isinstance(body, JSONSerializable):
            body = body.to_json()
        submission_dict = dict(_index=versioned_index_name,
                               _type=doc_type,
                               _id=ID,
                               _source=body)
        if routing is not None:
            submission_dict['_routing']=routing
        if parent is not None:
            submission_dict['_parent']=parent
        self.cache.append(submission_dict)

        if self.cache and ((len(self.cache) == self.chunk_size) or
                (time.time() - self._last_flush_time >= self.max_flush_interval)):
            self.flush()


    def flush(self, max_retry=10):
        retry = 0
        while 1:
            try:
               self._flush()
               break
            except Exception, e:
                retry+=1
                if retry >= max_retry:
                    logging.exception("push to elasticsearch failed for chunk, giving up...")
                    break
                else:
                    time_to_wait = 5*retry
                    logging.error("push to elasticsearch failed for chunk: %s.  retrying in %is..."%(str(e)[:250],time_to_wait))
                    time.sleep(time_to_wait)
        self.cache = []


        # if index_name:
        # self.cache[index_name] = []
        # else:
        #     for index_name in self.cache:
        #         self.cache[index_name] = []

    def _flush(self):
        if not self.dry_run:
            thread_count = 10
            chunk_size = int(self.chunk_size/thread_count)
            for ok, results in parallel_bulk(
                    self.es,
                    self.cache,
                    thread_count=thread_count,
                    chunk_size=chunk_size,
                    request_timeout=60000,
                ):

                action, result = results.popitem()
                self.results[result['_index']].append(result['_id'])
                doc_id = '/%s/%s' % (result['_index'], result['_id'])
                try:
                    if (len(self.results[result['_index']]) % self.chunk_size) == 0:
                        logging.debug(
                            "%i entries uploaded in elasticsearch for index %s" % (
                            len(self.results[result['_index']]), result['_index']))
                    if not ok:
                        logging.error('Failed to %s document %s: %r' % (action, doc_id, result))
                except ZeroDivisionError:
                    pass


    def close(self):

        self.flush()
        self.restore_after_bulk_indexing()


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()

    def prepare_for_bulk_indexing(self, index_name):
        if not self.dry_run:
            old_cluster_settings = self.es.cluster.get_settings()
            try:
                if old_cluster_settings['persistent']['indices']['store']['throttle']['type']=='none':
                    pass
                else:
                    raise ValueError
            except (KeyError, ValueError):
                transient_cluster_settings = {
                                            "persistent" : {
                                                "indices.store.throttle.type" : "none"
                                            }
                                        }
                self.es.cluster.put_settings(transient_cluster_settings)
            old_index_settings = self.es.indices.get_settings(index=index_name)
            temp_index_settings = {
                        "index" : {
                            "refresh_interval" : "-1",
                            "number_of_replicas" : 0,
                            "translog.durability" : 'async',
                        }
            }
            self.es.indices.put_settings(index=index_name,
                                         body =temp_index_settings)
            self.indexes_optimised[index_name]= dict(settings_to_restore={
                        "index" : {
                            "refresh_interval" : "1s",
                            "number_of_replicas" : old_index_settings[index_name]['settings']['index']['number_of_replicas'],
                            "translog.durability": 'request',
                        }
                })
    def restore_after_bulk_indexing(self):
        if not self.dry_run:
            for index_name in self.indexes_optimised:
                self.es.indices.put_settings(index=index_name,
                                             body=self.indexes_optimised[index_name]['settings_to_restore'])
                self.optimize_index(index_name)


    def _safe_create_index(self, index_name, body, ignore=400):
        res = self.es.indices.create(index=index_name,
                                     ignore=ignore,
                                     body=body
                                     )
        if not self._check_is_aknowledge(res):
            if res['error']['root_cause'][0]['reason']== 'already exists':
                logging.error('cannot create index %s because it already exists'%index_name) #TODO: remove this temporary workaround, and fail if the index exists
                return
            else:
                raise ValueError('creation of index %s was not acknowledged. ERROR:%s'%(index_name,str(res['error'])))
        mappings = self.es.indices.get_mapping(index=index_name)
        settings = self.es.indices.get_settings(index=index_name)

        if 'mappings' in body:
            assertJSONEqual(mappings[index_name]['mappings'],
                            body['mappings'],
                            msg='mappings in elasticsearch are different from the ones sent')
        if 'settings' in body:
            assertJSONEqual(settings[index_name]['settings']['index'],
                            body['settings'],
                            msg='settings in elasticsearch are different from the ones sent',
                            keys=body['settings'].keys(),#['number_of_replicas','number_of_shards','refresh_interval']
                            )

    def create_new_index(self, index_name, recreate = False):
        index_name = self.get_versioned_index(index_name)
        if self.es.indices.exists(index_name):
            if recreate:
                res = self.es.indices.delete(index_name)
                if not self._check_is_aknowledge(res):
                    raise ValueError(
                        'deletion of index %s was not acknowledged. ERROR:%s' % (index_name, str(res['error'])))
                try:
                    self.es.indices.flush(index_name,  wait_if_ongoing =True)
                except NotFoundError:
                    pass
                logging.debug("%s index deleted: %s" %(index_name, str(res)))

            else:
                logging.info("%s index already existing" % index_name)
                return

        index_created = False
        for index_root,mapping in ElasticSearchConfiguration.INDEX_MAPPPINGS.items():
            if index_root in index_name:
                self._safe_create_index(index_name, mapping)
                index_created=True
                break

        if not index_created:
            raise ValueError('Cannot create index %s because no mappings are set'%index_name)
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
            self.es.indices.optimize(index=index_name, max_num_segments=5, wait_for_merge = False)
        except:
            logging.warn('optimisation of index %s failed'%index_name)

    def _check_is_aknowledge(self, res):
        return (u'acknowledged' in res) and (res[u'acknowledged'] == True)
