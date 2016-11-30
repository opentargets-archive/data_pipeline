import json
import random
from collections import defaultdict
from datetime import datetime
import time
import logging
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import parallel_bulk, bulk
from common.DataStructure import JSONSerializable
from common.EvidenceJsonUtils import assertJSONEqual
from settings import Config
from elasticsearch_config import ElasticSearchConfiguration
__author__ = 'andreap'


class Loader():
    """
    Loads data to elasticsearch
    """

    def __init__(self,
                 es = None,
                 chunk_size=1000,
                 dry_run = False,
                 max_flush_interval = random.choice(range(8,16))):

        self.es = es
        self.cache = []
        self.results = defaultdict(list)
        self.chunk_size = chunk_size
        self.indexes_created=[]
        self.indexes_optimised = {}
        self.dry_run = dry_run
        if es is None:
            self.dry_run = True
        self.max_flush_interval = max_flush_interval
        self._last_flush_time = time.time()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def get_versioned_index(index_name):
        if index_name.startswith(Config.RELEASE_VERSION+'_'):
            raise ValueError('Cannot add %s twice to index %s'%(Config.RELEASE_VERSION, index_name))
        if index_name.startswith('!'):
            return index_name
        return Config.RELEASE_VERSION + '_' + index_name



    def put(self, index_name, doc_type, ID, body, create_index = True, operation= None,routing = None, parent = None, auto_optimise = False):

        if index_name not in self.indexes_created:
            if create_index:
                self.create_new_index(index_name)
            self.indexes_created.append(index_name)
        versioned_index_name = self.get_versioned_index(index_name)
        if auto_optimise and (versioned_index_name not in self.indexes_optimised):
            self.prepare_for_bulk_indexing(versioned_index_name)
        if isinstance(body, JSONSerializable):
            body = body.to_json()
        submission_dict = dict(_index=versioned_index_name,
                               _type=doc_type,
                               _id=ID,
                               _source=body)
        if operation is not None:
            submission_dict['_op_type']=operation
        if routing is not None:
            submission_dict['_routing']=routing
        if parent is not None:
            submission_dict['_parent']=parent
        self.cache.append(submission_dict)

        if self.cache and ((len(self.cache) == self.chunk_size) or
                (time.time() - self._last_flush_time >= self.max_flush_interval)):
            self.flush()

    def flush(self, max_retry=10):
        if self.cache:
            retry = 0
            while 1:
                try:
                   self._flush()
                   break
                except Exception as e:
                    retry+=1
                    if retry >= max_retry:
                        self.logger.exception("push to elasticsearch failed for chunk, giving up...")
                        break
                    else:
                        time_to_wait = 5*retry
                        self.logger.error("push to elasticsearch failed for chunk: %s.  retrying in %is..."%(str(e),time_to_wait))
                        time.sleep(time_to_wait)
            self.cache = []


        # if index_name:
        # self.cache[index_name] = []
        # else:
        #     for index_name in self.cache:
        #         self.cache[index_name] = []
    # @profile
    def _flush(self):
        if not self.dry_run:
            bulk(self.es,
                self.cache,
                 stats_only=True)
            # thread_count = 10
            # chunk_size = int(self.chunk_size/thread_count)
            # parallel_bulk(
            #     self.es,
            #     self.cache,
            #     thread_count=thread_count,
            #     chunk_size=chunk_size,)
            # for ok, results in parallel_bulk(
            #         self.es,
            #         self.cache,
            #         thread_count=thread_count,
            #         chunk_size=chunk_size,
            #         request_timeout=60000,
                # ):

                # action, result = results.popitem()
                # self.results[result['_index']].append(result['_id'])
                # doc_id = '/%s/%s' % (result['_index'], result['_id'])
                # try:
                #     if (len(self.results[result['_index']]) % self.chunk_size) == 0:
                #         self.logger.debug(
                #             "%i entries uploaded in elasticsearch for index %s" % (
                #             len(self.results[result['_index']]), result['_index']))
                #     if not ok:
                #         self.logger.error('Failed to %s document %s: %r' % (action, doc_id, result))
                # except ZeroDivisionError:
                #     pass


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


    def _safe_create_index(self, index_name, body={}, ignore=400):
        if not self.dry_run:
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
            if self._enforce_mapping(index_name):

                mappings = self.es.indices.get_mapping(index=index_name)
                settings = self.es.indices.get_settings(index=index_name)

            try:
                if 'mappings' in body:
                    datatypes = body['mappings'].keys()
                    for dt in datatypes:
                        if dt != '_default_':
                            keys = body['mappings'][dt].keys()
                            if 'dynamic_templates' in keys:
                                del keys[keys.index('dynamic_templates')]
                            assertJSONEqual(mappings[index_name]['mappings'][dt],
                                            body['mappings'][dt],
                                            msg='mappings in elasticsearch are different from the ones sent for datatype %s'%dt,
                                            keys = keys)
                if 'settings' in body:
                    assertJSONEqual(settings[index_name]['settings']['index'],
                                    body['settings'],
                                    msg='settings in elasticsearch are different from the ones sent',
                                    keys=body['settings'].keys(),#['number_of_replicas','number_of_shards','refresh_interval']
                                    )
            except ValueError as e:
                self.logger.exception("elasticsearch settings error")

    def create_new_index(self, index_name, recreate = False):
        if not self.dry_run:
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
                    self.logger.debug("%s index deleted: %s" %(index_name, str(res)))

                else:
                    self.logger.info("%s index already existing" % index_name)
                    return

            index_created = False
            for index_root,mapping in ElasticSearchConfiguration.INDEX_MAPPPINGS.items():
                if index_root in index_name:
                    self._safe_create_index(index_name, mapping)
                    index_created=True
                    break

            if not index_created:
                self._safe_create_index(index_name)
                logging.warning('Index %s created without explicit mappings' % index_name)
            logging.info("%s index created" % index_name)
            return

    def _enforce_mapping(self, index_name):
        for index_root in ElasticSearchConfiguration.INDEX_MAPPPINGS:
            if index_root in index_name:
                return True
        return False

    def clear_index(self, index_name):
        if not self.dry_run:
            self.es.indices.delete(index=index_name)

    def optimize_all(self):
        if not self.dry_run:
            try:
                self.es.indices.optimize(index='', max_num_segments=5, wait_for_merge = False)
            except:
                self.logger.warn('optimisation of all indexes failed')

    def optimize_index(self, index_name):
        if not self.dry_run:
            try:
                self.es.indices.optimize(index=index_name, max_num_segments=5, wait_for_merge = False)
            except:
                self.logger.warn('optimisation of index %s failed'%index_name)

    def _check_is_aknowledge(self, res):
        return (u'acknowledged' in res) and (res[u'acknowledged'] == True)
