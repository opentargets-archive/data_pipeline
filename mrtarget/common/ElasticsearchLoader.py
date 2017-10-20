import random
from collections import defaultdict
import time
import logging
import tempfile
import json
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.EvidenceJsonUtils import assertJSONEqual
from mrtarget.common.Redis import RedisQueueWorkerProcess
from mrtarget.common.connection import PipelineConnectors
from mrtarget.Settings import Config
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration



class Loader():
    """
    Loads data to elasticsearch
    """

    def __init__(self,
                 es = None,
                 chunk_size=1000,
                 dry_run = False,
                 max_flush_interval = random.choice(range(60,120))):

        self.logger = logging.getLogger(__name__)

        if es is None and not dry_run:
            connector = PipelineConnectors()
            connector.init_services_connections()
            es = connector.es
            if es is None:
                e = EnvironmentError('no elasticsearch connection '
                                     'was properly setup')
                self.logger.exception(e)
                raise e

        self.es = es
        self.cache = []
        self.results = defaultdict(list)
        self.chunk_size = chunk_size
        self.indexes_created = []
        self.indexes_optimised = {}
        self.dry_run = dry_run
        self.max_flush_interval = max_flush_interval
        self._last_flush_time = time.time()
        self._tmp_fd = None
        self._tmp_fd_enable = Config.DRY_RUN_OUTPUT
        self._tmp_fd_delete = Config.DRY_RUN_OUTPUT_DELETE
        self._tmp_fd_count = Config.DRY_RUN_OUTPUT_COUNT

    @staticmethod
    def get_versioned_index(index_name, check_custom_idxs=False):
        '''get a composed real name of the index

        If check_custom_idxs is set to True then it tries to get
        from ES_CUSTOM_IDXS_FILENAME config file. This config file
        is like this and no prefixes or versions will be appended

        [indexes]
        gene-data=new-gene-data-index-name

        if no index field or config file is found then a default
        composed index name will be returned
        '''
        if index_name.startswith(Config.RELEASE_VERSION+'_'):
            raise ValueError('Cannot add %s twice to index %s'
                             % (Config.RELEASE_VERSION, index_name))
        if index_name.startswith('!'):
            return index_name

        # quite tricky, isn't it? we do code HYPERfunctions
        # not mere functions you need to be reading this whole code
        # for a 5-dimensions space to get it in its full
        # why an asterisk? because the index name is really a string
        # to be parsed by elasticsearch as a multiindex shiny thing
        suffix = '*' if index_name.endswith('*') else ''
        raw_name = index_name[:-len(suffix)] if len(suffix) > 0 else index_name

        idx_name = Config.ES_CUSTOM_IDXS_INI.get('indexes', raw_name) \
            if check_custom_idxs and \
            Config.ES_CUSTOM_IDXS and \
            Config.ES_CUSTOM_IDXS_INI and \
            Config.ES_CUSTOM_IDXS_INI.has_option('indexes', raw_name) \
            else Config.RELEASE_VERSION + '_' + index_name

        return idx_name + suffix

    def is_dry_run(self):
        return self.dry_run

    def put(self,
            index_name,
            doc_type,
            ID,
            body,
            create_index = True,
            operation= None,
            routing = None,
            parent = None,
            auto_optimise = False):

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
                        self.logger.warning("push to elasticsearch failed for chunk: %s.  retrying in %is..."%(str(e),time_to_wait))
                        time.sleep(time_to_wait)

            del self.cache[:]


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
        else:
            # create a temporal file if necessary
            if self._tmp_fd_enable and self._tmp_fd_count > 0:
                if self._tmp_fd is None:
                    self._tmp_fd = tempfile.NamedTemporaryFile(
                        delete=self._tmp_fd_delete)
                    self.logger.info('create temporary file to output '
                                     'generated index docs while dry_run '
                                     'is activated with file %s',
                                     self._tmp_fd.name)


                # flush self.cache into temp file converted as text lines
                self._tmp_fd.writelines([json.dumps(el) + '\n'
                                         for el in self.cache])
                self._tmp_fd_count = self._tmp_fd_count - len(self.cache) if \
                    self._tmp_fd_count > 0 else 0

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
                self.logger.debug('restoring to normal and flushing index: %s'%index_name)
                self.es.indices.flush(index_name,
                                      wait_if_ongoing=True)
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
                    self.logger.error('cannot create index %s because it already exists'%index_name) #TODO: remove this temporary workaround, and fail if the index exists
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
                    time.sleep(0.5)#wait for the index to be deleted
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
                self.logger.warning('Index %s created without explicit mappings' % index_name)
            self.logger.info("%s index created" % index_name)
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



class LoaderWorker(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out = None,
                 chunk_size = 1000,
                 dry_run = False
                 ):
        super(LoaderWorker, self).__init__(queue_in, redis_path, queue_out)
        self.chunk_size = chunk_size
        self.es = None
        self.loader = None
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        '''
        expects an array of args and kwargs to pass to a loader.put method
        :param data:
        :return:
        '''
        if data:
            args, kwargs = data
            self.loader.put(*args,
                            **kwargs
                            )

    def init(self):
        super(LoaderWorker, self).init()
        self.loader = Loader(chunk_size=self.chunk_size,
                             dry_run=self.dry_run)

    def close(self):
        super(LoaderWorker, self).close()
        self.loader.close()
