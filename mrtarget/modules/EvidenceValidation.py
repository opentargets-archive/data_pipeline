from __future__ import division

import datetime
import hashlib
import logging
import re
from time import strftime
import time

from addict import Dict

import logging as l
from mrtarget.Settings import Config
from mrtarget.common import Actions, URLZSource, generate_validators_from_schemas, LogAccum
from mrtarget.common.ElasticsearchLoader import Loader, LoaderWorker
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.EvidenceJsonUtils import DatatStructureFlattener
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from tqdm import tqdm
from mrtarget.common import TqdmToLogger
import ujson as json


BLOCKSIZE = 65536
NB_JSON_FILES = 3
MAX_NB_EVIDENCE_CHUNKS = 1000
EVIDENCESTRING_VALIDATION_CHUNK_SIZE = 1


TOP_20_TARGETS_QUERY = {  "size": 0, "aggs" : {  "group_by_targets" : {   "terms" : { "field" : "target_id", "order" : { "_count" : "desc" }, "size": 20 } } } }
TOP_20_DISEASES_QUERY = {  "size": 0, "aggs" : {  "group_by_diseases" : {   "terms" : { "field" : "disease_id", "order" : { "_count" : "desc" }, "size": 20 } } } }
DISTINCT_TARGETS_QUERY = {  "size": 0, "aggs" : {  "distinct_targets" : {  "cardinality" : { "field" : "target_id" } } } }
DISTINCT_DISEASES_QUERY = {  "size": 0, "aggs" : {  "distinct_diseases" : {  "cardinality" : { "field" : "disease_id" } } } }
SUBMISSION_FILTER_FILENAME_QUERY = '''
{
  "query": {
    "constant_score": {
      "filter": {
        "terms" : { "filename": ["%s"]}
      }
    }
  }
}
'''



SUBMISSION_FILTER_MD5_QUERY = '''
{
  "query": {
    "constant_score": {
      "filter": {
        "terms" : { "md5": ["%s"]}
      }
    }
  }
}

'''




# yyyyMMdd'T'HHmmssZ
VALIDATION_DATE = strftime("%Y%m%dT%H%M%SZ")

# figlet -c "Validation Passed"
messagePassed = '''-----------------
VALIDATION PASSED
-----------------
'''

messageFailed = '''-----------------
VALIDATION FAILED
-----------------
'''

class FileTypes():
    LOCAL='local'
    S3='s3'
    HTTP='http'

class ValidationActions(Actions):
    CHECKFILES = 'checkfiles'
    VALIDATE = 'validate'
    GENEMAPPING = 'genemapping'
    RESET = 'reset'


class FileProcesser():
    def __init__(self,
                 output_q,
                 es,
                 r_server,
                 input_files=[],
                 dry_run=False,
                 increment=False):
        self.output_q = output_q
        self.es = es
        self.loader = Loader(dry_run = dry_run)
        self.start_time = time.time()
        self.r_server = r_server
        self._remote_filenames =dict()
        self.input_files = input_files
        self.logger = logging.getLogger(__name__)
        self.dry_run = dry_run
        self.increment = increment


    def _callback_not_used(self, path):
        self.logger.debug("skipped "+path)

    @staticmethod
    def _tokens_from_filename(filename):
        '''return a dict with all needed tokens based on a filename format'''
        rres = re.search(Config.EVIDENCEVALIDATION_FILENAME_REGEX, filename)
        valid_rres = rres.groupdict() if rres else None

        if valid_rres and valid_rres['datasource'] is None:
            # 'datasource' field is a really required one so I cannot be
            # missed
            valid_rres = None

        # if not valid date get from now()
        if valid_rres and valid_rres['d3'] is None:
            now = datetime.datetime.now()
            valid_rres['d1'] = str(now.day)
            valid_rres['d2'] = str(now.month)
            valid_rres['d3'] = str(now.year)

        return valid_rres

    def run(self):
        '''create index for the evidence if it does not exists if the
        evidence index exists, the index the submitted files if it does not
        exists
        '''
        processed_datasources = []

        self.logger.info('preprocess all input files to get filename '
                         'info and submit')
        for uri in self.input_files:
            self.logger.debug('get tokens from filename %s', uri)
            tokens = self._tokens_from_filename(uri)

            if tokens:
                self.logger.debug('got tokens so now creating idx and submit')
                idx_name = Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + \
                           '-' + tokens['datasource']
                self.loader.create_new_index(idx_name,
                                             recreate=not self.increment)
                self.loader.prepare_for_bulk_indexing(
                    self.loader.get_versioned_index(idx_name,))
                processed_datasources.append(tokens['datasource'])

                try:
                    v_name = ''.join([tokens[k] for k in ('d3', 'd2', 'd1')])
                    self.submit_file(uri, v_name, 'datasourcefile',
                                     tokens['datasource'], None, None, None)

                    self.logger.debug('submitted filename %s with version %s '
                                      'and these tokens from uri %s',
                                      uri, v_name, str(tokens))
                except Exception as e:
                    self.logger.exception(e)

            else:
                self.logger.error('failed to parse and get tokens from'
                                  ' filename %s and must to match %s',
                                  uri,
                                  Config.EVIDENCEVALIDATION_FILENAME_REGEX)

        self.output_q.set_submission_finished(self.r_server)

        self.logger.info('finished preprocessing and submitting files')
        return processed_datasources

    def submit_file(self,
                    file_path,
                    file_version,
                    provider_id,
                    data_source_name,
                    md5_hash=None,
                    logfile=None,
                    file_type=None,
                    ):
        self.output_q.put((file_path, file_version, provider_id,
                           data_source_name, md5_hash, logfile, file_type),
                          self.r_server)



class FileReaderProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out=None,
                 es=None,
                 dry_run=False):
        super(FileReaderProcess, self).__init__(queue_in, redis_path, queue_out)
        self.es = None
        self.loader = None
        self.dry_run = dry_run
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self.logger,level=logging.INFO)

    def process(self, data):
        file_path, file_version, provider_id, data_source_name, md5_hash,\
            logfile, file_type = data

        self.logger.info('Starting to parse  file %s and putting evidences'
                         ' in the queue', file_path)
        self.parse_file(file_path, file_version, provider_id, data_source_name,
                        md5_hash, logfile=logfile)
        self.logger.info("%s finished", self.name)

    def parse_file(self, file_path, file_version, provider_id,
                   data_source_name, md5_hash=None, logfile=None):
        self.logger.info('%s Starting parsing %s' % (self.name, file_path))

        with URLZSource(file_path).open() as f_handler:
            self.logger.debug('reading the file to get n lines and'
                              ' process each line')

            n_lines = self._count_file_lines(f_handler)
            total_chunks = n_lines/EVIDENCESTRING_VALIDATION_CHUNK_SIZE

            if n_lines % EVIDENCESTRING_VALIDATION_CHUNK_SIZE:
                total_chunks += 1

            self.queue_out.incr_total(int(round(total_chunks)), self.r_server)
            # we seek the file back to reuse it again
            f_handler.seek(0)

            for i, line in enumerate(f_handler):
                self.put_into_queue_out(
                    (file_path, file_version, provider_id, data_source_name,
                     md5_hash, logfile,
                     i / EVIDENCESTRING_VALIDATION_CHUNK_SIZE,
                     i, [line], False))

        self.logger.debug('finished reading the file to get n lines and'
                            ' process each line')
        self.queue_out.set_submission_finished(self.r_server)
        return

    @staticmethod
    def _count_file_lines(file_handle):
        '''return the number of lines in a text file including empty ones'''
        return sum(1 for el in file_handle)

    def init(self):
        super(FileReaderProcess, self).init()
        self.loader = Loader(dry_run=self.dry_run, chunk_size=1000)

    def close(self):
        super(FileReaderProcess, self).close()
        self.loader.close()


class ValidatorProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out=None,
                 es=None,
                 lookup_data= None,
                 dry_run=False,
                 ):
        super(ValidatorProcess, self).__init__(queue_in, redis_path, queue_out)
        self.es = es
        self.dry_run = dry_run
        # log accumulator
        self.log_acc = None
        self.loader = None
        self.lookup_data = lookup_data

        self.start_time = time.time()
        self.audit = list()
        self.logger = None
        self.validators = None

    def init(self):
        super(ValidatorProcess, self).init()
        self.logger = logging.getLogger(__name__)
        self.lookup_data.set_r_server(self.get_r_server())
        self.loader = Loader(dry_run=self.dry_run, chunk_size=1000)
        # log accumulator
        self.log_acc = LogAccum(self.logger, 128)
        # generate all validators once
        self.validators = \
            generate_validators_from_schemas(Config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS)


    def close(self):
        super(ValidatorProcess, self).close()
        self.log_acc.flush(True)
        self.loader.close()

    def process(self, data):
        # file_path, file_version, provider_id, data_source_name, md5_hash,
        # logfile, chunk, offset, line_buffer, end_of_transmission = data
        file_path, file_version, provider_id, data_source_name, md5_hash, \
            logfile, chunk, offset, line_buffer, end_of_transmission = data
        return self.validate_evidence(file_path,
                                      data_source_name,
                                      offset,
                                      line_buffer)

    # @profile
    def validate_evidence(self,
                          file_path,
                          data_source_name,
                          offset,
                          line_buffer):
        '''validate evidence strings from a chunk cumulate the logs, acquire a lock,
        write the logs write the data to the database

        '''
        line_counter = (offset + 1)

        # going per line inside buffer
        for i, line in enumerate(line_buffer):
            line_counter = line_counter + i
            is_valid = False
            explanation = {}
            disease_failed = False
            gene_failed = False
            other_failures = False
            gene_mapping_failed = False
            target_id = None
            efo_id = None
            data_type = None
            uniq_elements_flat_hexdig = None #uuid.uuid4().hex
            parsed_line = None

            try:
                parsed_line = json.loads(line)
                json_doc_hashdig = DatatStructureFlattener(parsed_line).get_hexdigest()
            except Exception as e:
                self.log_acc.log(l.ERROR, 'cannot parse line %i: %s', line_counter, e)
                json_doc_hashdig = hashlib.md5(line).hexdigest()
                explanation['unparsable_json'] = True

            if all([parsed_line is not None,
                    (any(k in parsed_line for k in ('label', 'type')))]):
                # setting type from label in case we have label??
                if 'label' in parsed_line:
                    parsed_line['type'] = parsed_line.pop('label', None)

                data_type = parsed_line['type']

            else:
                explanation['key_fields_missing'] = True
                other_failures = True
                self.log_acc.log(l.ERROR, "Line %i: Not a valid %s evidence string"
                                  " - missing label and type mandatory attributes",
                                 line_counter,
                                 Config.EVIDENCEVALIDATION_SCHEMA)



            if data_type is None:
                explanation['missing_datatype'] = True
                other_failures = True
                self.log_acc.log(l.ERROR, "Line %i: Not a valid %s evidence string - "
                                  "please add the mandatory 'type' attribute",
                                 line_counter, Config.EVIDENCEVALIDATION_SCHEMA)

            elif data_type not in Config.EVIDENCEVALIDATION_DATATYPES:
                other_failures = True
                self.log_acc.log(l.ERROR, 'unsupported_datatype with data type %s and line %s', data_type, parsed_line)
                explanation['unsupported_datatype'] = data_type

            else:
                t1 = time.time()
                validation_errors = [str(e) for e in self.validators[data_type].iter_errors(parsed_line)]
                t2 = time.time()

                if validation_errors:
                    # here I have to log all fails to logger and elastic
                    error_messages = ' '.join(validation_errors).replace('\n', ' ; ').replace('\r', '')

                    error_messages_len = len(error_messages)

                    # capping error message to 2048
                    error_messages = error_messages if error_messages_len <= 2048 \
                        else error_messages[:2048] + ' ; ...'

                    explanation['validation_errors'] = error_messages
                    other_failures = True
                    self.log_acc.log(l.DEBUG, 'validation_errors failed to validate %s:%i '
                                      'eval %s secs with these errors %s',
                                     file_path, line_counter, str(t2 - t1),
                                     error_messages)

                else:
                    # generate fantabulous dict from addict
                    evidence_obj = Dict(parsed_line)

                    if evidence_obj.target.id:
                        target_id = evidence_obj.target.id
                    if evidence_obj.disease.id:
                        efo_id = evidence_obj.disease.id

                    # flatten but is it always valid unique_association_fields?
                    uniq_elements = evidence_obj.unique_association_fields
                    uniq_elements_flat = DatatStructureFlattener(uniq_elements)
                    uniq_elements_flat_hexdig = uniq_elements_flat.get_hexdigest()

                    if efo_id:
                        # Check disease term or phenotype term
                        #if (short_disease_id not in self.lookup_data.available_efos) and \
                        if (efo_id not in self.lookup_data.efo_ontology.current_classes) and \
                                (efo_id not in self.lookup_data.hpo_ontology.current_classes) and \
                                (efo_id not in self.lookup_data.mp_ontology.current_classes):# or \
                                # (disease_id in self.efo_uncat):
                            explanation['invalid_disease'] = efo_id
                            disease_failed = True
                        if (efo_id in self.lookup_data.efo_ontology.obsolete_classes) or \
                                (efo_id in self.lookup_data.hpo_ontology.obsolete_classes) or \
                                (efo_id in self.lookup_data.mp_ontology.obsolete_classes):

                            explanation['obsolete_disease'] = efo_id
                    else:
                        explanation['missing_disease'] = True
                        disease_failed = True

                    # CHECK GENE/PROTEIN IDENTIFIER Check Ensembl ID, UniProt ID
                    # and UniProt ID mapping to a Gene ID
                    # http://identifiers.org/ensembl/ENSG00000178573
                    if target_id:
                        if 'ensembl' in target_id:
                            # ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
                            ensembl_id = target_id.split('/')[-1]
                            if not ensembl_id in self.lookup_data.available_genes:
                                gene_failed = True
                                explanation['unknown_ensembl_gene'] = ensembl_id
                            elif ensembl_id in self.lookup_data.non_reference_genes:
                                explanation['nonref_ensembl_gene'] = ensembl_id
                                gene_mapping_failed = True

                        elif 'uniprot' in target_id:
                            uniprot_id =  target_id.split('/')[-1]
                            if uniprot_id not in self.lookup_data.uni2ens:
                                gene_failed = True
                                explanation['unknown_uniprot_entry'] = uniprot_id
                            elif not self.lookup_data.uni2ens[uniprot_id]:#TODO:this will not happen wit the current gene processing pipeline
                                gene_mapping_failed = True
                                gene_failed = True
                                explanation['missing_ensembl_xref_for_uniprot_entry'] = uniprot_id

                            elif (uniprot_id in self.lookup_data.uni2ens) and  \
                                            self.lookup_data.uni2ens[uniprot_id] in self.lookup_data.available_genes  and \
                                            'is_reference' in  self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]  and \
                                            (not self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True) :
                                gene_failed = True
                                explanation['nonref_ensembl_xref_for_uniprot_entry'] = uniprot_id
                            else:
                                try:
                                    reference_target_list = self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True
                                except KeyError:
                                    reference_target_list = []
                                if reference_target_list:
                                    target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[0]
                                else:
                                    # get the first one, needs a better way
                                    target_id = self.lookup_data.uni2ens[uniprot_id]
                                # self.logger.info("Found target id being: %s for %s" %(target_id, uniprot_id))
                                if target_id is None:
                                    self.log_acc.log(l.INFO, "Found no target id for %s", uniprot_id)


                    # If there is no target id after the processing step
                    if target_id is None:
                        explanation['missing_target_id'] = True
                        gene_failed = True

            # flag as valid or not
            if not (disease_failed or gene_failed or other_failures):
                is_valid = True
            else:
                explanation['disease_error'] = disease_failed
                explanation['gene_error'] = gene_failed
                explanation['gene_mapping_failed'] = gene_mapping_failed
                self.log_acc.log(l.ERROR, 'evidence validation step failed at the end with an '
                            'explanation %s', str(explanation))


            loader_args = (Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '-' + data_source_name,
                           data_source_name,
                           json_doc_hashdig,
                           dict(
                               uniq_assoc_fields_hashdig=uniq_elements_flat_hexdig,
                               json_doc_hashdig=json_doc_hashdig,
                               evidence_string=line,
                               target_id=target_id,
                               disease_id=efo_id,
                               data_source_name=data_source_name,
                               json_schema_version=Config.EVIDENCEVALIDATION_SCHEMA,
                               json_doc_version=1,
                               release_date=VALIDATION_DATE,
                               is_valid=is_valid,
                               explanation=explanation,
                               line = line_counter,
                               file_name = file_path))
            loader_kwargs = dict(create_index=False)
            return loader_args, loader_kwargs


class EvidenceValidationFileChecker():
    def __init__(self,
                 es,
                 r_server,
                 chunk_size=1e4,
                 dry_run = False,
                 ):
        self.es = es
        self.esquery = ESQuery(self.es, dry_run = dry_run)
        self.es_loader = Loader(self.es)
        self.dry_run = dry_run
        self.r_server = r_server
        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0
        self.symbols = {}
        self.logger = logging.getLogger(__name__)

    def check_all(self,
                  input_files=[],
                  increment = False,
                  dry_run = False):
        '''Check every given evidence string'''

        self.logger.info('check_all() start and loading lookup data')
        dry_run = dry_run or self.dry_run
        lookup_data = LookUpDataRetriever(self.es,
                                          self.r_server,
                                          data_types=(
                                                      LookUpDataType.TARGET,
                                                      LookUpDataType.EFO,
                                                      LookUpDataType.ECO,
                                                      LookUpDataType.HPO,
                                                      LookUpDataType.MP,
                                                      # LookUpDataType.HPA
                                                     ),
                                          autoload=True,
                                          ).lookup

        # lookup_data.available_genes.load_uniprot2ensembl()

        self.logger.info('check_all() starting queue reporter')
        workers_number = Config.WORKERS_NUMBER
        loaders_number = min([16, int(workers_number/2+1)])
        readers_number = min([workers_number, len(input_files)])
        max_loader_chunk_size = 1000
        if (MAX_NB_EVIDENCE_CHUNKS / loaders_number) < max_loader_chunk_size:
            max_loader_chunk_size = int(MAX_NB_EVIDENCE_CHUNKS / loaders_number)

        'Create queues'
        file_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_file_q',
                            max_size=NB_JSON_FILES,
                            job_timeout=86400)
        evidence_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_evidence_q',
                            max_size=MAX_NB_EVIDENCE_CHUNKS*workers_number,
                            job_timeout=1200)
        store_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_store_q',
                             max_size=MAX_NB_EVIDENCE_CHUNKS*loaders_number*5,
                             job_timeout=1200)
        # audit_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_audit_q',
        #                      max_size=MAX_NB_EVIDENCE_CHUNKS,
        #                      job_timeout=1200)

        q_reporter = RedisQueueStatusReporter([file_q,
                                               evidence_q,
                                               store_q,
                                               # audit_q
                                               ],
                                              interval=30)
        q_reporter.start()

        # TODO XXX CHECK VALUE OF R_SERVER.DB
        self.logger.info('file reader process with %d processes', readers_number)

        readers = [FileReaderProcess(file_q,
                                     None,
                                     evidence_q,
                                     self.es,
                                     )
                                     for i in range(readers_number)
                                     ]

        self.logger.info('calling start to all file readers')
        for w in readers:
            w.start()

        self.logger.info('validator process with %d processes', workers_number)
        # Start validating the evidence in chunks on the evidence queue
        validators = [ValidatorProcess(evidence_q,
                                       None,
                                       store_q,
                                       self.es,
                                       lookup_data = lookup_data,
                                       dry_run=dry_run,
                                       ) for _ in range(workers_number)]
                                        # ) for i in range(1)]

        self.logger.info('calling start to all validators')
        for w in validators:
            w.start()

        self.logger.info('loader worker process with %d processes', loaders_number)
        loaders = [LoaderWorker(store_q,
                                None,
                                chunk_size=max_loader_chunk_size,
                                dry_run=dry_run
                                ) for _ in range(loaders_number)]
        for w in loaders:
            w.start()
        #
        # 'Audit the whole process and send e-mails'
        # auditor = AuditTrailProcess(
        #     audit_q,
        #     None,
        #     self.es,
        #     lookup_data=lookup_data,
        #     dry_run=dry_run,
        #     )
        #
        # auditor.start()

        self.logger.info('file processer started')
        'Start crawling the files'
        file_processer = FileProcesser(file_q,
                                       self.es,
                                       self.r_server,
                                       input_files,
                                       dry_run=dry_run,
                                       increment=increment)

        processed_datasources = file_processer.run()

        # wait for the validator workers to finish

        self.logger.info('collecting loaders')
        for w in loaders:
            w.join()

        self.logger.info('collecting validators')
        for w in validators:
            w.join()

        self.logger.info('collecting readers')
        for w in readers:
            w.join()
        # audit_q.set_submission_finished(self.r_server)

        # auditor.join()

        if not dry_run:
            file_processer.loader.restore_after_bulk_indexing()
            # for datasource in processed_datasources:
            #     self.logger.debug('flushing index for dataource %s'%datasource)
            #     self.es.indices.flush(self.es_loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'-'+datasource),
            #                           wait_if_ongoing=True)

        self.logger.info('collecting reporter')
        q_reporter.join()
        return

    def reset(self):
        audit_index_name = Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME)
        if self.es.indices.exists(audit_index_name):
            self.es.indices.delete(audit_index_name)
        data_indices = Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*')
        self.es.indices.delete(data_indices)
        self.logger.info('Validation data deleted')
