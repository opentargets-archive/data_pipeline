from __future__ import division

import copy
import gzip
import hashlib
import io
import json
import logging
import multiprocessing
import os
import re
import sys
import time
from cStringIO import StringIO
from datetime import datetime, date

import opentargets.model.core as opentargets
import pysftp
import requests
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from requests.packages.urllib3.exceptions import HTTPError
from tqdm import tqdm

from common import Actions
from common.ElasticsearchLoader import Loader, LoaderWorker
from common.ElasticsearchQuery import ESQuery
from  common.EvidenceJsonUtils import DatatStructureFlattener
from common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from settings import Config

# This bit is necessary for text mining data
reload(sys)
sys.setdefaultencoding("utf8")

__author__ = 'gautierk'

logging.getLogger("paramiko").setLevel(logging.WARNING)


BLOCKSIZE = 65536
NB_JSON_FILES = 3
MAX_NB_EVIDENCE_CHUNKS = 1000*multiprocessing.cpu_count()
EVIDENCESTRING_VALIDATION_CHUNK_SIZE = 1

EVIDENCE_STRING_INVALID = 10
EVIDENCE_STRING_INVALID_TYPE = 11
EVIDENCE_STRING_INVALID_SCHEMA_VERSION = 12
EVIDENCE_STRING_INVALID_MISSING_TYPE = 13
EVIDENCE_STRING_INVALID_MISSING_TARGET = 14
EVIDENCE_STRING_INVALID_MISSING_DISEASE = 15
EVIDENCE_STRING_INVALID_UNPARSABLE_JSON = 16
ENSEMBL_GENE_ID_UNKNOWN = 20
ENSEMBL_GENE_ID_ALTERNATIVE_SEQUENCE = 21
UNIPROT_PROTEIN_ID_UNKNOWN = 30
UNIPROT_PROTEIN_ID_MISSING_ENSEMBL_XREF = 31
UNIPROT_PROTEIN_ID_ALTERNATIVE_ENSEMBL_XREF = 32
DISEASE_ID_INVALID = 40
DISEASE_ID_OBSOLETE = 41

TOP_20_TARGETS_QUERY = {  "size": 0, "aggs" : {  "group_by_targets" : {   "terms" : { "field" : "target_id", "order" : { "_count" : "desc" }, "size": 20 } } } }
TOP_20_DISEASES_QUERY = {  "size": 0, "aggs" : {  "group_by_diseases" : {   "terms" : { "field" : "disease_id", "order" : { "_count" : "desc" }, "size": 20 } } } }
DISTINCT_TARGETS_QUERY = {  "size": 0, "aggs" : {  "distinct_targets" : {  "cardinality" : { "field" : "target_id" } } } }
DISTINCT_DISEASES_QUERY = {  "size": 0, "aggs" : {  "distinct_diseases" : {  "cardinality" : { "field" : "disease_id" } } } }
SUBMISSION_FILTER_FILENAME_QUERY = '''
{
  "query": {
    "filtered": {
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
    "filtered": {
      "filter": {
        "terms" : { "md5": ["%s"]}
      }
    }
  }
}

'''



from time import strftime

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


class DirectoryCrawlerProcess():
    def __init__(self,
                 output_q,
                 es,
                 r_server,
                 local_files=[],
                 remote_files=[],
                 dry_run=False,
                 increment=False):
        self.output_q = output_q
        self.es = es
        self.loader = Loader(dry_run = dry_run)
        self.submission_audit = SubmissionAuditElasticStorage(self.loader)
        self.start_time = time.time()
        self.r_server = r_server
        self._remote_filenames =dict()
        self.local_files = local_files
        self.remote_files = remote_files
        self.logger = logging.getLogger(__name__)
        self.dry_run = dry_run,
        self.increment = increment

    def _store_remote_filename(self, filename):
        if re.match(Config.EVIDENCEVALIDATION_FILENAME_REGEX, filename):
            try:
                version_name = filename.split('/')[3].split('.')[0]
                if '-' in version_name:
                    user, day, month, year = version_name.split('-')
                    if '_' in user:
                        datasource = ''.join(user.split('_')[1:])
                        user = user.split('_')[0]
                    else:
                        datasource = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[user]
                    release_date = date(int(year),int(month), int(day))
                    if user not in self._remote_filenames:
                        self._remote_filenames[user]={datasource : dict(date = release_date,
                                                                          file_path = filename,
                                                                          file_version = version_name)
                                                      }
                    elif datasource not in self._remote_filenames[user]:
                        self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                        file_path=filename,
                                                                        file_version=version_name)
                    else:
                        if release_date > self._remote_filenames[user][datasource]['date']:
                            self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                file_path=filename,
                                                                file_version=version_name)
            except Exception as e:
                self.logger.error('error getting remote file %s: %s'%(filename, e))

    def _callback_not_used(self, path):
        self.logger.debug("skipped "+path)

    def run(self):
        '''
        create index for"
         the evidence if it does not exists
         if the evidence index exists, the index
         the submitted files if it does not exists
        '''

        self.logger.info("%s started" % self.__class__.__name__)


        SubmissionAuditElasticStorage.storage_create_index(self.loader,recreate=False)

        '''scroll through remote  user directories and find the latest files'''

        if self.remote_files:
            for url in self.remote_files:
                f = url.split('/')[-1]
                if re.match(Config.EVIDENCEVALIDATION_FILENAME_REGEX, f):
                    version_name = f.split('/')[-1].split('.')[0]
                    if '-' in version_name:
                        user, day, month, year = version_name.split('-')
                        if '_' in user:
                            datasource = ''.join(user.split('_')[1:])
                            user = user.split('_')[0]
                        else:
                            datasource = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[user]
                        'Recreate the index if we are not in dry run mode and not in increment mode'
                        self.loader.create_new_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '-' + datasource,
                                                recreate= not self.increment)

                        try:
                            logfile = os.path.join(Config.TEMP_DIR, version_name + ".log")
                            self.check_file(url, version_name, user, datasource, None, logfile, FileTypes.HTTP)
                            self.logger.debug("%s %s DONE" % (self.__class__.__name__, version_name))

                        except AttributeError as e:
                            self.logger.error("%s Error checking file %s: %s" % (self.__class__.__name__, f, e))
                else:
                    raise AttributeError(
                        'invalid filename, should match regex: %s' % Config.EVIDENCEVALIDATION_FILENAME_REGEX)

        if self.local_files:
            for f in self.local_files:
                if re.match(Config.EVIDENCEVALIDATION_FILENAME_REGEX, f):
                    version_name = f.split('/')[-1].split('.')[0]
                    if '-' in version_name:
                        user, day, month, year = version_name.split('-')
                        if '_' in user:
                            datasource = ''.join(user.split('_')[1:])
                            user = user.split('_')[0]
                        else:
                            datasource = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[user]
                        self.loader.create_new_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '-' + datasource,
                                                recreate= not self.increment)
                        try:
                            ''' get md5 '''
                            md5_hash = self.md5_hash_local_file(f)
                            self.logger.debug("%s %s %s" % (self.__class__.__name__, f, md5_hash))
                            logfile = os.path.join(Config.TEMP_DIR, version_name + ".log")
                            self.check_file(f, version_name, user, datasource, md5_hash, logfile, FileTypes.LOCAL)
                            self.logger.debug("%s %s DONE" % (self.__class__.__name__, version_name))

                        except AttributeError as e:
                            self.logger.error("%s Error checking file %s: %s" % (self.__class__.__name__, f, e))
                else:
                    raise AttributeError('invalid filename, should match regex: %s'%Config.EVIDENCEVALIDATION_FILENAME_REGEX)



        self.output_q.set_submission_finished(self.r_server)

        self.logger.info("%s finished" % self.__class__.__name__)

    def md5_hash_local_file(self, filename):
        return self.md5_hash_from_file_stat(os.stat(filename))

    def md5_hash_remote_file(self, filename, srv):
        return self.md5_hash_from_file_stat(srv.stat(filename))

    def md5_hash_from_file_stat(self, file_stats):
        hasher = hashlib.md5()
        hasher.update(str(file_stats.st_size))
        hasher.update(str(file_stats.st_mtime))
        return hasher.hexdigest()

    def check_file(self,
                   file_path,
                   file_version,
                   provider_id,
                   data_source_name,
                   md5_hash = None,
                   logfile=None,
                   file_type= FileTypes.LOCAL,
                   validate=True,

                   ):
        '''check if the file was already parsed in ES'''

        if md5_hash:
            existing_submission= self.submission_audit.get_submission_md5(md5=md5_hash)

            if existing_submission:
                md5 = existing_submission["_source"]["md5"]
                if md5 == md5_hash:
                        self.logger.info('%s file already recorded. Won\'t parse' % file_path)
                        ''' should be false, set to true if you want to parse anyway '''
                        validate = Config.EVIDENCEVALIDATION_FORCE_VALIDATION
                else:
                    self.logger.info('file recorded with a different hash %s, revalidatiing.' % (md5))
                    validate = True
                    self.logger.info('validate for file %s %r' % (file_version,validate))
        else:
            existing_submission = False

        if validate == True:

            ''' reset information in submission_audit index '''

            now = datetime.now().strftime("%Y%m%dT%H%M%SZ")

            if existing_submission:
                ''' update '''
                submission = dict(
                        md5 = md5_hash,
                        nb_submission=existing_submission["_source"]["nb_submission"]+1,
                        nb_passed_validation=0,
                        nb_records = 0,
                        nb_errors = 0,
                        nb_duplicates = 0,
                        date_modified = now,
                        date_validated = now,
                        successfully_validated = False
                )

                self.submission_audit.storage_insert_or_update(submission, update=True)

            else:
                ''' insert '''
                submission=dict(
                    md5=md5_hash,
                    provider_id=provider_id,
                    data_source_name=data_source_name,
                    filename=file_path,
                    date_created=now,
                    date_modified=now,
                    date_validated=now,
                    nb_submission=1,
                    nb_passed_validation=0,
                    nb_records=0,
                    nb_errors=0,
                    nb_duplicates=0,
                    successfully_validated=False
                )

                self.submission_audit.storage_insert_or_update(submission, update=False)

            self.output_q.put((file_path, file_version, provider_id, data_source_name, md5_hash, logfile, file_type),
                              self.r_server)

        return


class FileReaderProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out=None,
                 es = None):
        super(FileReaderProcess, self).__init__(queue_in, redis_path, queue_out)
        self.es = es
        self.loader = Loader(self.es)
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        file_path, file_version, provider_id, data_source_name, md5_hash, logfile, file_type = data
        self.logger.info('Starting to parse  file %s' % file_path)
        ''' parse the file and put evidence in the queue '''
        self.parse_gzipfile(file_path, file_version, provider_id, data_source_name, md5_hash,
                            logfile=logfile, file_type= file_type)
        self.logger.info("%s finished" % self.name)



    def parse_gzipfile(self, file_path, file_version, provider_id, data_source_name, md5_hash, logfile=None, file_type = FileTypes.LOCAL):



        self.logger.info('%s Starting parsing %s' % (self.name, file_path))

        line_buffer = []
        offset = 0
        chunk = 1
        line_number = 0

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # disable host key checking.
        if file_path.endswith('.gz'):
            if file_type == FileTypes.LOCAL:
                file_stat = os.stat(file_path)
                file_size, file_mod_time = file_stat.st_size, file_stat.st_mtime
                '''temprorary get lines total'''
                lines = 0
                with open(file_path, mode='rb') as f:
                    with io.BufferedReader(gzip.GzipFile(filename=file_path.split('/')[1],
                                       mode='rb',
                                       fileobj=f,
                                       mtime=file_mod_time)) as fh:
                        lines = self._count_file_lines(fh)
                total_chunks = lines/EVIDENCESTRING_VALIDATION_CHUNK_SIZE
                if lines % EVIDENCESTRING_VALIDATION_CHUNK_SIZE:
                    total_chunks +=1
                self.queue_out.incr_total(int(round(total_chunks)), self.r_server)
                with open(file_path, mode='rb',) as f:
                    with io.BufferedReader(gzip.GzipFile(filename = file_path.split('/')[1],
                                       mode = 'rb',
                                       fileobj = f,
                                       mtime = file_mod_time)) as fh:
                        for i, line in enumerate(fh.readlines()):
                            self.put_into_queue_out(
                                (file_path, file_version, provider_id, data_source_name, md5_hash, logfile,
                                 i / EVIDENCESTRING_VALIDATION_CHUNK_SIZE,
                                 offset, [line], False))
            elif file_type == FileTypes.HTTP:
                '''temprorary get lines total'''
                response = requests.get(file_path, stream=True)
                response.raise_for_status()
                file_size = int(response.headers['content-length'])
                file_handler = StringIO()
                t = tqdm(desc='downloading %s via HTTP' % file_version,
                         total=file_size,
                         unit='B',
                         unit_scale=True)
                for remote_file_chunk in response.iter_content(chunk_size=512):
                    file_handler.write(remote_file_chunk)
                    t.update(len(remote_file_chunk))
                try:
                    self.logger.debug('Downloaded file %s from HTTP at %.2fMB/s' % (
                        file_version, (file_size / 1e6) / (t.last_print_t - t.start_t)))
                except ZeroDivisionError:
                    self.logger.debug('Downloaded file %s from HTTP' % file_path)
                t.close()
                response.close()
                file_handler.seek(0)
                with io.BufferedReader(gzip.GzipFile(filename=file_version.split('/')[-1],
                                                     mode='rb',
                                                     fileobj=file_handler,
                                                     )) as fh:
                    lines = self._count_file_lines(fh)
                file_handler.seek(0)
                total_chunks = lines / EVIDENCESTRING_VALIDATION_CHUNK_SIZE
                if lines % EVIDENCESTRING_VALIDATION_CHUNK_SIZE:
                    total_chunks += 1
                self.queue_out.incr_total(int(round(total_chunks)), self.r_server)
                with io.BufferedReader(gzip.GzipFile(filename=file_version.split('/')[-1],
                                                     mode='rb',
                                                     fileobj=file_handler,
                                                     )) as fh:
                    for i,line in enumerate(fh.readlines()):
                        self.put_into_queue_out(
                            (file_path, file_version, provider_id, data_source_name, md5_hash, logfile,
                             i / EVIDENCESTRING_VALIDATION_CHUNK_SIZE,
                             offset, [line], False))

        elif file_path.endswith('.json'):
            if file_type == FileTypes.LOCAL:
                with open(file_path) as fh:
                    lines = self._count_file_lines(fh)
                    total_chunks = lines / EVIDENCESTRING_VALIDATION_CHUNK_SIZE
                    if lines % EVIDENCESTRING_VALIDATION_CHUNK_SIZE:
                        total_chunks += 1
                    self.queue_out.incr_total(int(round(total_chunks)), self.r_server)
                    fh.seek(0)
                    for i,line in enumerate(fh.readlines()):
                        self.put_into_queue_out(
                            (file_path, file_version, provider_id, data_source_name, md5_hash, logfile,
                             i/EVIDENCESTRING_VALIDATION_CHUNK_SIZE,
                             offset, [line], False))
            if file_type == FileTypes.HTTP:
                response = requests.get(file_path, stream=True)
                response.raise_for_status()
                file_size = int(response.headers['content-length'])
                t = tqdm(desc='downloading %s via HTTP' % file_version,
                         total=file_size,
                         unit='B',
                         unit_scale=True)
                for i,line in enumerate(response.iter_lines()):
                    t.update(len(line))
                    self.put_into_queue_out(
                        (file_path, file_version, provider_id, data_source_name, md5_hash, logfile,
                         i/EVIDENCESTRING_VALIDATION_CHUNK_SIZE,
                         offset, [line], False))
                t.close()
                response.close()
                try:
                    self.logger.debug('Downloaded file %s from HTTP at %.2fMB/s' % (
                        file_version, (file_size / 1e6) / (t.last_print_t - t.start_t)))
                except ZeroDivisionError:
                    self.logger.debug('Downloaded file %s from HTTP' % file_path)




        else:
            raise AttributeError('File %s is not supported'%file_type)
        #
        # if line_buffer:
        #     self.queue_out.put((file_path, file_version, provider_id, data_source_name, md5_hash, logfile, chunk, offset,
        #                        list(line_buffer), False),
        #                        self.r_server)
        #     offset += len(line_buffer)
        #     chunk += 1
        #     line_buffer = []


        '''
        finally send a signal to inform that parsing is completed for this file and no other line are to be expected
        '''
        self.logger.info('%s %s %i %i' % (self.name, md5_hash, offset, len(line_buffer)))
        self.queue_out.put((file_path, file_version, provider_id, data_source_name, md5_hash, logfile, chunk, offset,
                           [], True),
                           self.r_server)

        return

    def _count_file_lines(self, f):
        for i,line in enumerate(f.readlines()):
            pass
        return i+1

    def _estimate_file_lines(self, fh, file_size, max_lines = 50000):
        lines = 0
        size = 0
        line = fh.readline()
        while line:
            lines += 1
            size += len(line)
            line = fh.readline()
            if lines > max_lines:
                return int(round(file_size/(float(size)/max_lines)))
        return lines


class ValidatorProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out=None,
                 es=None,
                 lookup_data= None,
                 dry_run=False,
                 increment=False,
                 queue_storage = None,
                 ):
        super(ValidatorProcess, self).__init__(queue_in, redis_path, queue_out)
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.queue_storage = queue_storage
        self.es = es
        self.loader= Loader(dry_run=dry_run, chunk_size=1000)
        self.lookup_data = lookup_data
        self.start_time = time.time()
        self.audit = list()
        self.logger = logging.getLogger(__name__)

    def process(self, data):
        file_path, file_version, provider_id, data_source_name, md5_hash, logfile, chunk, offset, line_buffer, end_of_transmission = data
        return self.validate_evidence(file_path, file_version, provider_id, data_source_name, md5_hash, chunk,
                                   offset, line_buffer, end_of_transmission, logfile=logfile)

    # @profile
    def validate_evidence(self, file_path, file_version, provider_id, data_source_name, md5_hash, chunk, offset,
                          line_buffer, end_of_transmission, logfile=None):
        '''
        validate evidence strings from a chunk
        cumulate the logs, acquire a lock,
        write the logs
        write the data to the database
        '''

        if end_of_transmission:
            logging.info("%s Validation of %s completed" % (self.name, file_path))
            '''
            Send a message to the audit trail process with the md5 key of the file
            '''

            return (file_path,
                   file_version,
                   provider_id,
                   data_source_name,
                   md5_hash,
                   chunk,
                   dict(nb_lines=0,
                        nb_valid=0,
                        nb_efo_invalid=0,
                        nb_efo_obsolete=0,
                        nb_ensembl_invalid=0,
                        nb_ensembl_nonref=0,
                        nb_uniprot_invalid=0,
                        nb_missing_uniprot_id_xrefs=0,
                        nb_uniprot_invalid_mapping=0,
                        invalid_diseases={},
                        obsolete_diseases={},
                        invalid_ensembl_ids={},
                        nonref_ensembl_ids={},
                        invalid_uniprot_ids={},
                        missing_uniprot_id_xrefs={},
                        invalid_uniprot_id_mappings={},
                        nb_errors=0,
                        nb_duplicates=0),
                   list(),
                   offset,
                   len(line_buffer),
                   logfile,
                   end_of_transmission)


        else:

            diseases = {}
            top_diseases = []
            targets = {}
            top_targets = []
            obsolete_diseases = {}
            invalid_diseases = {}
            invalid_ensembl_ids = {}
            nonref_ensembl_ids = {}
            invalid_uniprot_ids = {}
            missing_uniprot_id_xrefs = {}
            invalid_uniprot_id_mappings = {}

            cc = 0
            lc = offset
            nb_valid = 0
            nb_errors = 0
            nb_duplicates = 0
            nb_efo_invalid = 0
            nb_efo_obsolete = 0
            nb_ensembl_invalid = 0
            nb_ensembl_nonref = 0
            nb_uniprot_invalid = 0
            nb_missing_uniprot_id_xrefs = 0
            nb_uniprot_invalid_mapping = 0
            audit = list()


            for line in line_buffer:
                lc += 1
                try:
                    python_raw = json.loads(line)
                except Exception as e:
                    self.logger.error('cannot parse line %i: %s'%(lc, e))
                    audit.append((lc, EVIDENCE_STRING_INVALID_UNPARSABLE_JSON, None))
                    nb_errors += 1
                    continue


                # now validate
                obj = None
                disease_failed = False
                gene_failed = False
                target_id = None
                efo_id = None

                if (('label' in python_raw or
                             'type' in python_raw) and
                            'validated_against_schema_version' in python_raw and
                            python_raw['validated_against_schema_version'] == Config.EVIDENCEVALIDATION_SCHEMA):
                    if 'label' in python_raw:
                        python_raw['type'] = python_raw.pop('label', None)
                    data_type = python_raw['type']
                    if data_type in Config.EVIDENCEVALIDATION_DATATYPES:
                        if data_type == 'genetic_association':
                            obj = opentargets.Genetics.fromMap(python_raw)
                        elif data_type == 'rna_expression':
                            obj = opentargets.Expression.fromMap(python_raw)
                        elif data_type in ['genetic_literature', 'affected_pathway', 'somatic_mutation']:
                            obj = opentargets.Literature_Curated.fromMap(python_raw)
                            if data_type == 'somatic_mutation' and not isinstance(python_raw['evidence']['known_mutations'], list):
                                mutations = copy.deepcopy(python_raw['evidence']['known_mutations'])
                                python_raw['evidence']['known_mutations'] = [ mutations ]
                                # self.logger.error(json.dumps(python_raw['evidence']['known_mutations'], indent=4))
                                obj = opentargets.Literature_Curated.fromMap(python_raw)
                        elif data_type == 'known_drug':
                            obj = opentargets.Drug.fromMap(python_raw)
                        elif data_type == 'literature':
                            obj = opentargets.Literature_Mining.fromMap(python_raw)
                        elif data_type == 'animal_model':
                            obj = opentargets.Animal_Models.fromMap(python_raw)


                        if obj is not None:
                            if obj.target.id:
                                target_id = obj.target.id
                                if target_id in targets:
                                    targets[target_id] += 1
                                else:
                                    targets[target_id] = 1
                                if not target_id in top_targets:
                                    if len(top_targets) < Config.EVIDENCEVALIDATION_NB_TOP_TARGETS:
                                        top_targets.append(target_id)
                                    else:
                                        # map,reduce
                                        for n in range(0, len(top_targets)):
                                            if targets[top_targets[n]] < targets[target_id]:
                                                top_targets[n] = target_id;
                                                break;

                            if obj.disease.id:
                                efo_id = obj.disease.id
                                if efo_id in diseases:
                                    diseases[efo_id] += 1
                                else:
                                    diseases[efo_id] = 1
                                if not efo_id in top_diseases:
                                    if len(top_diseases) < Config.EVIDENCEVALIDATION_NB_TOP_DISEASES:
                                        top_diseases.append(efo_id)
                                    else:
                                        # map,reduce
                                        for n in range(0, len(top_diseases)):
                                            if diseases[top_diseases[n]] < diseases[efo_id]:
                                                top_diseases[n] = efo_id
                                                break

                            # flatten
                            uniq_elements = obj.unique_association_fields
                            uniq_elements_flat = DatatStructureFlattener(uniq_elements)
                            uniq_elements_flat_hexdig = uniq_elements_flat.get_hexdigest()

                            '''
                            VALIDATE EVIDENCE STRING
                            This will return errors and
                            will log the errors in the chosen logger
                            '''
                            validation_result = obj.validate(self.logger)
                            nb_errors += validation_result

                            '''
                            CHECK DISEASE IDENTIFIER
                            Now check that the disease IRI is a valid disease term
                            There is only one disease to look at
                            '''
                            if efo_id:
                                short_disease_id = efo_id.split('/')[-1]
                                ' Check disease term or phenotype term '
                                #if (short_disease_id not in self.lookup_data.available_efos) and \
                                if (efo_id not in self.lookup_data.efo_ontology.current_classes) and \
                                        (efo_id not in self.lookup_data.hpo_ontology.current_classes) and \
                                        (efo_id not in self.lookup_data.mp_ontology.current_classes):# or \
                                        # (disease_id in self.efo_uncat):
                                    audit.append((lc, DISEASE_ID_INVALID, efo_id))
                                    disease_failed = True
                                    if efo_id not in invalid_diseases:
                                        invalid_diseases[efo_id] = 1
                                    else:
                                        invalid_diseases[efo_id] += 1
                                    nb_efo_invalid += 1
                                if efo_id in self.lookup_data.efo_ontology.obsolete_classes:
                                     audit.append((lc, DISEASE_ID_OBSOLETE, efo_id))
                                     # self.logger.error("Line {0}: Obsolete disease term detected {1} ('{2}'): {3}".format(lc+1, disease_id, self.efo_current[disease_id], self.efo_obsolete[disease_id]))
                                     disease_failed = True
                                     if efo_id not in obsolete_diseases:
                                         obsolete_diseases[efo_id] = 1
                                     else:
                                         obsolete_diseases[efo_id] += 1
                                     nb_efo_obsolete += 1
                                elif (efo_id in self.lookup_data.hpo_ontology.obsolete_classes) or \
                                        (efo_id in self.lookup_data.mp_ontology.obsolete_classes):
                                    audit.append((lc, DISEASE_ID_OBSOLETE, efo_id))
                                    # self.logger.error("Line {0}: Obsolete disease term detected {1} ('{2}'): {3}".format(lc+1, disease_id, self.efo_current[disease_id], self.efo_obsolete[disease_id]))
                                    disease_failed = True
                                    if efo_id not in obsolete_diseases:
                                        obsolete_diseases[efo_id] = 1
                                    else:
                                        obsolete_diseases[efo_id] += 1
                                    nb_efo_obsolete += 1

                            else:
                                ''' no disease id !!!! '''
                                audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_DISEASE))
                                disease_failed = True

                            '''
                            CHECK GENE/PROTEIN IDENTIFIER
                            Check Ensembl ID, UniProt ID and UniProt ID mapping to a Gene ID
                            http://identifiers.org/ensembl/ENSG00000178573
                            '''
                            if target_id:
                                ensemblMatch = re.match('http://identifiers.org/ensembl/(ENSG\d+)', target_id)
                                uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', target_id)
                                if ensemblMatch:
                                    # ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
                                    ensembl_id = target_id.split('/')[-1]
                                    if not ensembl_id in self.lookup_data.available_genes:
                                        gene_failed = True
                                        audit.append((lc, ENSEMBL_GENE_ID_UNKNOWN, ensembl_id))
                                        # self.logger.error("Line {0}: Unknown Ensembl gene detected {1}. Please provide a correct gene identifier on the reference genome assembly {2}".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                        if not ensembl_id in invalid_ensembl_ids:
                                            invalid_ensembl_ids[ensembl_id] = 1
                                        else:
                                            invalid_ensembl_ids[ensembl_id] += 1
                                        nb_ensembl_invalid += 1
                                    elif ensembl_id in self.lookup_data.non_reference_genes:
                                        gene_mapping_failed = True
                                        audit.append((lc, ENSEMBL_GENE_ID_ALTERNATIVE_SEQUENCE, ensembl_id))
                                        # self.logger.warning("Line {0}: Human Alternative sequence Ensembl Gene detected {1}. We will attempt to map it to a gene identifier on the reference genome assembly {2} or choose a Human Alternative sequence Ensembl Gene Id".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                        if not ensembl_id in invalid_ensembl_ids:
                                            nonref_ensembl_ids[ensembl_id] = 1
                                        else:
                                            nonref_ensembl_ids[ensembl_id] += 1
                                        nb_ensembl_nonref += 1

                                elif uniprotMatch:
                                    # uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                                    uniprot_id =  target_id.split('/')[-1]
                                    if uniprot_id not in self.lookup_data.uni2ens:
                                        gene_failed = True
                                        audit.append((lc, UNIPROT_PROTEIN_ID_UNKNOWN, uniprot_id))
                                        # self.logger.error("Line {0}: Invalid UniProt entry detected {1}. Please provide a correct identifier".format(lc+1, uniprot_id))
                                        if uniprot_id not in invalid_uniprot_ids:
                                            invalid_uniprot_ids[uniprot_id] = 1
                                        else:
                                            invalid_uniprot_ids[uniprot_id] += 1
                                        nb_uniprot_invalid += 1
                                    elif not self.lookup_data.uni2ens[uniprot_id]:#TODO:this will not happen wit the current gene processing pipeline
                                        # check symbol mapping (get symbol first)
                                        # gene_mapping_failed = True
                                        gene_failed = True
                                        audit.append((lc, UNIPROT_PROTEIN_ID_MISSING_ENSEMBL_XREF, uniprot_id))
                                        # self.logger.warning("Line {0}: UniProt entry {1} does not have any cross-reference to Ensembl.".format(lc+1, uniprot_id))
                                        if not uniprot_id in missing_uniprot_id_xrefs:
                                            missing_uniprot_id_xrefs[uniprot_id] = 1
                                        else:
                                            missing_uniprot_id_xrefs[uniprot_id] += 1
                                        nb_missing_uniprot_id_xrefs += 1
                                        # This identifier is not in the current EnsEMBL database
                                    elif (uniprot_id in self.lookup_data.uni2ens) and  \
                                                    self.lookup_data.uni2ens[uniprot_id] in self.lookup_data.available_genes  and \
                                                    'is_reference' in  self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]  and \
                                                    (not self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True) :
                                        audit.append((lc, UNIPROT_PROTEIN_ID_ALTERNATIVE_ENSEMBL_XREF, uniprot_id))
                                        # self.logger.warning("Line {0}: The UniProt entry {1} does not have a cross-reference to an Ensembl Gene Id on the reference genome assembly {2}. It will be mapped to a Human Alternative sequence Ensembl Gene Id.".format(lc+1, uniprot_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                        if not uniprot_id in invalid_uniprot_id_mappings:
                                            invalid_uniprot_id_mappings[uniprot_id] = 1
                                        else:
                                            invalid_uniprot_id_mappings[uniprot_id] += 1
                                        nb_uniprot_invalid_mapping += 1
                                    else:
                                        try:
                                            reference_target_list =self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True
                                        except KeyError:
                                            reference_target_list = []
                                        if reference_target_list:
                                            target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[
                                                0]
                                        else:
                                            # get the first one, needs a better way
                                            target_id = self.lookup_data.uni2ens[uniprot_id]
                                        # self.logger.info("Found target id being: %s for %s" %(target_id, uniprot_id))
                                        if target_id is None:
                                            self.logger.info("Found no target id for %s" % (uniprot_id))

                            '''
                            If there is no target id after the processing step
                            '''
                            if obj.target.id is None or target_id is None:
                                ''' no target id !!!! '''
                                audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_TARGET))
                                gene_failed = True
                                nb_errors += 1

                            '''
                            STORE THE EVIDENCE
                            '''
                            if validation_result == 0 and not disease_failed and not gene_failed:
                                nb_valid += 1
                                json_doc_hashdig = DatatStructureFlattener(python_raw).get_hexdigest()
                                loader_args = (Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '-' + data_source_name,
                                               data_source_name,
                                               uniq_elements_flat_hexdig,
                                               dict(
                                                   uniq_assoc_fields_hashdig=uniq_elements_flat_hexdig,
                                                   json_doc_hashdig=json_doc_hashdig,
                                                   evidence_string=line,
                                                   target_id=target_id,
                                                   disease_id=efo_id,
                                                   data_source_name=data_source_name,
                                                   json_schema_version=Config.EVIDENCEVALIDATION_SCHEMA,
                                                   json_doc_version=1,
                                                   release_date=VALIDATION_DATE))
                                loader_kwargs = dict(create_index=False)
                                self.queue_storage.put((loader_args, loader_kwargs), r_server=self.r_server)

                            else:
                                if disease_failed:
                                    nb_errors += 1
                                if gene_failed:
                                    nb_errors += 1
                        else:
                            audit.append((lc, EVIDENCE_STRING_INVALID))
                            nb_errors += 1

                    else:
                        audit.append((lc, EVIDENCE_STRING_INVALID_TYPE, data_type))
                        nb_errors += 1

                elif (not 'validated_against_schema_version' in python_raw or
                          ('validated_against_schema_version' in python_raw and
                                   python_raw['validated_against_schema_version'] != Config.EVIDENCEVALIDATION_SCHEMA
                           )
                      ):
                    audit.append((lc, EVIDENCE_STRING_INVALID_SCHEMA_VERSION))
                    self.logger.error("Line %i: Not a valid %s evidence string - please check the 'validated_against_schema_version' mandatory attribute"%(lc+1, Config.EVIDENCEVALIDATION_SCHEMA))
                    nb_errors += 1
                else:
                    ''' type '''
                    audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_TYPE))
                    self.logger.error(
                        "Line %i: Not a valid %s evidence string - please add the mandatory 'type' attribute"%(lc+1, Config.EVIDENCEVALIDATION_SCHEMA))
                    nb_errors += 1

                cc += len(line)




            'Inform the audit trailer to generate a report and send an e-mail to the data provider'
            return (file_path,
                   file_version,
                   provider_id,
                   data_source_name,
                   md5_hash,
                   chunk,
                   dict(nb_lines=lc,
                        nb_valid=nb_valid,
                        nb_efo_invalid=nb_efo_invalid,
                        nb_efo_obsolete=nb_efo_obsolete,
                        nb_ensembl_invalid=nb_ensembl_invalid,
                        nb_ensembl_nonref=nb_ensembl_nonref,
                        nb_uniprot_invalid=nb_uniprot_invalid,
                        nb_missing_uniprot_id_xrefs=nb_missing_uniprot_id_xrefs,
                        nb_uniprot_invalid_mapping=nb_uniprot_invalid_mapping,
                        invalid_diseases=invalid_diseases,
                        obsolete_diseases=obsolete_diseases,
                        invalid_ensembl_ids=invalid_ensembl_ids,
                        nonref_ensembl_ids=nonref_ensembl_ids,
                        invalid_uniprot_ids=invalid_uniprot_ids,
                        missing_uniprot_id_xrefs=missing_uniprot_id_xrefs,
                        invalid_uniprot_id_mappings=invalid_uniprot_id_mappings,
                        nb_errors=nb_errors,
                        nb_duplicates=nb_duplicates),
                   audit,
                   offset,
                   len(line_buffer),
                   logfile,
                   end_of_transmission)

    def close(self):
        super(ValidatorProcess, self).close()
        if hasattr(self, 'loader') and self.loader is not None:
            self.loader.close()




class AuditTrailProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 es=None,
                 lookup_data = None,
                 dry_run=False,
                 ):
        super(AuditTrailProcess, self).__init__(queue_in, redis_path )
        self.queue_in = queue_in
        self.es = es
        self.submission_audit = SubmissionAuditElasticStorage(loader=Loader(es, dry_run=dry_run))
        self.lookup_data = lookup_data
        self.start_time = time.time()
        self.registry = dict()
        self.logger = logging.getLogger(__name__)

    def process(self, data):
        file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, stats, audit, offset, buffer_size, \
        logfile, end_of_transmission = data
        self.audit_submission(file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, stats,
                              audit, offset, buffer_size, end_of_transmission, logfile)


    def send_email(self, provider_id, data_source_name, filename, bValidated, nb_records, errors, when, extra_text, logfile):
        sender = Config.EVIDENCEVALIDATION_SENDER_ACCOUNT
        recipient =Config.EVIDENCEVALIDATION_PROVIDER_EMAILS[provider_id]
        status = "passed"
        if not bValidated:
            status = "failed"


        text = ["This is an automated message generated by the Open Targets Core Platform Pipeline on {0}".format(when)]
        if bValidated:
            text.append(messagePassed)
            text.append("Congratulations :)")
        else:
            text.append(messageFailed)
            text.append("See details in the attachment {0}\n".format(os.path.basename(logfile)))
        text.append("Data Provider:\t%s"%data_source_name)
        text.append("JSON schema version:\t%s"%Config.EVIDENCEVALIDATION_SCHEMA)
        text.append("Number of records parsed:\t{0}".format(nb_records))
        for key in errors:
            text.append("Number of {0}:\t{1}".format(key, errors[key]))
        text.append("")
        text.append(extra_text)
        text.append( "\nYours\nThe Open Targets Core Platform Team")
        # text.append( signature
        text = '\n'.join(text)
        self.logger.info(text)
        if Config.EVIDENCEVALIDATION_SEND_EMAIL:
            r = requests.post(
                Config.MAILGUN_MESSAGES,
                auth=("api", Config.MAILGUN_API_KEY),
                files=[("attachment", open(logfile,'rb'))],
                data={"from": sender,
                      "to": "andreap@ebi.ac.uk",#recipient,
                      "bcc": Config.EVIDENCEVALIDATION_BCC_ACCOUNT,
                      "subject": "Open Targets: {0} validation {1} for {2}".format(data_source_name, status, filename),
                      "text": text,
                      # "html": "<html>HTML version of the body</html>"
                      },
                )
            try:
                r.raise_for_status()
                self.logger.info('Email sent to %s. Response: \n%s' % (recipient, r.text))
            except HTTPError, e:
                self.logger.error("Email not sent")
                self.logger.error(e)


        return

    def merge_dict_sum(self, x, y):
        # merge keys
        # x.update(y) won't work
        for key, value in y.items():
            if key in x:
                x[key] += value
            else:
                x[key] = value
        return x


    def get_reference_gene_from_Ensembl(self, ensembl_gene_id):
        '''
        Given an ensembl gene id return the corresponding reference assembly gene id if it exists.
        It get the gene external name and get the corresponding primary assembly gene id.
        :param self:
        :param ensembl_gene_id: ensembl gene identifier to check
        :return: a string indicating if the gene is mapped to a reference assembly or an alternative assembly only
        '''
        if ensembl_gene_id in self.lookup_data.available_genes:
            symbol = self.lookup_data.available_genes[ensembl_gene_id]['approved_symbol']
            return ensembl_gene_id + " " + symbol + " (reference assembly)"
        return ensembl_gene_id + " (non reference assembly)"

    def get_reference_gene_from_list(self, genes):
        '''
        Given a list of genes will return the gene that is mapped to the reference assembly
        :param genes: a list of ensembl gene identifiers
        :return: the ensembl gene identifier mapped to the reference assembly if it exists, the list of ensembl gene
        identifiers passed in the input otherwise
        '''
        for ensembl_gene_id in genes:
            gene = self.lookup_data.available_genes[ensembl_gene_id]
            if gene and 'is_ensembl_reference' in gene and gene['is_ensembl_reference'] is True:
                return ensembl_gene_id + " " + gene['approved_symbol'] + " (reference assembly)"
        return ", ".join(genes) + " (non reference assembly)"

    def write_logs(self, lfh, audit):

        for item in audit:
            if item[1] == DISEASE_ID_INVALID:
                # lc, DISEASE_ID_INVALID, disease_id
                lfh.write("Line %i: invalid disease %s\n" % (item[0], item[2]))
            elif item[1] == DISEASE_ID_OBSOLETE:
                # lc, DISEASE_ID_INVALID, disease_id
                lfh.write("Line %i: obsolete ontology class %s\n" % (item[0], item[2]))
            elif item[1] == EVIDENCE_STRING_INVALID_MISSING_DISEASE:
                lfh.write("Line %i: missing disease information\n" % (item[0]))
            elif item[1] == EVIDENCE_STRING_INVALID_SCHEMA_VERSION:
                lfh.write("Line %i: Not a valid 1.2.2 evidence string - please check the 'validated_against_schema_version' mandatory attribute\n" % (item[0]))
            # self.logger.error("Line {0}: Not a valid 1.2.1 evidence string - please check the 'validated_against_schema_version' mandatory attribute".format(lc+1))
            elif item[1] == EVIDENCE_STRING_INVALID:
                lfh.write("Line %i: Not a valid 1.2.2 evidence string - There was an error parsing the JSON document. The document may contain an invalid field\n" % (item[0]))
            elif item[1] == EVIDENCE_STRING_INVALID_MISSING_TYPE:
                lfh.write("Line %i: Not a valid 1.2.2 evidence string - please add the mandatory 'type' attribute\n" % (item[0]))
            elif item[1] == ENSEMBL_GENE_ID_UNKNOWN:
                lfh.write("Line %i: Unknown Ensembl gene detected %s. Please provide a correct gene identifier on the reference genome assembly %s\n" % (item[0], item[2], Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
            elif item[1] == ENSEMBL_GENE_ID_ALTERNATIVE_SEQUENCE:
                lfh.write("Line %i: Human Alternative sequence Ensembl Gene detected %s. We will attempt to map it to a gene identifier on the reference genome assembly %s or choose a Human Alternative sequence Ensembl Gene Id\n" % (item[0], item[2], Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
            elif item[1] == UNIPROT_PROTEIN_ID_UNKNOWN:
                lfh.write("Line %i: Invalid UniProt entry detected %s. Please provide a correct identifier\n" % (item[0], item[2]))
            elif item[1] == UNIPROT_PROTEIN_ID_MISSING_ENSEMBL_XREF:
                lfh.write("Line %i: UniProt entry %s does not have any cross-reference to Ensembl\n" % (item[0], item[2]))
            elif item[1] == UNIPROT_PROTEIN_ID_ALTERNATIVE_ENSEMBL_XREF:
                lfh.write("Line %i: The UniProt entry %s does not have a cross-reference to an Ensembl Gene Id on the reference genome assembly %s. It will be mapped to a Human Alternative sequence Ensembl Gene Id\n" % (item[0], item[2], Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
            elif item[1] == EVIDENCE_STRING_INVALID_MISSING_TARGET:
                lfh.write("Line %i: missing target information\n" % (item[0]))

    def audit_submission(self, file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, stats, audit,
                         offset, buffer_size, end_of_transmission, logfile=None):


        ''' check invalid disease and report it in the logs '''
        #for item in audit:
        #    if item[1] == DISEASE_ID_INVALID:
        #        # lc, DISEASE_ID_INVALID, disease_id
        #        self.logger.info("Line %i: invalid disease %s" % (item[0], item[2]))

        ''' it's the first time we hear about this submission '''
        if not md5_hash in self.registry:
            ''' open a file handler to write the logs '''
            self.registry[md5_hash] = dict(
                    current_index=0,
                    chunk_received=0,
                    chunk_expected=-1,
                    nb_lines=0,
                    chunks=list())

        if end_of_transmission:
            self.registry[md5_hash]['chunk_expected'] = chunk - 1
            self.registry[md5_hash]['nb_lines'] = offset
        else:
            self.registry[md5_hash]['chunk_received'] += 1
            '''
             Add the stats and audit to an array
            '''
            new_chunk = dict(chunk=chunk, stats=stats, audit=audit)
            self.registry[md5_hash]['chunks'].append(new_chunk)
            ''' TODO: write the audit results to the log file as they are received '''


        '''
        Generate the audit report if all audit parts have been received.
        '''
        if self.registry[md5_hash]['chunk_expected'] == self.registry[md5_hash]['chunk_received']:

            self.logger.info("%s generating report "%self.name)

            text = []

            '''
             refresh the indice
            '''
            self.es.indices.refresh(index=Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'-'+ data_source_name))

            '''
            Sum up numbers across chunks (map reduce)
            '''
            nb_efo_invalid = reduce((lambda x, y: x + y),
                                    map(lambda x: x['stats']['nb_efo_invalid'], self.registry[md5_hash]['chunks']))
            nb_ensembl_invalid = reduce((lambda x, y: x + y), map(lambda x: x['stats']['nb_ensembl_invalid'],
                                                                  self.registry[md5_hash]['chunks']))
            nb_efo_obsolete = reduce((lambda x, y: x + y),
                                     map(lambda x: x['stats']['nb_efo_obsolete'], self.registry[md5_hash]['chunks']))
            nb_ensembl_nonref = reduce((lambda x, y: x + y), map(lambda x: x['stats']['nb_ensembl_nonref'],
                                                                 self.registry[md5_hash]['chunks']))
            nb_uniprot_invalid = reduce((lambda x, y: x + y), map(lambda x: x['stats']['nb_uniprot_invalid'],
                                                                  self.registry[md5_hash]['chunks']))
            nb_missing_uniprot_id_xrefs = reduce((lambda x, y: x + y),
                                                 map(lambda x: x['stats']['nb_missing_uniprot_id_xrefs'],
                                                     self.registry[md5_hash]['chunks']))
            nb_uniprot_invalid_mapping = reduce((lambda x, y: x + y),
                                                map(lambda x: x['stats']['nb_uniprot_invalid_mapping'],
                                                    self.registry[md5_hash]['chunks']))
            nb_errors = reduce((lambda x, y: x + y),
                               map(lambda x: x['stats']['nb_errors'], self.registry[md5_hash]['chunks']))
            nb_duplicates = reduce((lambda x, y: x + y),
                                   map(lambda x: x['stats']['nb_duplicates'], self.registry[md5_hash]['chunks']))
            nb_valid = reduce((lambda x, y: x + y),
                              map(lambda x: x['stats']['nb_valid'], self.registry[md5_hash]['chunks']))

            # for x in self.registry[md5_hash]['chunks']:
            #     self.logger.debug("%s %i"%(data_source_name, x['chunk']))
                # if x['stats']['invalid_diseases'] is not None:
                #     self.logger.debug("len invalid_diseases: %i"%len(x['stats']['invalid_diseases']))
                # else:
                #     self.logger.debug('None invalid_diseases')
            invalid_diseases = reduce((lambda x, y: self.merge_dict_sum(x, y)), map(lambda x: x['stats']['invalid_diseases'].copy(),
                                                                      self.registry[md5_hash]['chunks']))
            self.logger.debug("len TOTAL invalid_diseases: %i"%len(invalid_diseases))

            obsolete_diseases = reduce((lambda x, y: self.merge_dict_sum(x, y)), map(lambda x: x['stats']['obsolete_diseases'].copy(),
                                                                       self.registry[md5_hash]['chunks']))
            invalid_ensembl_ids = reduce((lambda x, y: self.merge_dict_sum(x, y)),
                                         map(lambda x: x['stats']['invalid_ensembl_ids'].copy(),
                                             self.registry[md5_hash]['chunks']))
            nonref_ensembl_ids = reduce((lambda x, y: self.merge_dict_sum(x, y)),
                                        map(lambda x: x['stats']['nonref_ensembl_ids'].copy(),
                                            self.registry[md5_hash]['chunks']))
            invalid_uniprot_ids = reduce((lambda x, y: self.merge_dict_sum(x, y)),
                                         map(lambda x: x['stats']['invalid_uniprot_ids'].copy(),
                                             self.registry[md5_hash]['chunks']))
            missing_uniprot_id_xrefs = reduce((lambda x, y: self.merge_dict_sum(x, y)),
                                              map(lambda x: x['stats']['missing_uniprot_id_xrefs'].copy(),
                                                  self.registry[md5_hash]['chunks']))
            invalid_uniprot_id_mappings = reduce((lambda x, y: self.merge_dict_sum(x, y)),
                                                 map(lambda x: x['stats']['invalid_uniprot_id_mappings'].copy(),
                                                     self.registry[md5_hash]['chunks']))



            self.logger.debug("not sorted chunks %i", len(self.registry[md5_hash]['chunks']))
            sortedChunks = sorted(self.registry[md5_hash]['chunks'], key=lambda k: k['chunk'])
            self.logger.debug("sortedChunks %i", len(sortedChunks))
            self.logger.debug("keys %s", ",".join(sortedChunks[0]))


            '''
            Write audit logs
            '''
            lfh = open(logfile, 'wb')
            for chunk in sortedChunks:
                self.write_logs(lfh, chunk['audit'])
            lfh.close()

            '''
            Count nb of documents
            '''
            self.logger.debug("Count nb of inserted documents")
            nb_documents = 0
            versioned_index = Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'-'+data_source_name)
            if self.es.indices.exists(versioned_index):
                search = self.es.search(
                        index=versioned_index,
                        doc_type=data_source_name,
                        body='{ "query": { "match_all": {} } }',
                        search_type='count'
                )

                # {"hits": {"hits": [], "total": 9468, "max_score": 0.0}, "_shards": {"successful": 3, "failed": 0, "total": 3}, "took": 3, "timed_out": false}
                nb_documents = search['hits']['total']
            if nb_documents == 0:
                nb_documents = 1

            '''
            Get top 20 diseases
            Get top 20 targets
            '''

            self.logger.debug("Get top 20 targets")
            if self.es.indices.exists(versioned_index):
                search = self.es.search(
                        index=versioned_index,
                        doc_type=data_source_name,
                        body=TOP_20_TARGETS_QUERY,
                )

                if search['hits']['total']:
                    text.append("Top %i targets:" % Config.EVIDENCEVALIDATION_NB_TOP_TARGETS)
                    for top_targets in search['aggregations']['group_by_targets']['buckets']:
                        id = top_targets['key']
                        doc_count = top_targets['doc_count']
                        id_text = None
                        symbol = None
                        # uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', id)
                        ensemblMatch = re.match('http://identifiers.org/ensembl/(ENSG\d+)', id)
                        # uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', id)
                        if ensemblMatch:
                            ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
                            id_text = self.get_reference_gene_from_Ensembl(ensembl_id)
                        # elif uniprotMatch:
                        #    uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                        #    id_text = self.get_reference_gene_from_list(self.uniprot_current[uniprot_id]["gene_ids"]);
                        text.append("\t-{0}:\t{1} ({2:.2f}%) {3}".format(id, doc_count, doc_count * 100.0 / nb_documents,
                                                                       id_text))
                    text.append("")

                    # self.logger.info(json.dumps(top_target))

            self.logger.debug("Get top 20 diseases")
            if self.es.indices.exists(versioned_index):
                search = self.es.search(
                        index=versioned_index,
                        doc_type=data_source_name,
                        body=TOP_20_DISEASES_QUERY,
                )

                if search['hits']['total']:
                    text.append("Top %i diseases:" % (Config.EVIDENCEVALIDATION_NB_TOP_DISEASES))
                    for top_diseases in search['aggregations']['group_by_diseases']['buckets']:
                        # self.logger.info(json.dumps(result))
                        disease = top_diseases['key']
                        doc_count = top_diseases['doc_count']
                        if disease in self.lookup_data.efo_ontology.current_classes:
                            text.append("\t-{0}:\t{1} ({2:.2f}%) {3}".format(disease, doc_count,
                                                                           doc_count * 100.0 / nb_documents,
                                                                           self.lookup_data.efo_ontology.current_classes[disease]))
                        else:
                            text.append("\t-{0}:\t{1} ({2:.2f}%)".format(disease, doc_count, doc_count * 100.0 / nb_documents))
                    text.append("")

            # report invalid/obsolete EFO term
            self.logger.debug("report invalid EFO term")
            if nb_efo_invalid > 0:
                text.append("Errors:")
                text.append("\t%i invalid ontology term(s) found in %i (%.2f%s) of the records." % (
                    len(invalid_diseases), nb_efo_invalid, nb_efo_invalid * 100.0 / nb_documents, '%'))
                for disease_id in invalid_diseases:
                    if invalid_diseases[disease_id] == 1:
                        text.append("\t%s\t(reported once)" % disease_id)
                    else:
                        text.append("\t%s\t(reported %i times)" % (disease_id, invalid_diseases[disease_id]))

                text.append("")

            self.logger.debug("report obsolete EFO term")
            if nb_efo_obsolete > 0:
                text.append("Errors:")
                text.append("\t%i obsolete ontology term(s) found in %i (%.1f%s) of the records." % (
                    len(obsolete_diseases), nb_efo_obsolete, nb_efo_obsolete * 100 / nb_documents, '%'))
                for disease_id in obsolete_diseases:
                    new_term = None
                    if disease_id in self.lookup_data.efo_ontology.obsolete_classes:
                        new_term = self.lookup_data.efo_ontology.obsolete_classes[disease_id]
                    elif disease_id in self.lookup_data.hpo_ontology.obsolete_classes[disease_id]:
                        new_term = self.lookup_data.hpo_ontology.obsolete_classes[disease_id][disease_id]
                    elif disease_id in self.lookup_data.mp_ontology.obsolete_classes[disease_id]:
                        new_term = self.lookup_data.mp_ontology.obsolete_classes[disease_id][disease_id]
                    if obsolete_diseases[disease_id] == 1:
                        text.append("\t%s\t(reported once)\t%s" % (
                            disease_id, new_term))
                    else:
                        text.append("\t%s\t(reported %i times)\t%s" % (
                            disease_id, obsolete_diseases[disease_id], new_term))
                text.append("")

            # report invalid Ensembl genes
            self.logger.debug("report invalid Ensembl genes")
            if nb_ensembl_invalid > 0:
                text.append("Errors:")
                text.append("\t%i unknown Ensembl identifier(s) found in %i (%.1f%s) of the records." % (
                    len(invalid_ensembl_ids), nb_ensembl_invalid, nb_ensembl_invalid * 100 / nb_documents, '%'))
                for ensembl_id in invalid_ensembl_ids:
                    if invalid_ensembl_ids[ensembl_id] == 1:
                        text.append("\t%s\t(reported once)" % (ensembl_id))
                    else:
                        text.append("\t%s\t(reported %i times)" % (ensembl_id, invalid_ensembl_ids[ensembl_id]))
                text.append("")

            # report Ensembl genes not on reference assembly
            self.logger.debug("report Ensembl genes not on reference assembly")
            if nb_ensembl_nonref > 0:
                text.append("Warnings:")
                text.append("\t%i Ensembl Human Alternative sequence Gene identifier(s) not mapped to the reference genome assembly %s found in %i (%.1f%s) of the records." % (
                    len(nonref_ensembl_ids), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, nb_ensembl_nonref,
                    nb_ensembl_nonref * 100 / nb_documents, '%'))
                text.append("\tPlease map them to a reference assembly gene if possible.")
                text.append("\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifier.")
                for ensembl_id in nonref_ensembl_ids:
                    if nonref_ensembl_ids[ensembl_id] == 1:
                        text.append("\t%s\t(reported once) maps to %s" % (
                            ensembl_id, self.get_reference_gene_from_Ensembl(ensembl_id)))
                    else:
                        text.append("\t%s\t(reported %i times) maps to %s" % (
                            ensembl_id, nonref_ensembl_ids[ensembl_id], self.get_reference_gene_from_Ensembl(ensembl_id)))
                text.append("")

            # report invalid Uniprot entries
            self.logger.debug("report invalid Uniprot entries")
            if nb_uniprot_invalid > 0:
                text.append("Errors:")
                text.append("\t%i invalid UniProt identifier(s) found in %i (%.1f%s) of the records." % (
                    len(invalid_uniprot_ids), nb_uniprot_invalid, nb_uniprot_invalid * 100 / nb_documents, '%'))
                for uniprot_id in invalid_uniprot_ids:
                    if invalid_uniprot_ids[uniprot_id] == 1:
                        text.append("\t%s\t(reported once)" % (uniprot_id))
                    else:
                        text.append("\t%s\t(reported %i times)" % (uniprot_id, invalid_uniprot_ids[uniprot_id]))
                text.append("")

                # report UniProt ids with no mapping to Ensembl
            # missing_uniprot_id_xrefs
            self.logger.debug("report UniProt ids with no mapping to Ensembl")
            if nb_missing_uniprot_id_xrefs > 0:
                text.append("Warnings:")
                text.append("\t%i UniProt identifier(s) without cross-references to Ensembl found in %i (%.1f%s) of the records." % (
                    len(missing_uniprot_id_xrefs), nb_missing_uniprot_id_xrefs,
                    nb_missing_uniprot_id_xrefs * 100 / nb_documents, '%'))
                text.append("\tThe corresponding evidence strings have been discarded.")
                for uniprot_id in missing_uniprot_id_xrefs:
                    if missing_uniprot_id_xrefs[uniprot_id] == 1:
                        text.append("\t%s\t(reported once)" % (uniprot_id))
                    else:
                        text.append("\t%s\t(reported %i times)" % (uniprot_id, missing_uniprot_id_xrefs[uniprot_id]))
                text.append("")

            # report invalid Uniprot mapping entries
            self.logger.debug("report invalid Uniprot mapping entries")
            if nb_uniprot_invalid_mapping > 0:
                text.append("Warnings:")
                text.append("\t%i UniProt identifier(s) not mapped to Ensembl reference genome assembly %s gene identifiers found in %i (that's %.2f%s) of the records." % (
                    len(invalid_uniprot_id_mappings), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY,
                    nb_uniprot_invalid_mapping, nb_uniprot_invalid_mapping * 100 / nb_documents, '%'))
                text.append("\tIf you think that might be an error in your submission, please use a UniProt identifier that will map to a reference assembly gene identifier.")
                text.append("\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifiers.")
                for uniprot_id in invalid_uniprot_id_mappings:
                    if invalid_uniprot_id_mappings[uniprot_id] == 1:
                        text.append("\t%s\t(reported once) maps to %s" % (
                            uniprot_id, self.get_reference_gene_from_list([self.lookup_data.uni2ens[uniprot_id]])))
                    else:
                        text.append("\t%s\t(reported %i times) maps to %s" % (
                            uniprot_id, invalid_uniprot_id_mappings[uniprot_id],
                            self.get_reference_gene_from_list([self.lookup_data.uni2ens[uniprot_id]])))
                text.append("")

            text = '\n'.join(text)
            # self.logger.info(text)

            # A file is successfully validated if it meets the following conditions
            successfully_validated = (
            nb_errors == 0 and nb_duplicates == 0 and nb_efo_invalid == 0 and nb_efo_obsolete == 0 and nb_ensembl_invalid == 0 and nb_uniprot_invalid == 0)

            '''
            Update audit index
            '''

            now = datetime.now().strftime("%Y%m%dT%H%M%SZ")
            now_nice = datetime.now().strftime("%d/%m/%Y at %H:%M:%S")

            submission = dict(
                    md5 = md5_hash,
                    nb_records = self.registry[md5_hash]['nb_lines'],
                    nb_errors = nb_errors,
                    nb_duplicates = nb_duplicates,
                    nb_passed_validation = nb_valid,
                    date_modified = now,
                    date_validated = now,
                    successfully_validated = successfully_validated
            )

            self.submission_audit.storage_insert_or_update(submission, update=True)

            self.logger.debug('%s updated %s file in the validation table' % (self.name, file_on_disk))

            '''
            Send e-mail
            '''
            self.send_email(
                    provider_id,
                    data_source_name,
                    filename,
                    successfully_validated,
                    self.registry[md5_hash]['nb_lines'],
                    {'valid records': nb_valid,
                     'JSON errors': nb_errors,
                     'records with duplicates': nb_duplicates,
                     'records with invalid EFO terms': nb_efo_invalid,
                     'records with obsolete EFO terms': nb_efo_obsolete,
                     'records with invalid Ensembl ids': nb_ensembl_invalid,
                     'records with Human Alternative sequence Gene Ensembl ids (warning)': nb_ensembl_nonref,
                     'records with invalid UniProt ids': nb_uniprot_invalid,
                     'records with UniProt entries without x-refs to Ensembl (warning)': nb_missing_uniprot_id_xrefs,
                     'records with UniProt ids not mapped to a reference assembly Ensembl gene (warning)': nb_uniprot_invalid_mapping
                     },
                    now_nice,
                    text,
                    logfile
            )

            '''
            Release space
            '''
            del self.registry[md5_hash]


        return


    def close(self):
        super(AuditTrailProcess, self).close()



class SubmissionAuditElasticStorage():
    def __init__(self, loader, chunk_size=1e3):
        self.loader = loader
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def storage_create_index(loader, recreate=False):
        loader.create_new_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME, recreate)

    def exists(self, filename=None):
        search = self.loader.es.search(
                index=self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                body=SUBMISSION_FILTER_FILENAME_QUERY%filename,
        )

        return search and search["hits"]["total"] == 1

    def get_submission_md5(self, md5=None):
        try:
            search = self.loader.es.search(
                    index=self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                    doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                    body=SUBMISSION_FILTER_MD5_QUERY%md5,
            )

            if search and "hits" in search and search["hits"]["total"] == 1:
                return search["hits"]["hits"][0]
        except NotFoundError:
            pass
        return None

    def get_submission(self, filename=None):
        try:
            search = self.loader.es.search(
                    index=self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                    doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                    body=SUBMISSION_FILTER_FILENAME_QUERY%filename,
            )

            if search and search["hits"]["total"] == 1:
                return search["hits"]["hits"][0]
        except NotFoundError:
            pass
        return None

    def storage_insert_or_update(self, submission=None, update=False):
        start_time = time.time()

        actions = []
        if update:
            action = {
                '_op_type': 'update',
                '_index': '%s' % self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                '_type': '%s' % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                '_id': submission['md5'],
                'doc': submission
            }
            actions.append(action)

        else:
            action = {
                "_index": "%s" % self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                "_type": "%s" % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                "_id": submission['md5'],
                "_source":
                    json.dumps(submission)
            }
            actions.append(action)


        if not self.loader.dry_run:
            success = helpers.bulk(self.loader.es, actions, stats_only=False)
            if success[0] !=1:
                # print("ERRORS REPORTED " + json.dumps(nb_errors))
                self.logger.debug("SubmissionAuditElasticStorage: command failed:%s"%success[0])
            else:
                self.logger.debug('SubmissionAuditElasticStorage: insertion took %ss' % (str(time.time() - start_time)))

            self.loader.es.indices.flush(index=self.loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME))

        return

    def storage_flush(self, data_source_name):

        self.loader.flush()

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

    def startCapture(self, newLogLevel=None):
        """ Start capturing log output to a string buffer.

        http://docs.python.org/release/2.6/library/logging.html

        @param newLogLevel: Optionally change the global logging level, e.g. logging.DEBUG
        """
        self.buffer = StringIO()
        self.logHandler = logging.StreamHandler(self.buffer)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        self.logHandler.setFormatter(formatter)

        # print >> self.buffer, "Log output"

        # for module in [ 'opentargets.model.core', 'opentargets.model.evidence.core', 'opentargets.model.evidence.association_score' ]:
        rootLogger = logging.getLogger()

        if newLogLevel:
            self.oldLogLevel = rootLogger.getEffectiveLevel()
            rootLogger.setLevel(newLogLevel)
        else:
            self.oldLogLevel = None

        rootLogger.addHandler(self.logHandler)

    def stopCapture(self):
        """ Stop capturing log output.

        @return: Collected log output as string
        """

        # Remove our handler
        # for module in [ 'opentargets.model.core', 'opentargets.model.evidence.core', 'opentargets.model.evidence.association_score' ]:
        rootLogger = logging.getLogger()

        # Restore logging level (if any)
        if self.oldLogLevel:
            rootLogger.setLevel(self.oldLogLevel)

        rootLogger.removeHandler(self.logHandler)

        self.logHandler.flush()
        self.buffer.flush()

        return self.buffer.getvalue()


    def check_all(self,
                  local_files = [],
                  remote_files = [],
                  increment = False,
                  dry_run = False):
        '''
        Check every given evidence string
        :return:
        '''

        #self.load_mp()
        #return;
        dry_run = dry_run or self.dry_run

        lookup_data = LookUpDataRetriever(self.es,
                                          self.r_server,
                                          data_types=(LookUpDataType.TARGET,
                                                      LookUpDataType.EFO,
                                                      LookUpDataType.ECO,
                                                      LookUpDataType.HPO,
                                                      LookUpDataType.MP,
                                                     ),
                                          autoload=True,
                                          ).lookup

        # lookup_data.available_genes.load_uniprot2ensembl()

        'Create queues'
        file_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_file_q',
                            max_size=NB_JSON_FILES,
                            job_timeout=86400)
        evidence_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_evidence_q',
                            max_size=MAX_NB_EVIDENCE_CHUNKS,
                            job_timeout=1200)
        store_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_store_q',
                             max_size=MAX_NB_EVIDENCE_CHUNKS,
                             job_timeout=1200)
        audit_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|validation_audit_q',
                             max_size=MAX_NB_EVIDENCE_CHUNKS,
                             job_timeout=1200)

        q_reporter = RedisQueueStatusReporter([file_q,
                                               evidence_q,
                                               store_q,
                                               audit_q
                                               ],
                                              interval=30)
        q_reporter.start()


        workers_number = Config.WORKERS_NUMBER or multiprocessing.cpu_count()
        loaders_number = int(workers_number/3+1)
        readers_number = min([3, len(local_files)+len(remote_files)])
        'Start file reader workers'
        readers = [FileReaderProcess(file_q,
                                     self.r_server.db,
                                     evidence_q,
                                     self.es,
                                     )
                                     for i in range(readers_number)
                                     ]
        for w in readers:
            w.start()




        'Start validating the evidence in chunks on the evidence queuei'

        validators = [ValidatorProcess(evidence_q,
                                       self.r_server.db,
                                       audit_q,
                                       self.es,
                                       lookup_data = lookup_data,
                                       dry_run=dry_run,
                                       increment = increment,
                                       queue_storage = store_q,
                                       ) for i in range(workers_number)]
                                        # ) for i in range(1)]
        for w in validators:
            w.start()

        max_chunk_size = 500
        if MAX_NB_EVIDENCE_CHUNKS/loaders_number <max_chunk_size:
            max_chunk_size = MAX_NB_EVIDENCE_CHUNKS/loaders_number
        loaders = [LoaderWorker(store_q,
                                self.r_server.db,
                                chunk_size=max_chunk_size
                                ) for i in range(loaders_number)]
        for w in loaders:
            w.start()

        'Audit the whole process and send e-mails'
        auditor = AuditTrailProcess(
            audit_q,
            self.r_server.db,
            self.es,
            lookup_data=lookup_data,
            dry_run=dry_run,
            )

        auditor.start()

        'Start crawling the FTP directory'
        directory_crawler = DirectoryCrawlerProcess(
                file_q,
                self.es,
                self.r_server,
                local_files,
                remote_files,
                dry_run=dry_run,
                increment = increment
        )

        directory_crawler.run()

        '''wait for the validator workers to finish'''

        for w in validators:
            w.join()

        for w in loaders:
            w.join()

        # audit_q.set_submission_finished(self.r_server)

        auditor.join()

        if not dry_run:

            self.es.indices.flush(Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME),
                                  wait_if_ongoing=True)
            self.es.indices.flush(Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*'),
                                  wait_if_ongoing=True)
        return

    def reset(self):
        audit_index_name = Loader.get_versioned_index(Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME)
        if self.es.indices.exists(audit_index_name):
            self.es.indices.delete(audit_index_name)
        data_indices = Loader.get_versioned_index(Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME+'*')
        self.es.indices.delete(data_indices)
        self.logger.info('Validation data deleted')

