import os
import sys
import copy
# This bit is necessary for text mining data
from common.ElasticsearchQuery import ESQuery

reload(sys);
sys.setdefaultencoding("utf8");
# blahblah
import re
import gzip
import smtplib
import time
import iso8601
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import Encoders
import logging
from StringIO import StringIO
import json
from json import JSONDecoder
from json import JSONEncoder
from datetime import datetime
from sqlalchemy import and_, or_, table, column, select, update, insert, desc
from common import Actions
from common.PGAdapter import *
from common.UniprotIO import UniprotIterator, Parser
import cttv.model.core as cttv
import cttv.model.flatten as flat
from settings import Config, ElasticSearchConfiguration
import hashlib
from lxml.etree import tostring
from xml.etree import cElementTree as ElementTree
from sqlalchemy.dialects.postgresql import JSON
import multiprocessing
import Queue
from common.PGAdapter import Adapter
import elasticsearch
import itertools
from elasticsearch import Elasticsearch, helpers
from SPARQLWrapper import SPARQLWrapper, JSON

from multiprocessing import Manager

__author__ = 'gautierk'

logger = logging.getLogger(__name__)

BLOCKSIZE = 65536
NB_JSON_FILES = 5
MAX_NB_EVIDENCE_CHUNKS = 500
CHUNK_SIZE = 1e4

EVIDENCE_STRING_INVALID = 10
EVIDENCE_STRING_INVALID_TYPE = 11
EVIDENCE_STRING_INVALID_SCHEMA_VERSION = 12
EVIDENCE_STRING_INVALID_MISSING_TYPE = 13
EVIDENCE_STRING_INVALID_MISSING_TARGET = 14
EVIDENCE_STRING_INVALID_MISSING_DISEASE = 15
ENSEMBL_GENE_ID_UNKNOWN = 20
ENSEMBL_GENE_ID_ALTERNATIVE_SEQUENCE = 21
UNIPROT_PROTEIN_ID_UNKNOWN = 30
UNIPROT_PROTEIN_ID_MISSING_ENSEMBL_XREF = 31
UNIPROT_PROTEIN_ID_ALTERNATIVE_ENSEMBL_XREF = 32
DISEASE_ID_INVALID = 40
DISEASE_ID_OBSOLETE = 41

TOP_20_TARGETS_QUERY = '{  "size": 0, "aggs" : {  "group_by_targets" : {   "terms" : { "field" : "target_id", "order" : { "_count" : "desc" }, "size": 20 } } } }'
TOP_20_DISEASES_QUERY = '{  "size": 0, "aggs" : {  "group_by_diseases" : {   "terms" : { "field" : "disease_id", "order" : { "_count" : "desc" }, "size": 20 } } } }'
DISTINCT_TARGETS_QUERY = '{  "size": 0, "aggs" : {  "distinct_targets" : {  "cardinality" : { "field" : "target_id" } } } }'
DISTINCT_DISEASES_QUERY = '{  "size": 0, "aggs" : {  "distinct_diseases" : {  "cardinality" : { "field" : "disease_id" } } } }'
SUBMISSION_FILTER_FILENAME_QUERY = '''
{
  "query": {
    "filtered": {
    "filtered": {
      "filter": {
        "terms" : { "filename": ["%s"]}
      }
    }
  }
}
'''

'''
curl "localhost:9201/submission-audit/_search" -d '{ "query": { "filtered": { "filter": { "terms" : { "filename": ["/Users/koscieln/Documents/data/ftp/cttv008/upload/submissions/cttv008-04-03-2016.json.gz"]} } } } }'
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

'''
curl "localhost:9201/submission-audit/_search?" -d '
{
  "query": {
    "filtered": {
      "filter": {
        "terms" : { "md5": ["c4053985ca0680dd5eb1c0e226ecd587"]}
      }
    }
  }
}
'

curl "localhost:9201/submission-audit/_search?" -d '
{
  "query": {
    "filtered": {
      "filter": {
        "terms" : { "filename": ["/Users/koscieln/Documents/data/ftp/cttv012/upload/submissions/cttv012-26-11-2015.json.gz"]}
      }
    }
  }
}
'
'''


from time import strftime

# yyyyMMdd'T'HHmmssZ
VALIDATION_DATE = strftime("%Y%m%dT%H%M%SZ")

# figlet -c "Validation Passed"
messagePassed = '''
__     __    _ _     _       _   _               ____                        _
\ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __   |  _ \ __ _ ___ ___  ___  __| |
 \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \  | |_) / _` / __/ __|/ _ \/ _` |
  \ V / (_| | | | (_| | (_| | |_| | (_) | | | | |  __/ (_| \__ \__ \  __/ (_| |
   \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_| |_|   \__,_|___/___/\___|\__,_|

'''

messageFailed = '''
   __     __    _ _     _       _   _               _____     _ _          _
   \ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __   |  ___|_ _(_) | ___  __| |
    \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \  | |_ / _` | | |/ _ \/ _` |
     \ V / (_| | | | (_| | (_| | |_| | (_) | | | | |  _| (_| | | |  __/ (_| |
      \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_| |_|  \__,_|_|_|\___|\__,_|

'''

eva_curated = {
    "http://www.orpha.net/ORDO/Orphanet_1838": "http://www.orpha.net/ORDO/Orphanet_175",
    "http://www.orpha.net/ORDO/Orphanet_68381": "http://www.orpha.net/ORDO/Orphanet_183497",
    "http://www.orpha.net/ORDO/Orphanet_120487": "http://www.orpha.net/ORDO/Orphanet_903",
    "http://www.orpha.net/ORDO/Orphanet_91414": "http://www.orpha.net/ORDO/Orphanet_183487",
    "http://www.orpha.net/ORDO/Orphanet_1047": "http://www.orpha.net/ORDO/Orphanet_98362",
    "http://www.orpha.net/ORDO/Orphanet_121066": "http://www.orpha.net/ORDO/Orphanet_818",
# http://www.orpha.net/ORDO/Orphanet_818
    "http://www.orpha.net/ORDO/Orphanet_216675": "http://www.ebi.ac.uk/efo/EFO_0005269",
    "http://www.orpha.net/ORDO/Orphanet_117623": "http://www.orpha.net/ORDO/Orphanet_827",
    "http://www.orpha.net/ORDO/Orphanet_306773": "http://www.orpha.net/ORDO/Orphanet_3197",
    "http://www.orpha.net/ORDO/Orphanet_137646": "http://www.orpha.net/ORDO/Orphanet_110",
    "http://www.orpha.net/ORDO/Orphanet_121730": "http://www.orpha.net/ORDO/Orphanet_84",
    "http://www.orpha.net/ORDO/Orphanet_211247": "http://www.ebi.ac.uk/efo/EFO_0006888",
    "http://www.orpha.net/ORDO/Orphanet_98867": "http://www.orpha.net/ORDO/Orphanet_288",  # obsolete
    "http://www.orpha.net/ORDO/Orphanet_99977": "http://www.ebi.ac.uk/efo/EFO_0005922",
    "http://www.orpha.net/ORDO/Orphanet_118746": "http://www.orpha.net/ORDO/Orphanet_59",
# Allan-Herndon-Dudley syndrome
    "http://www.orpha.net/ORDO/Orphanet_86834": "http://www.ebi.ac.uk/efo/EFO_1000309",
    "http://www.orpha.net/ORDO/Orphanet_120431": "http://www.orpha.net/ORDO/Orphanet_79277",
    "http://www.orpha.net/ORDO/Orphanet_120935": "http://www.orpha.net/ORDO/Orphanet_379",
    "http://www.orpha.net/ORDO/Orphanet_121722": "http://www.orpha.net/ORDO/Orphanet_84",
    "http://www.orpha.net/ORDO/Orphanet_120345": "http://www.orpha.net/ORDO/Orphanet_794",
    "http://www.orpha.net/ORDO/Orphanet_364559": "http://www.orpha.net/ORDO/Orphanet_183524",
    "http://www.orpha.net/ORDO/Orphanet_122492": "http://www.orpha.net/ORDO/Orphanet_79430",
    "http://www.orpha.net/ORDO/Orphanet_123772": "http://www.orpha.net/ORDO/Orphanet_636",
    "http://www.orpha.net/ORDO/Orphanet_120309": "http://www.orpha.net/ORDO/Orphanet_805",
    "http://www.orpha.net/ORDO/Orphanet_117820": "http://www.orpha.net/ORDO/Orphanet_397596",
# not mapped. -- a gene only valid this time Activated PI3K-delta syndrome Orphanet_397596
    "http://www.orpha.net/ORDO/Orphanet_449": "http://www.ebi.ac.uk/efo/EFO_1000292",
    "http://www.orpha.net/ORDO/Orphanet_52416": "http://www.ebi.ac.uk/efo/EFO_0000096",
# Mantle cell lymphoma (climbed up ontology)
    "http://www.orpha.net/ORDO/Orphanet_251612": "http://www.ebi.ac.uk/efo/EFO_0000272",  # (climbed up ontology)
    "http://www.orpha.net/ORDO/Orphanet_2965": "http://www.ebi.ac.uk/efo/EFO_0004125",  # (climbed up ontology)
    "http://www.orpha.net/ORDO/Orphanet_121715": "http://www.orpha.net/ORDO/Orphanet_84",
    "http://www.orpha.net/ORDO/Orphanet_121719": "http://www.orpha.net/ORDO/Orphanet_84",
# (this and record above are mapped to versions of complementation group)
    "http://www.orpha.net/ORDO/Orphanet_767": "http://www.ebi.ac.uk/efo/EFO_0006803",
# (Polyarteritis nodosa ->climbed up ontology)
    "http://www.orpha.net/ORDO/Orphanet_282196": "http://www.orpha.net/ORDO/Orphanet_3453",
    "http://www.orpha.net/ORDO/Orphanet_99927": "http://www.ebi.ac.uk/efo/EFO_1000298",
    "http://www.orpha.net/ORDO/Orphanet_803": "http://www.ebi.ac.uk/efo/EFO_0000253",
    "http://www.orpha.net/ORDO/Orphanet_1501": "http://www.ebi.ac.uk/efo/EFO_0003093",
    "http://www.orpha.net/ORDO/Orphanet_123411": "http://www.ebi.ac.uk/efo/EFO_1000642",
    "http://www.orpha.net/ORDO/Orphanet_118978": "http://www.orpha.net/ORDO/Orphanet_110",
    "http://www.orpha.net/ORDO/Orphanet_547": "http://www.ebi.ac.uk/efo/EFO_0005952",
    "http://www.orpha.net/ORDO/Orphanet_123257": "http://www.orpha.net/ORDO/Orphanet_2478",
    "http://www.orpha.net/ORDO/Orphanet_79358": "http://www.orpha.net/ORDO/Orphanet_183444",
    "http://www.orpha.net/ORDO/Orphanet_553": "http://www.ebi.ac.uk/efo/EFO_0003099",
    "http://www.orpha.net/ORDO/Orphanet_88": "http://www.ebi.ac.uk/efo/EFO_0006926",  #
    "http://www.orpha.net/ORDO/Orphanet_120353": "http://www.orpha.net/ORDO/Orphanet_79431",
# tyrosinase, oculocutaneous albinism IA => Oculocutaneous albinism type 1A
    "http://www.orpha.net/ORDO/Orphanet_120795": "http://www.orpha.net/ORDO/Orphanet_157",
# carnitine palmitoyltransferase 2 => Carnitine palmitoyltransferase II deficiency
    "http://www.orpha.net/ORDO/Orphanet_121802": "http://www.orpha.net/ORDO/Orphanet_710",
# fibroblast growth factor receptor 1 => Pfeiffer syndrome
    "http://www.orpha.net/ORDO/Orphanet_178826": "http://www.orpha.net/ORDO/Orphanet_65",
# spermatogenesis associated 7 => Leber congenital amaurosis
    # "Orphanet_132262" : "Orphanet_93262" # Crouzon syndrome with acanthosis nigricans, Crouzon syndrome => need intermediate Crouzon Syndrome in EFO!!!
    "http://www.orpha.net/ORDO/Orphanet_123572": "http://www.orpha.net/ORDO/Orphanet_2170",
# 5-methyltetrahydrofolate-homocysteine methyltransferase => METHYLCOBALAMIN DEFICIENCY, cblG TYPE => Methylcobalamin deficiency type cblG
    "http://www.orpha.net/ORDO/Orphanet_189330": "http://www.orpha.net/ORDO/Orphanet_250",
# ALX3 mutations leading to Frontonasal dysplasia
    "http://www.orpha.net/ORDO/Orphanet_121400": "http://www.orpha.net/ORDO/Orphanet_976",
# APRT mutations leading to Adenine phosphoribosyltransferase deficiency
    "http://www.orpha.net/ORDO/Orphanet_1984": "http://www.orpha.net/ORDO/Orphanet_182050",
# obsolete Fechtner syndrome => http://www.orpha.net/ORDO/Orphanet_182050 with label: MYH9-related disease
    "http://www.orpha.net/ORDO/Orphanet_79354": "http://www.orpha.net/ORDO/Orphanet_183426",
# map to upper term as EFO does not have this one, should request it!
    "http://www.orpha.net/ORDO/Orphanet_850": "http://www.orpha.net/ORDO/Orphanet_182050",
# May-Hegglin thrombocytopenia deprecated => The preferred class is use http://www.orpha.net/ORDO/Orphanet_182050 with label: MYH9-related disease
    "http://www.orpha.net/ORDO/Orphanet_123524": "http://www.orpha.net/ORDO/Orphanet_379",
# Granulomatous disease, chronic, autosomal recessive, cytochrome b-negative => Chronic granulomatous disease
    "http://www.orpha.net/ORDO/Orphanet_69": "http://www.orpha.net/ORDO/Orphanet_2118",
# wrong eVA mapping!! should be 4-Hydroxyphenylpyruvate dioxygenase deficiency
    "http://www.orpha.net/ORDO/Orphanet_123334": "http://www.orpha.net/ORDO/Orphanet_99901",
# wrong mapping should be Acyl-CoA dehydrogenase 9 deficiency
    "http://www.orpha.net/ORDO/Orphanet_123421": "http://www.orpha.net/ORDO/Orphanet_361",
# wrong mapping in eVA for this gene: mutations lead to ACTH resistance
    # "http://www.orpha.net/ORDO/Orphanet_171059" # needs to be refined variant by variant, terms exists in EFO for each of the conditions : methylmalonic aciduria (cobalamin deficiency) cblD type, with homocystinuria in CLinVAR: Homocystinuria, cblD type, variant 1 OR Methylmalonic aciduria, cblD type, variant 2
    "http://www.orpha.net/ORDO/Orphanet_158032": "http://www.orpha.net/ORDO/Orphanet_540",
# STX11, STXBP2 => Familial hemophagocytic lymphohistiocytosis
    "http://www.orpha.net/ORDO/Orphanet_159550": "http://www.orpha.net/ORDO/Orphanet_35698"
# POLG : Mitochondrial DNA depletion syndrome
}


class ValidationActions(Actions):
    CHECKFILES = 'checkfiles'
    VALIDATE = 'validate'
    GENEMAPPING = 'genemapping'


class DirectoryCrawlerProcess(multiprocessing.Process):
    def __init__(self,
                 output_q,
                 es,
                 input_file_loading_finished,
                 input_file_count,
                 lock):
        super(DirectoryCrawlerProcess, self).__init__()
        self.output_q = output_q
        self.es = es
        self.evidence_chunk = EvidenceChunkElasticStorage(es=self.es)
        self.submission_audit = SubmissionAuditElasticStorage(es=self.es)
        self.start_time = time.time()
        self.input_file_loading_finished = input_file_loading_finished
        self.input_file_count = input_file_count
        self.start_time = time.time()  # reset timer start
        self.lock = lock

    def run(self):
        logger.info("%s started" % self.name)

        '''
        create index for:
         the evidence if it does not exists
         the submitted files if it does not exists
        '''
        self.evidence_chunk.storage_create_index(b_recreate=False)
        self.submission_audit.storage_create_index(b_recreate=False)

        '''
        now scroll through directories
        '''
        for dirname, dirnames, filenames in os.walk(Config.EVIDENCEVALIDATION_FTP_SUBMISSION_PATH):
            dirnames.sort()
            for subdirname in dirnames:
                cttv_match = re.match("^(cttv[0-9]{3})$", subdirname)
                #cttv_match = re.match("^(cttv012)$", subdirname)
                if cttv_match:
                    # get provider id
                    provider_id = cttv_match.groups()[0]

                    cttv_dir = os.path.join(dirname, subdirname)
                    logging.info(cttv_dir)
                    path = os.path.join(cttv_dir, "upload/submissions")
                    for cttv_dirname, cttv_dirs, filenames in os.walk(path):
                        # sort by the last modified time of the files
                        filenames.sort(key=lambda x: os.stat(os.path.join(path, x)).st_mtime)
                        for filename in filenames:

                            cttv_filename_match = re.match(Config.EVIDENCEVALIDATION_FILENAME_REGEX, filename);
                            logging.info("%s %r"%(filename, cttv_filename_match))
                            # cttv_filename_match = re.match("cttv006_Networks_Reactome-03-12-2015.json.gz", filename);
                            if (cttv_filename_match
                                #and
                                #(filename == 'cttv012-26-11-2015.json.gz') or
                                #(filename == 'cttv_external_mousemodels-26-01-2016.json.gz') or
                                #(filename == 'cttv006_Networks_Reactome-18-02-2016.json.gz')
                                #(filename == 'cttv025-24-02-2016.json.gz')
                                #(filename == 'cttv007-09-03-2016.json.gz')
                                #(filename == 'cttv012-03-03-2016.json.gz')
                                #(filename == 'cttv008-26-02-2016.json.gz') or
                                #(filename == 'cttv009-25-02-2016.json.gz') or
                                #(filename == 'cttv010-10-03-2016.json.gz')
                                #(filename == 'cttv011-26-02-2016.json.gz') or
                                #(filename == 'cttv012-03-03-2016.json.gz')
                                ):
                                #: #(filename == "cttv006_Networks_Reactome-03-12-2015.json.gz" or filename == "cttv_external_mousemodels-26-01-2016.json.gz"): #"cttv006_Networks_Reactome-03-12-2015.json.gz":
                                cttv_file = os.path.join(cttv_dirname, filename)
                                logging.info(cttv_file)
                                cttv_data_source_name = Config.JSON_FILE_TO_DATASOURCE_MAPPING[
                                    cttv_filename_match.groups()[0]]
                                data_source_name = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[
                                    cttv_data_source_name]
                                logging.info(data_source_name)
                                last_modified = os.path.getmtime(cttv_file)
                                # july = time.strptime("01 Jul 2015", "%d %b %Y")
                                # julyseconds = time.mktime(july)
                                earliest_date = time.strptime("15 Feb 2016", "%d %b %Y")
                                sepseconds = time.mktime(earliest_date)
                                if (last_modified - sepseconds) > 0:
                                    logging.info("Elapsed time %i"%(last_modified - sepseconds))
                                    m = re.match("^(.+).json.gz$", filename)
                                    logfile = os.path.join(cttv_dirname, m.groups()[0] + "_log.txt")
                                    logging.info("%s %s" % (self.name, cttv_file))

                                    # push to the queue
                                    # self.validate_gzipfile(cttv_file, filename, provider_id, data_source_name, md5_hash, logfile = logfile)

                                    try:
                                        ''' get md5 '''
                                        md5_hash = self.md5_hash_gzipfile(cttv_file)
                                        logging.info("%s %s %s" % (self.name, cttv_file, md5_hash))
                                        self.check_gzipfile(cttv_file, filename, provider_id, data_source_name,
                                                            md5_hash, logfile)
                                        logging.info("%s %s DONE" % (self.name, cttv_file))

                                    except Exception, error:

                                        if isinstance(error, AttributeError):
                                            logger.error("%s Error checking file %s: %s" % (self.name, cttv_file, str(error)))
                                        else:
                                            logger.exception(
                                                "%s Error loading file for id %s: %s" % (self.name, cttv_file, str(error)))

        self.input_file_loading_finished.set()
        logger.info("%s finished" % self.name)

    def md5_hash_gzipfile(self, filename):

        hasher = hashlib.md5()
        with gzip.open(filename, 'rb') as afile:
            # with open(filename, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        md5_hash = hasher.hexdigest()
        print(md5_hash)
        afile.close()
        return md5_hash

    def check_gzipfile(self, file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile=None):
        '''
        check if the file was already processed
        '''
        bValidate = False
        rowToUpdate = None

        '''
        check if the file was parsed already in ES
        '''

        #existing_submission= self.submission_audit.get_submission(filename=file_on_disk)
        existing_submission= self.submission_audit.get_submission_md5(md5=md5_hash)

        if existing_submission:
            md5 = existing_submission["_source"]["md5"]
            if md5 == md5_hash:
                    logging.info('%s == %s' % (md5, md5_hash))
                    logging.info('%s file already recorded. Won\'t parse' % file_on_disk)
                    ''' should be false, set to true if you want to parse anyway '''
                    bValidate = False
            else:
                logging.info('%s != %s' % (md5, md5_hash))
                bValidate = True
        else:
            bValidate = True
        logging.info('bValidate %r' % (bValidate))

        if bValidate == True:

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
                    filename=file_on_disk,
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

            self.output_q.put((file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile))
            with self.lock:
                self.input_file_count.value += 1

        return


class FileReaderProcess(multiprocessing.Process):
    def __init__(self,
                 input_q,
                 output_q,
                 es,
                 input_file_loading_finished,
                 input_file_processing_finished,
                 input_file_count,
                 input_file_processed_count,
                 evidence_loaded_count,
                 lock):
        super(FileReaderProcess, self).__init__()
        self.input_q = input_q
        self.output_q = output_q
        self.es = es
        self.evidence_chunk_storage = EvidenceChunkElasticStorage(es=self.es)
        self.start_time = time.time()
        self.input_file_loading_finished = input_file_loading_finished
        self.input_file_processing_finished = input_file_processing_finished
        self.input_file_count = input_file_count
        self.input_file_processed_count = input_file_processed_count
        self.evidence_loaded_count = evidence_loaded_count
        self.start_time = time.time()  # reset timer start
        self.lock = lock

    def run(self):
        logger.info("%s started" % self.name)
        while not ((
                       self.input_file_count.value == self.input_file_processed_count.value) and self.input_file_loading_finished.is_set()):
            logger.info("%s %i" % (self.name, self.input_file_count.value))
            time.sleep(1)
            data = None
            try:
                data = self.input_q.get_nowait()
            except Queue.Empty:
                pass
            if data:
                with self.lock:
                    self.input_file_processed_count.value += 1
                file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile = data
                try:
                    logging.info(file_on_disk)
                    ''' parse the file and put evidence in the queue '''
                    self.parse_gzipfile(file_on_disk, filename, provider_id, data_source_name, md5_hash,
                                        logfile=logfile)

                except Exception, error:
                    # with self.lock:
                    #    self.processing_errors_count.value +=1
                    # UploadError(ev, error, idev).save()
                    # err += 1

                    if isinstance(error, AttributeError):
                        logger.error("Error loading data for id %s: %s" % (file_on_disk, str(error)))
                        break
                        # logger.error("%i %i"%(self.output_computed_count.value,self.processing_errors_count.value))
                    #elif isinstance(error, elasticsearch.connection.exceptions.TransportError):
                    #    logger.error("Error updating data in ElasticSearch for id %s: %s" % (file_on_disk, str(error)))
                    #    # we have to stop the program in case of an error
                    #    break

                    else:
                        logger.exception("Error loading data for id %s: %s" % (file_on_disk, str(error)))
                        # traceback.print_exc(limit=1, file=sys.stdout)
                        break

        self.input_file_processing_finished.set()
        logger.info("%s finished" % self.name)

    def parse_gzipfile(self, file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile=None):

        logging.info('%s Delete previous data for %s' % (self.name, data_source_name))
        self.evidence_chunk_storage.storage_delete(data_source_name)

        logging.info('%s Starting parsing %s' % (self.name, file_on_disk))

        fh = gzip.GzipFile(file_on_disk, "r")
        # lfh = gzip.open(logfile, 'wb', compresslevel=5)
        line_buffer = []
        offset = 0
        chunk = 1
        line_number = 0

        for line in fh:
            line_buffer.append(line)
            line_number += 1
            if line_number % CHUNK_SIZE == 0:
                logging.info('%s %s %i %i' % (self.name, md5_hash, offset, len(line_buffer)))
                self.output_q.put((file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile, chunk,
                                   offset, list(line_buffer), False))
                offset += CHUNK_SIZE
                chunk += 1
                del line_buffer[:]
        if len(line_buffer) > 0:
            logging.info('%s %s %i %i' % (self.name, md5_hash, offset, len(line_buffer)))
            self.output_q.put((file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile, chunk, offset,
                               list(line_buffer), False))
            offset += len(line_buffer)
            chunk += 1
            del line_buffer[:]

        '''
        indicate how many evidence were sent
        '''
        with self.lock:
            self.evidence_loaded_count.value += line_number

        '''
        finally send a signal to inform that parsing is completed for this file and no other line are to be expected
        '''
        logging.info('%s %s %i %i' % (self.name, md5_hash, offset, len(line_buffer)))
        self.output_q.put((file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile, chunk, offset,
                           line_buffer, True))

        return


class ValidatorProcess(multiprocessing.Process):
    def __init__(self,
                 input_q,
                 output_q,
                 es,
                 efo_current,
                 efo_uncat,
                 efo_obsolete,
                 hpo_current,
                 hpo_obsolete,
                 mp_current,
                 mp_obsolete,
                 uniprot_current,
                 ensembl_current,
                 input_file_processing_finished,
                 input_file_validation_finished,
                 input_file_processed_count,
                 input_file_validated_count,
                 evidence_loaded_count,
                 evidence_validated_count,
                 lock):
        super(ValidatorProcess, self).__init__()
        self.input_q = input_q
        self.output_q = output_q
        self.es = es
        self.evidence_chunk_storage = EvidenceChunkElasticStorage(es=self.es)
        self.efo_current = efo_current
        self.efo_uncat = efo_uncat
        self.efo_obsolete = efo_obsolete
        self.hpo_current = hpo_current
        self.hpo_obsolete = hpo_obsolete
        self.mp_current = mp_current
        self.mp_obsolete = mp_obsolete
        self.uniprot_current = uniprot_current
        self.ensembl_current = ensembl_current
        self.start_time = time.time()
        self.input_file_validation_finished = input_file_validation_finished
        self.input_file_processing_finished = input_file_processing_finished
        self.input_file_validated_count = input_file_validated_count
        self.input_file_processed_count = input_file_processed_count
        self.evidence_loaded_count = evidence_loaded_count
        self.evidence_validated_count = evidence_validated_count
        self.start_time = time.time()  # reset timer start
        self.lock = lock
        self.audit = list()

    def run(self):
        logger.info("%s started" % self.name)
        ''' 3 conditions to fullfill to exit the loop:
            1. make sure that each file has been validated: this is ensured by the last information sent in the queue
            for every file to be validated
            2. make sure all evidence strings have been validated, that is the number of evidence strings received is
               equals to the number of evidence string processed.
            3. make sure that the producer has finished processing the files
        '''
        while not ((self.evidence_loaded_count.value == self.evidence_validated_count.value) and
                       (self.input_file_processed_count.value == self.input_file_validated_count.value) and
                       self.input_file_processing_finished.is_set()):
            # logger.info("%s %i %i"%(self.name,self.evidence_loaded_count.value, self.evidence_validated_count.value ))
            time.sleep(1)
            data = None
            try:
                data = self.input_q.get_nowait()
            except Queue.Empty:
                pass
            if data:
                file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile, chunk, offset, line_buffer, end_of_transmission = data

                try:
                    # logging.info(file_on_disk)
                    self.validate_evidence(file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk,
                                           offset, line_buffer, end_of_transmission, logfile=logfile)

                except Exception, error:

                    if isinstance(error, AttributeError):
                        logger.error("%s Error checking file %s: %s" % (self.name, file_on_disk, str(error)))
                        # logger.error("%i %i"%(self.output_computed_count.value,self.processing_errors_count.value))
                    else:
                        logger.error("%s Error checking file %s: %s" % (self.name, file_on_disk, str(error)))
                        # traceback.print_exc(limit=1, file=sys.stdout)

        self.input_file_validation_finished.set()
        logger.info("%s finished" % self.name)

    def validate_evidence(self, file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, offset,
                          line_buffer, end_of_transmission, logfile=None):
        '''
        validate evidence strings from a chunk
        cumulate the logs, acquire a lock,
        write the logs
        write the data to the database
        '''

        if end_of_transmission:
            logging.info("%s Validation of %s completed" % (self.name, file_on_disk))
            '''
            Send a message to the audit trail process with the md5 key of the file
            '''

            self.output_q.put((file_on_disk,
                               filename,
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
                               end_of_transmission))

            '''
            Increment the number of file validated
            '''
            with self.lock:
                self.input_file_validated_count.value += 1
        else:
            logging.info('%s Validating %s %i %i' % (self.name, md5_hash, offset, len(line_buffer)))

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

                python_raw = json.loads(line)
                lc += 1

                # now validate
                obj = None
                validation_result = 0
                validation_failed = False
                disease_failed = False
                gene_failed = False
                gene_mapping_failed = False
                uniq_elements_flat_hexdig = None
                target_id = None
                efo_id = None

                if (('label' in python_raw or
                             'type' in python_raw) and
                            'validated_against_schema_version' in python_raw and
                            python_raw['validated_against_schema_version'] == Config.EVIDENCEVALIDATION_SCHEMA):
                    if 'label' in python_raw:
                        python_raw['type'] = python_raw.pop('label', None)
                    data_type = python_raw['type']
                    #logging.info('type %s'%data_type)
                    if data_type in Config.EVIDENCEVALIDATION_DATATYPES:
                        #logging.info(line)
                        try:
                            if data_type == 'genetic_association':
                                obj = cttv.Genetics.fromMap(python_raw)
                            elif data_type == 'rna_expression':
                                obj = cttv.Expression.fromMap(python_raw)
                            elif data_type in ['genetic_literature', 'affected_pathway', 'somatic_mutation']:
                                obj = cttv.Literature_Curated.fromMap(python_raw)
                                if data_type == 'somatic_mutation' and not isinstance(python_raw['evidence']['known_mutations'], list):
                                    mutations = copy.deepcopy(python_raw['evidence']['known_mutations'])
                                    python_raw['evidence']['known_mutations'] = [ mutations ]
                                    logging.error(json.dumps(python_raw['evidence']['known_mutations'], indent=4))
                                    obj = cttv.Literature_Curated.fromMap(python_raw)
                            elif data_type == 'known_drug':
                                obj = cttv.Drug.fromMap(python_raw)
                                # logging.info(obj.evidence.association_score.__class__.__name__)
                                # logging.info(obj.evidence.target2drug.association_score.__class__.__name__)
                                # logging.info(obj.evidence.drug2clinic.association_score.__class__.__name__)
                            elif data_type == 'literature':
                                obj = cttv.Literature_Mining.fromMap(python_raw)
                            elif data_type == 'animal_model':
                                obj = cttv.Animal_Models.fromMap(python_raw)
                        except:
                            obj = None

                        if obj:

                            if obj.target.id:
                                for id in obj.target.id:
                                    if id in targets:
                                        targets[id] += 1
                                    else:
                                        targets[id] = 1
                                    if not id in top_targets:
                                        if len(top_targets) < Config.EVIDENCEVALIDATION_NB_TOP_TARGETS:
                                            top_targets.append(id)
                                        else:
                                            # map,reduce
                                            for n in range(0, len(top_targets)):
                                                if targets[top_targets[n]] < targets[id]:
                                                    top_targets[n] = id;
                                                    break;

                            if obj.disease.id:
                                for id in obj.disease.id:
                                    if id in diseases:
                                        diseases[id] += 1
                                    else:
                                        diseases[id] = 1
                                    if not id in top_diseases:
                                        if len(top_diseases) < Config.EVIDENCEVALIDATION_NB_TOP_DISEASES:
                                            top_diseases.append(id)
                                        else:
                                            # map,reduce
                                            for n in range(0, len(top_diseases)):
                                                if diseases[top_diseases[n]] < diseases[id]:
                                                    top_diseases[n] = id;
                                                    break;

                            # flatten
                            uniq_elements = obj.unique_association_fields
                            uniq_elements_flat = flat.DatatStructureFlattener(uniq_elements)
                            uniq_elements_flat_hexdig = uniq_elements_flat.get_hexdigest()

                            '''
                            Validate evidence string
                            '''
                            validation_result = obj.validate(logger)
                            nb_errors += validation_result

                            '''
                            Check EFO
                            '''
                            disease_count = 0
                            if obj.disease.id:
                                index = 0
                                for disease_id in obj.disease.id:
                                    disease_count += 1
                                    efo_id = disease_id
                                    # fix for EVA data for release 1.1
                                    if disease_id in eva_curated:
                                        obj.disease.id[index] = eva_curated[disease_id];
                                        disease_id = obj.disease.id[index];
                                    index += 1

                                    ''' Check disease term or phenotype term '''
                                    if (disease_id not in self.efo_current and disease_id not in self.hpo_current and disease_id not in self.mp_current) or disease_id in self.efo_uncat:
                                        audit.append((lc, DISEASE_ID_INVALID, disease_id))
                                        disease_failed = True
                                        if disease_id not in invalid_diseases:
                                            invalid_diseases[disease_id] = 1
                                        else:
                                            invalid_diseases[disease_id] += 1
                                        nb_efo_invalid += 1
                                    if disease_id in self.efo_obsolete:
                                        audit.append((lc, DISEASE_ID_OBSOLETE, disease_id))
                                        # logger.error("Line {0}: Obsolete disease term detected {1} ('{2}'): {3}".format(lc+1, disease_id, self.efo_current[disease_id], self.efo_obsolete[disease_id]))
                                        disease_failed = True
                                        if disease_id not in obsolete_diseases:
                                            obsolete_diseases[disease_id] = 1
                                        else:
                                            obsolete_diseases[disease_id] += 1
                                        nb_efo_obsolete += 1
                                    elif disease_id in self.hpo_obsolete or disease_id in self.mp_obsolete:
                                        audit.append((lc, DISEASE_ID_OBSOLETE, disease_id))
                                        # logger.error("Line {0}: Obsolete disease term detected {1} ('{2}'): {3}".format(lc+1, disease_id, self.efo_current[disease_id], self.efo_obsolete[disease_id]))
                                        disease_failed = True
                                        if disease_id not in obsolete_diseases:
                                            obsolete_diseases[disease_id] = 1
                                        else:
                                            obsolete_diseases[disease_id] += 1
                                        nb_efo_obsolete += 1

                            if obj.disease.id is None or disease_count == 0:
                                ''' no disease id !!!! '''
                                audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_DISEASE))
                                disease_failed = True

                            '''
                            Check Ensembl ID, UniProt ID and UniProt ID mapping to a Gene ID
                            '''
                            target_count = 0
                            if obj.target.id:
                                for id in obj.target.id:
                                    target_count += 1
                                    # http://identifiers.org/ensembl/ENSG00000178573
                                    ensemblMatch = re.match('http://identifiers.org/ensembl/(ENSG\d+)', id)
                                    uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', id)
                                    if ensemblMatch:
                                        ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
                                        target_id = id
                                        if not ensembl_id in self.ensembl_current:
                                            gene_failed = True
                                            audit.append((lc, ENSEMBL_GENE_ID_UNKNOWN, ensembl_id))
                                            # logger.error("Line {0}: Unknown Ensembl gene detected {1}. Please provide a correct gene identifier on the reference genome assembly {2}".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not ensembl_id in invalid_ensembl_ids:
                                                invalid_ensembl_ids[ensembl_id] = 1
                                            else:
                                                invalid_ensembl_ids[ensembl_id] += 1
                                            nb_ensembl_invalid += 1
                                        elif self.ensembl_current[ensembl_id]['is_reference'] is False:
                                            gene_mapping_failed = True
                                            audit.append((lc, ENSEMBL_GENE_ID_ALTERNATIVE_SEQUENCE, ensembl_id))
                                            # logger.warning("Line {0}: Human Alternative sequence Ensembl Gene detected {1}. We will attempt to map it to a gene identifier on the reference genome assembly {2} or choose a Human Alternative sequence Ensembl Gene Id".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not ensembl_id in invalid_ensembl_ids:
                                                nonref_ensembl_ids[ensembl_id] = 1
                                            else:
                                                nonref_ensembl_ids[ensembl_id] += 1
                                            nb_ensembl_nonref += 1

                                    elif uniprotMatch:
                                        uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                                        if uniprot_id not in self.uniprot_current:
                                            gene_failed = True
                                            audit.append((lc, UNIPROT_PROTEIN_ID_UNKNOWN, uniprot_id))
                                            # logger.error("Line {0}: Invalid UniProt entry detected {1}. Please provide a correct identifier".format(lc+1, uniprot_id))
                                            if uniprot_id not in invalid_uniprot_ids:
                                                invalid_uniprot_ids[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_ids[uniprot_id] += 1
                                            nb_uniprot_invalid += 1
                                        elif "gene_ids" not in self.uniprot_current[uniprot_id]:
                                            # check symbol mapping (get symbol first)
                                            # gene_mapping_failed = True
                                            gene_failed = True
                                            audit.append((lc, UNIPROT_PROTEIN_ID_MISSING_ENSEMBL_XREF, uniprot_id))
                                            # logger.warning("Line {0}: UniProt entry {1} does not have any cross-reference to Ensembl.".format(lc+1, uniprot_id))
                                            if not uniprot_id in missing_uniprot_id_xrefs:
                                                missing_uniprot_id_xrefs[uniprot_id] = 1
                                            else:
                                                missing_uniprot_id_xrefs[uniprot_id] += 1
                                            nb_missing_uniprot_id_xrefs += 1
                                            # This identifier is not in the current EnsEMBL database
                                        elif not reduce((lambda x, y: x or y),
                                                        map(lambda x: self.ensembl_current[x]['is_reference'] is True,
                                                            self.uniprot_current[uniprot_id]["gene_ids"])):
                                            gene_mapping_failed = True
                                            audit.append((lc, UNIPROT_PROTEIN_ID_ALTERNATIVE_ENSEMBL_XREF, uniprot_id))
                                            # logger.warning("Line {0}: The UniProt entry {1} does not have a cross-reference to an Ensembl Gene Id on the reference genome assembly {2}. It will be mapped to a Human Alternative sequence Ensembl Gene Id.".format(lc+1, uniprot_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not uniprot_id in invalid_uniprot_id_mappings:
                                                invalid_uniprot_id_mappings[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_id_mappings[uniprot_id] += 1
                                            nb_uniprot_invalid_mapping += 1
                                        else:
                                            reference_target_list = filter(
                                                lambda x: self.ensembl_current[x]['is_reference'] is True,
                                                self.uniprot_current[uniprot_id]["gene_ids"])
                                            if reference_target_list:
                                                target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[
                                                    0]
                                            else:
                                                # get the first one, needs a better way
                                                target_id = self.uniprot_current[uniprot_id]["gene_ids"][0]
                                            # logger.info("Found target id being: %s for %s" %(target_id, uniprot_id))
                                            if target_id is None:
                                                logger.info("Found no target id for %s" % (uniprot_id))

                            if obj.target.id is None or target_count == 0 or target_id is None:
                                ''' no target id !!!! '''
                                audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_TARGET))
                                gene_failed = True
                                nb_errors += 1

                            ''' store the evidence '''
                            if validation_result == 0 and not disease_failed and not gene_failed:
                                nb_valid += 1;
                                # logger.info("Add evidence for %s %s " %(target_id, disease_id))
                                # flatten data structure
                                # logging.info('%s Adding to chunk %s %s'% (self.name, target_id, disease_id))
                                json_doc_hashdig = flat.DatatStructureFlattener(python_raw).get_hexdigest();


                                self.evidence_chunk_storage.storage_add(uniq_elements_flat_hexdig,
                                                                        EvidenceString11(
                                                                            uniq_assoc_fields_hashdig=uniq_elements_flat_hexdig,
                                                                            json_doc_hashdig=json_doc_hashdig,
                                                                            evidence_string=json.loads(obj.to_JSON()),
                                                                            target_id=target_id,
                                                                            disease_id=efo_id,
                                                                            data_source_name=data_source_name,
                                                                            json_schema_version="1.2.2",
                                                                            json_doc_version=1,
                                                                            release_date=VALIDATION_DATE),
                                                                        # release_date = datetime.utcnow()),
                                                                        data_source_name);

                            else:
                                if disease_failed:
                                    nb_errors += 1
                                if gene_failed:
                                    nb_errors += 1
                        else:
                            audit.append((lc, EVIDENCE_STRING_INVALID))
                            # logger.error("Line {0}: Not a valid 1.2.2 evidence string - There was an error parsing the JSON document. The document may contain an invalid field".format(lc+1))
                            nb_errors += 1
                            validation_failed = True

                    else:
                        audit.append((lc, EVIDENCE_STRING_INVALID_TYPE, data_type))
                        # logger.error("Line {0}: '{1}' is not a valid 1.2.2 evidence string type".format(lc+1, data_type))
                        nb_errors += 1
                        validation_failed = True

                elif (not 'validated_against_schema_version' in python_raw or
                          ('validated_against_schema_version' in python_raw and
                                   python_raw['validated_against_schema_version'] != Config.EVIDENCEVALIDATION_SCHEMA
                           )
                      ):
                    audit.append((lc, EVIDENCE_STRING_INVALID_SCHEMA_VERSION))
                    # logger.error("Line {0}: Not a valid 1.2.1 evidence string - please check the 'validated_against_schema_version' mandatory attribute".format(lc+1))
                    nb_errors += 1
                    validation_failed = True
                else:
                    ''' type '''
                    audit.append((lc, EVIDENCE_STRING_INVALID_MISSING_TYPE))
                    logger.error(
                        "Line {0}: Not a valid 1.2.2 evidence string - please add the mandatory 'type' attribute".format(
                            lc + 1))
                    nb_errors += 1
                    validation_failed = True

                cc += len(line)
                # for line :)

            logging.info('%s nb line parsed %i (size %i)' % (self.name, lc, cc))

            ''' write results in ES '''

            self.evidence_chunk_storage.storage_flush(data_source_name)

            logging.info('%s bulk insert complete' % (self.name))

            '''
            Inform the audit trailer to generate a report and send an e-mail to the data provider
            '''
            self.output_q.put((file_on_disk,
                               filename,
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
                               end_of_transmission))

            with self.lock:
                self.evidence_validated_count.value += len(line_buffer)

        return


class AuditTrailProcess(multiprocessing.Process):
    def __init__(self,
                 input_q,
                 es,
                 ensembl_current,
                 uniprot_current,
                 symbols,
                 efo_current,
                 efo_obsolete,
                 mp_current,
                 mp_obsolete,
                 hpo_current,
                 hpo_obsolete,
                 input_file_validation_finished,
                 input_file_auditing_finished,
                 input_file_validated_count,
                 input_file_audited_count,
                 lock):
        super(AuditTrailProcess, self).__init__()
        self.input_q = input_q
        self.es = es
        self.submission_audit = SubmissionAuditElasticStorage(es=self.es)
        self.ensembl_current = ensembl_current
        self.uniprot_current = uniprot_current
        self.symbols = symbols
        self.efo_current = efo_current
        self.efo_obsolete = efo_obsolete
        self.hpo_current = hpo_current
        self.hpo_obsolete = hpo_obsolete
        self.mp_current = mp_current
        self.mp_obsolete = mp_obsolete
        self.start_time = time.time()
        self.input_file_auditing_finished = input_file_auditing_finished
        self.input_file_validation_finished = input_file_validation_finished
        self.input_file_audited_count = input_file_audited_count
        self.input_file_validated_count = input_file_validated_count
        self.start_time = time.time()  # reset timer start
        self.lock = lock
        self.registry = dict()

    def run(self):
        logger.info("%s started" % self.name)
        while not ((self.input_file_audited_count.value == self.input_file_validated_count.value) and
                       self.input_file_validation_finished.is_set()):
            # logger.info("%s %i"%(self.name, self.input_file_audited_count.value))
            time.sleep(1)
            data = None
            try:
                data = self.input_q.get_nowait()
            except Queue.Empty:
                pass
            if data:
                try:
                    file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, stats, audit, offset, buffer_size, logfile, end_of_transmission = data
                    self.audit_submission(file_on_disk, filename, provider_id, data_source_name, md5_hash, chunk, stats,
                                          audit, offset, buffer_size, end_of_transmission, logfile)

                except Exception, error:
                    # with self.lock:
                    #    self.processing_errors_count.value +=1
                    # UploadError(ev, error, idev).save()
                    # err += 1

                    if isinstance(error, AttributeError):
                        logger.error("%s Error auditing file %s: %s" % (self.name, file_on_disk, str(error)))
                        import traceback
                        logging.error('generic exception: ' + traceback.format_exc())
                        # logger.error("%i %i"%(self.output_computed_count.value,self.processing_errors_count.value))
                    else:
                        logger.error("%s Error auditing file %s: %s" % (self.name, file_on_disk, str(error)))
                        import traceback
                        traceback.print_exc(limit=1, file=sys.stdout)

        self.input_file_auditing_finished.set()
        logger.info("%s finished" % self.name)

    def send_email(self, bSend, provider_id, data_source_name, filename, bValidated, nb_records, errors, when, extra_text, logfile):
        me = Config.EVIDENCEVALIDATION_SENDER_ACCOUNT
        you = ",".join(Config.EVIDENCEVALIDATION_PROVIDER_EMAILS[provider_id])
        status = "passed"
        if not bValidated:
            status = "failed"
        # Create message container - the correct MIME type is multipart/alternative.
        # msg = MIMEMultipart('alternative')
        msg = MIMEMultipart()
        msg['Subject'] = "CTTV: {0} validation {1} for {2}".format(data_source_name, status, filename)
        msg['From'] = me
        msg['To'] = you
        rcpt = Config.EVIDENCEVALIDATION_PROVIDER_EMAILS[provider_id]
        if provider_id != 'cttv001':
            rcpt.extend(Config.EVIDENCEVALIDATION_PROVIDER_EMAILS['cttv001'])
            msg['Cc'] = ",".join(Config.EVIDENCEVALIDATION_PROVIDER_EMAILS['cttv001'])

        text = "This is an automated message generated by the CTTV Core Platform Pipeline on {0}\n".format(when)

        if bValidated:
            text += messagePassed
            text += "Congratulations :)\n"
        else:
            text += messageFailed
            text += "See details in the attachment {0}\n\n".format(os.path.basename(logfile))
        text += "Data Provider:\t%s\n"%data_source_name
        text += "JSON schema version:\t1.2.2\n"
        text += "Number of records parsed:\t{0}\n".format(nb_records)
        for key in errors:
            text += "Number of {0}:\t{1}\n".format(key, errors[key])
        text += "\n"
        text += extra_text
        text += "\nYours\nThe CTTV Core Platform Team"
        # text += signature
        print text

        if bSend:
            logging.info("Send e-mail to data provider...")
            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(text, 'plain')

            # Attach parts into message container.
            # According to RFC 2046, the last part of a multipart message, in this case
            # the HTML message, is best and preferred.
            msg.attach(part1)

            if not bValidated:
                part2 = MIMEBase('application', "octet-stream")
                part2.set_payload(open(logfile, "rb").read())
                Encoders.encode_base64(part2)
                part2.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(logfile)))
                msg.attach(part2)

            # Send the message via local SMTP server.
            mail = smtplib.SMTP('smtp.office365.com', 587)

            mail.ehlo()

            mail.starttls()

            mail.login(me, Config.EVIDENCEVALIDATION_SENDER_PASSWORD)
            mail.sendmail(me, rcpt, msg.as_string())
            mail.quit()
            logging.info("e-mail sent")
        return 0

    def merge_dict_sum(self, x, y):
        # merge keys
        # x.update(y) won't work
        for key, value in y.iteritems():
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
        symbol = self.ensembl_current[ensembl_gene_id]['display_name']
        if symbol in self.symbols and 'ensembl_primary_id' in self.symbols[symbol]:
            return self.symbols[symbol]['ensembl_primary_id'] + " " + symbol + " (reference assembly)"
        return symbol + " (non reference assembly)"

    def get_reference_gene_from_list(self, genes):
        '''
        Given a list of genes will return the gene that is mapped to the reference assembly
        :param genes: a list of ensembl gene identifiers
        :return: the ensembl gene identifier mapped to the reference assembly if it exists, the list of ensembl gene
        identifiers passed in the input otherwise
        '''
        for ensembl_gene_id in genes:
            if self.ensembl_current[ensembl_gene_id]['is_reference'] is True:
                return ensembl_gene_id + " " + self.ensembl_current[ensembl_gene_id][
                    'display_name'] + " (reference assembly)"
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
            # logger.error("Line {0}: Not a valid 1.2.1 evidence string - please check the 'validated_against_schema_version' mandatory attribute".format(lc+1))
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

        logger.info("%s %s chunk=%i nb_lines=%i nb_valid=%i nb_errors=%i"
                    % (self.name, md5_hash, chunk, stats["nb_lines"], stats["nb_valid"], stats["nb_errors"]))

        ''' check invalid disease and report it in the logs '''
        #for item in audit:
        #    if item[1] == DISEASE_ID_INVALID:
        #        # lc, DISEASE_ID_INVALID, disease_id
        #        logger.info("Line %i: invalid disease %s" % (item[0], item[2]))

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

            logging.info("%s generating report "%self.name)

            text = ''

            '''
             refresh the indice
            '''
            self.es.indices.refresh(index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME)

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

            for x in self.registry[md5_hash]['chunks']:
                logging.info("%s %i"%(data_source_name, x['chunk']))
                if x['stats']['invalid_diseases'] is not None:
                    logging.info("len invalid_diseases: %i"%len(x['stats']['invalid_diseases']))
                else:
                    logging.info('None invalid_diseases')
            invalid_diseases = reduce((lambda x, y: self.merge_dict_sum(x, y)), map(lambda x: x['stats']['invalid_diseases'].copy(),
                                                                      self.registry[md5_hash]['chunks']))
            logging.info("len TOTAL invalid_diseases: %i"%len(invalid_diseases))

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



            logging.info("not sorted chunks %i", len(self.registry[md5_hash]['chunks']))
            sortedChunks = sorted(self.registry[md5_hash]['chunks'], key=lambda k: k['chunk'])
            logging.info("sortedChunks %i", len(sortedChunks))
            logging.info("keys %s", ",".join(sortedChunks[0]))


            '''
            Write audit logs
            '''
            logging.info("Open log file")
            lfh = open(logfile, 'wb')
            for chunk in sortedChunks:
                logging.info("%i"%chunk['chunk'])
                self.write_logs(lfh, chunk['audit'])
            lfh.close()
            logging.info("Close log file")

            '''
            Count nb of documents
            '''
            logging.info("Count nb of inserted documents")
            search = self.es.search(
                    index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
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
            {"hits": {"hits": [], "total": 9468, "max_score": 0.0}, "_shards": {"successful": 3, "failed": 0, "total": 3}, "took": 21, "aggregations": {"group_by_targets": {"buckets": [{"key": "ENSG00000146648", "doc_count": 4157}, {"key": "ENSG00000066468", "doc_count": 513}, {"key": "ENSG00000068078", "doc_count": 377}, {"key": "ENSG00000148400", "doc_count": 322}, {"key": "ENSG00000160867", "doc_count": 125}, {"key": "ENSG00000077782", "doc_count": 118}, {"key": "ENSG00000164690", "doc_count": 78}, {"key": "ENSG00000005075", "doc_count": 45}, {"key": "ENSG00000047315", "doc_count": 45}, {"key": "ENSG00000099817", "doc_count": 45}, {"key": "ENSG00000100142", "doc_count": 45}, {"key": "ENSG00000102978", "doc_count": 45}, {"key": "ENSG00000105258", "doc_count": 45}, {"key": "ENSG00000125651", "doc_count": 45}, {"key": "ENSG00000144231", "doc_count": 45}, {"key": "ENSG00000147669", "doc_count": 45}, {"key": "ENSG00000163882", "doc_count": 45}, {"key": "ENSG00000168002", "doc_count": 45}, {"key": "ENSG00000177700", "doc_count": 45}, {"key": "ENSG00000181222", "doc_count": 45}], "sum_other_doc_count": 3193, "doc_count_error_upper_bound": 18}}, "timed_out": false}
            '''

            logging.info("Get top 20 targets")
            search = self.es.search(
                    index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                    doc_type=data_source_name,
                    body=TOP_20_TARGETS_QUERY,
            )

            if search:
                text += "Top %i targets:\n" % Config.EVIDENCEVALIDATION_NB_TOP_TARGETS
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
                        symbol = self.ensembl_current[ensembl_id]['display_name']
                    # elif uniprotMatch:
                    #    uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                    #    id_text = self.get_reference_gene_from_list(self.uniprot_current[uniprot_id]["gene_ids"]);
                    text += "\t-{0}:\t{1} ({2:.2f}%) {3}\n".format(id, doc_count, doc_count * 100.0 / nb_documents,
                                                                   id_text)
                text += "\n"

                # logging.info(json.dumps(top_target))

            logging.info("Get top 20 diseases")
            search = self.es.search(
                    index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                    doc_type=data_source_name,
                    body=TOP_20_DISEASES_QUERY,
            )

            if search:
                text += "Top %i diseases:\n" % (Config.EVIDENCEVALIDATION_NB_TOP_DISEASES)
                for top_diseases in search['aggregations']['group_by_diseases']['buckets']:
                    # logging.info(json.dumps(result))
                    disease = top_diseases['key']
                    doc_count = top_diseases['doc_count']
                    if top_diseases['key'] in self.efo_current:
                        text += "\t-{0}:\t{1} ({2:.2f}%) {3}\n".format(disease, doc_count,
                                                                       doc_count * 100.0 / nb_documents,
                                                                       self.efo_current[disease])
                    else:
                        text += "\t-{0}:\t{1} ({2:.2f}%)\n".format(disease, doc_count, doc_count * 100.0 / nb_documents)
                text += "\n"

            # report invalid/obsolete EFO term
            logging.info("report invalid EFO term")
            if nb_efo_invalid > 0:
                text += "Errors:\n"
                text += "\t%i invalid ontology term(s) found in %i (%.2f%s) of the records.\n" % (
                len(invalid_diseases), nb_efo_invalid, nb_efo_invalid * 100.0 / nb_documents, '%')
                for disease_id in invalid_diseases:
                    if invalid_diseases[disease_id] == 1:
                        text += "\t%s\t(reported once)\n" % disease_id
                    else:
                        text += "\t%s\t(reported %i times)\n" % (disease_id, invalid_diseases[disease_id])

                text += "\n"

            logging.info("report obsolete EFO term")
            if nb_efo_obsolete > 0:
                text += "Errors:\n"
                text += "\t%i obsolete ontology term(s) found in %i (%.1f%s) of the records.\n" % (
                len(obsolete_diseases), nb_efo_obsolete, nb_efo_obsolete * 100 / nb_documents, '%')
                for disease_id in obsolete_diseases:
                    new_term = None
                    if disease_id in self.efo_obsolete:
                        new_term = self.efo_obsolete[disease_id]
                    elif disease_id in self.hpo_obsolete:
                        new_term = self.hpo_obsolete[disease_id]
                    else:
                        new_term = self.mp_obsolete[disease_id]
                    if obsolete_diseases[disease_id] == 1:
                        text += "\t%s\t(reported once)\t%s\n" % (
                        disease_id, new_term.replace("\n", " "))
                    else:
                        text += "\t%s\t(reported %i times)\t%s\n" % (
                        disease_id, obsolete_diseases[disease_id], new_term.replace("\n", " "))
                text += "\n"

            # report invalid Ensembl genes
            logging.info("report invalid Ensembl genes")
            if nb_ensembl_invalid > 0:
                text += "Errors:\n"
                text += "\t%i unknown Ensembl identifier(s) found in %i (%.1f%s) of the records.\n" % (
                len(invalid_ensembl_ids), nb_ensembl_invalid, nb_ensembl_invalid * 100 / nb_documents, '%')
                for ensembl_id in invalid_ensembl_ids:
                    if invalid_ensembl_ids[ensembl_id] == 1:
                        text += "\t%s\t(reported once)\n" % (ensembl_id)
                    else:
                        text += "\t%s\t(reported %i times)\n" % (ensembl_id, invalid_ensembl_ids[ensembl_id])
                text += "\n"

            # report Ensembl genes not on reference assembly
            logging.info("report Ensembl genes not on reference assembly")
            if nb_ensembl_nonref > 0:
                text += "Warnings:\n"
                text += "\t%i Ensembl Human Alternative sequence Gene identifier(s) not mapped to the reference genome assembly %s found in %i (%.1f%s) of the records.\n" % (
                len(nonref_ensembl_ids), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, nb_ensembl_nonref,
                nb_ensembl_nonref * 100 / nb_documents, '%')
                text += "\tPlease map them to a reference assembly gene if possible.\n"
                text += "\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifier.\n"
                for ensembl_id in nonref_ensembl_ids:
                    if nonref_ensembl_ids[ensembl_id] == 1:
                        text += "\t%s\t(reported once) maps to %s\n" % (
                        ensembl_id, self.get_reference_gene_from_Ensembl(ensembl_id))
                    else:
                        text += "\t%s\t(reported %i times) maps to %s\n" % (
                        ensembl_id, nonref_ensembl_ids[ensembl_id], self.get_reference_gene_from_Ensembl(ensembl_id))
                text += "\n"

            # report invalid Uniprot entries
            logging.info("report invalid Uniprot entries")
            if nb_uniprot_invalid > 0:
                text += "Errors:\n"
                text += "\t%i invalid UniProt identifier(s) found in %i (%.1f%s) of the records.\n" % (
                len(invalid_uniprot_ids), nb_uniprot_invalid, nb_uniprot_invalid * 100 / nb_documents, '%')
                for uniprot_id in invalid_uniprot_ids:
                    if invalid_uniprot_ids[uniprot_id] == 1:
                        text += "\t%s\t(reported once)\n" % (uniprot_id)
                    else:
                        text += "\t%s\t(reported %i times)\n" % (uniprot_id, invalid_uniprot_ids[uniprot_id])
                text += "\n"

                # report UniProt ids with no mapping to Ensembl
            # missing_uniprot_id_xrefs
            logging.info("report UniProt ids with no mapping to Ensembl")
            if nb_missing_uniprot_id_xrefs > 0:
                text += "Warnings:\n"
                text += "\t%i UniProt identifier(s) without cross-references to Ensembl found in %i (%.1f%s) of the records.\n" % (
                len(missing_uniprot_id_xrefs), nb_missing_uniprot_id_xrefs,
                nb_missing_uniprot_id_xrefs * 100 / nb_documents, '%')
                text += "\tThe corresponding evidence strings have been discarded.\n"
                for uniprot_id in missing_uniprot_id_xrefs:
                    if missing_uniprot_id_xrefs[uniprot_id] == 1:
                        text += "\t%s\t(reported once)\n" % (uniprot_id)
                    else:
                        text += "\t%s\t(reported %i times)\n" % (uniprot_id, missing_uniprot_id_xrefs[uniprot_id])
                text += "\n"

            # report invalid Uniprot mapping entries
            logging.info("report invalid Uniprot mapping entries")
            if nb_uniprot_invalid_mapping > 0:
                text += "Warnings:\n"
                text += "\t%i UniProt identifier(s) not mapped to Ensembl reference genome assembly %s gene identifiers found in %i (that's %.2f%s) of the records.\n" % (
                len(invalid_uniprot_id_mappings), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY,
                nb_uniprot_invalid_mapping, nb_uniprot_invalid_mapping * 100 / nb_documents, '%')
                text += "\tIf you think that might be an error in your submission, please use a UniProt identifier that will map to a reference assembly gene identifier.\n"
                text += "\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifiers.\n"
                for uniprot_id in invalid_uniprot_id_mappings:
                    if invalid_uniprot_id_mappings[uniprot_id] == 1:
                        text += "\t%s\t(reported once) maps to %s\n" % (
                        uniprot_id, self.get_reference_gene_from_list(self.uniprot_current[uniprot_id]["gene_ids"]))
                    else:
                        text += "\t%s\t(reported %i times) maps to %s\n" % (
                        uniprot_id, invalid_uniprot_id_mappings[uniprot_id],
                        self.get_reference_gene_from_list(self.uniprot_current[uniprot_id]["gene_ids"]))
                text += "\n"

            print(text)

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

            logging.info('%s updated %s file in the validation table' % (self.name, file_on_disk))

            '''
            Send e-mail
            '''

            self.send_email(
                    Config.EVIDENCEVALIDATION_SEND_EMAIL,
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

            with self.lock:
                self.input_file_audited_count.value += 1

        return


class ELasticStorage():
    @staticmethod
    def create_data_index(es, index_name=None, mappings=None, b_recreate=False):
        # delete former index, uncomment if required
        if es.indices.exists(index_name):
            if b_recreate:
                print("deleting '%s' index..." % (index_name))
                res = es.indices.delete(index=index_name)
                print(" response: '%s'" % (res))
            else:
                return

        # ElasticSearchConfiguration().validated_data_settings_and_mappings
        # ignore 400 cause by IndexAlreadyExistsException when creating an index
        res = es.indices.create(index=index_name,
                                body=mappings,
                                ignore=400)
        print(" response: '%s'" % (res))

    @staticmethod
    def delete_prev_data_in_es_obsolete(es, data_source_name):
        # Delete all documents in an index of specific type without deleting its mapping
        # delete_by_query
        es.delete(index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                  doc_type=data_source_name,
                  q='{"query":{"match_all":{}}}')

    @staticmethod
    def delete_prev_data_in_es(es, data_source_name):
        # Create a query for results you want to delete

        count = 0
        # filter without a query
        # { "query": { "filtered": { "filter": { "type" : { "value" : "efo" } } } } }
        # {"took":1,"timed_out":false,"_shards":{"total":3,"successful":3,"failed":0},"hits":{"total":0,"max_score":0.0,"hits":[]}}
        try:
            q = '{ "query": { "filtered": { "filter": { "type" : { "value" : "%s" } } } } }' % (data_source_name)

            res = es.search(
                    index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                    body=q,
                    size=0,
                    search_type='count')

            count = res["hits"]["total"]

            logger.info(
                "EvidenceStringELasticStorage %s nb docs: %i" % (Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME, count))

        except Exception:
            # import traceback
            # logging.error('generic exception: ' + traceback.format_exc())
            # generate HTTP_EXCEPTIONS.get(status_code, TransportError)(status_code, error_message, additional_info)
            # NotFoundError: TransportError(404, u'IndexMissingException[[validated-data] missing]')
            return

        if (count > 0):
            logging.info("Delete previous submitted data: %i evidence will be removed"%count)
            #search = es.search(
            #        index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
            #        doc_type=data_source_name,
            #        q='{"query":{"match_all":{}}}',
            #        size=10,
            #        search_type="scan",
            #        scroll='5m',
            #)

            search = es.search(
                    index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                    doc_type=data_source_name,
                    body=q,
                    size=int(CHUNK_SIZE),
                    search_type="scan",
                    scroll='5m')

            nb_scroll = 0
            total_hits = count

            while total_hits > 0:
                try:
                    # Get the next page of results.
                    if nb_scroll % 10 == 0:
                        logging.info("Get Scroll %i and delete data for datasource %s"%(nb_scroll, data_source_name))
                    nb_scroll+=1
                    scroll = es.scroll(scroll_id=search['_scroll_id'], scroll='5m')
                    # Since scroll throws an error catch it and break the loop.
                    # We have results initialize the bulk variable.
                    bulk = ""
                    for result in scroll['hits']['hits']:
                        bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(
                                result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
                    # Finally do the deleting.
                    es.bulk(body=bulk)
                    total_hits -= len(scroll['hits']['hits'])
                except Exception, error:
                    if isinstance(error, elasticsearch.exceptions.NotFoundError):
                        logger.error("ElasticSearch Error updating data in ElasticSearch %s" % (str(error)))
                    else:
                        logger.error("ElasticSearch Error %s" % (str(error)))
                    break

                # es.delete(index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME, doc_type=data_source_name)

    @staticmethod
    def store_to_es(es,
                    data_source_name,
                    data,
                    quiet=False):

        start_time = time.time()
        rows_to_insert = 0

        actions = []

        for key, value in data.iteritems():
            rows_to_insert += 1
            action = {
                "_index": "%s" % Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                "_type": "%s" % data_source_name,
                "_id": key,
                "_source":
                    json.dumps(dict(uniq_assoc_fields_hashdig=key,
                                    json_doc_hashdig=value.json_doc_hashdig,
                                    evidence_string=value.evidence_string,
                                    target_id=value.target_id,
                                    disease_id=value.disease_id,
                                    data_source_name=value.data_source_name,
                                    json_schema_version=value.json_schema_version,
                                    json_doc_version=value.json_doc_version,
                                    release_date=value.release_date
                                    ))
                # "timestamp": datetime.now()
            }
            actions.append(action)

            '''
            es.index(index=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                     doc_type=data_source_name,
                     id=key,
                     body=json.dumps(dict(uniq_assoc_fields_hashdig = key,
                                        json_doc_hashdig = value.json_doc_hashdig,
                                        evidence_string = value.evidence_string,
                                        target_id = value.target_id,
                                        disease_id = value.disease_id,
                                        data_source_name = value.data_source_name,
                                        json_schema_version = value.json_schema_version,
                                        json_doc_version = value.json_doc_version,
                                        release_date = value.release_date
                                      )))
            '''
        if len(actions) > 0:
            helpers.bulk(es, actions)
        # if not quiet:
        logging.info('EvidenceStringStorage: inserted %i rows of %s inserted in evidence_string took %ss' % (
        rows_to_insert, data_source_name, str(time.time() - start_time)))

        return rows_to_insert


class EvidenceChunkElasticStorage():
    def __init__(self, es, chunk_size=1e3):
        self.es = es
        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0

    def storage_create_index(self, b_recreate=False):
        ELasticStorage.create_data_index(
                self.es,
                index_name=Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
                mappings=ElasticSearchConfiguration.validated_data_settings_and_mappings,
                b_recreate=b_recreate)

    def storage_reset(self):
        self.cache = {}
        self.counter = 0

    def storage_add(self, id, evidence_string, data_source_name):

        self.cache[id] = evidence_string
        self.counter += 1
        if (len(self.cache) % self.chunk_size) == 0:
            self.storage_flush(data_source_name)

    def storage_delete(self, data_source_name):
        ELasticStorage.delete_prev_data_in_es(self.es, data_source_name)
        self.cache = {}
        self.counter = 0

    def storage_flush(self, data_source_name):

        #logging.info("Flush storage for %s" % data_source_name)
        if self.cache:
            ELasticStorage.store_to_es(self.es,
                                       data_source_name,
                                       self.cache,
                                       quiet=False
                                       )
            self.counter += len(self.cache)
            self.cache = {}

class SubmissionAuditElasticStorage():
    def __init__(self, es, chunk_size=1e3):
        self.es = es

    def storage_create_index(self, b_recreate=False):
        ELasticStorage.create_data_index(
                self.es,
                index_name=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                mappings=ElasticSearchConfiguration.submission_audit_settings_and_mappings,
                b_recreate=b_recreate)

    def exists(self, filename=None):
        search = self.es.search(
                index=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                body=SUBMISSION_FILTER_FILENAME_QUERY%filename,
        )

        return (search and search["hits"]["total"] == 1)

    def get_submission_md5(self, md5=None):
        search = self.es.search(
                index=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                body=SUBMISSION_FILTER_MD5_QUERY%md5,
        )

        if search and search["hits"]["total"] == 1:
            return search["hits"]["hits"][0]
        else:
            return None

    def get_submission(self, filename=None):
        search = self.es.search(
                index=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                doc_type=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                body=SUBMISSION_FILTER_FILENAME_QUERY%filename,
        )

        if search and search["hits"]["total"] == 1:
            return search["hits"]["hits"][0]
        else:
            return None

    def storage_insert_or_update(self, submission=None, update=False):
        start_time = time.time()

        actions = []
        if update:
            logging.info(json.dumps(submission, indent=4))
            action = {
                '_op_type': 'update',
                '_index': '%s' % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                '_type': '%s' % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                '_id': submission['md5'],
                'doc': submission
            }
            actions.append(action)
            #actions.append()

        else:
            action = {
                "_index": "%s" % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME,
                "_type": "%s" % Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME,
                "_id": submission['md5'],
                "_source":
                    json.dumps(submission)
            }
            actions.append(action)

        logging.info(json.dumps(actions[0], indent=4))

        nb_success = helpers.bulk(self.es, actions, stats_only=False)
        logging.info(json.dumps(nb_success, indent=4))
        if nb_success[0] !=1:
            # print("ERRORS REPORTED " + json.dumps(nb_errors))
            logging.info("SubmissionAuditElasticStorage: command failed:%s"%nb_success[0])
        else:
            logging.info('SubmissionAuditElasticStorage: insertion took %ss' % (str(time.time() - start_time)))

        self.es.indices.refresh(index=Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME)

        return

    def storage_delete(self, data_source_name):
        ELasticStorage.delete_prev_data_in_es(self.es, data_source_name)
        self.cache = {}
        self.counter = 0

    def storage_flush(self, data_source_name):

        #logging.info("Flush storage for %s" % data_source_name)
        if self.cache:
            ELasticStorage.store_to_es(self.es,
                                       data_source_name,
                                       self.cache,
                                       quiet=False
                                       )
            self.counter += len(self.cache)
            self.cache = {}

class EvidenceValidationFileChecker():
    def __init__(self, adapter, es, sparql, chunk_size=1e4):
        self.adapter = adapter
        self.session = adapter.session
        self.es = es
        self.esquery = ESQuery(self.es)
        self.sparql = sparql
        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0
        # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        # self.buffer = StringIO()
        # streamhandler = logging.StreamHandler(self.buffer)
        # streamhandler.setFormatter(formatter)
        # memoryhandler = logging.handlers.MemoryHandler(1024*10, logging.DEBUG, streamhandler)
        # LOGGER = logging.getLogger('cttv.model.core')
        # LOGGER.setLevel(logging.ERROR)
        # LOGGER.addHandler(memoryhandler)
        # LOGGER = logging.getLogger('cttv.model.evidence.core')
        # LOGGER.setLevel(logging.ERROR)
        # LOGGER.addHandler(memoryhandler)
        # LOGGER = logging.getLogger('cttv.model.evidence.association_score')
        # LOGGER.setLevel(logging.ERROR)
        # LOGGER.addHandler(memoryhandler)

        self.uniprot_current = {}
        self.efo_current = {}
        self.efo_obsolete = {}
        self.efo_uncat = []
        self.hpo_current = {}
        self.hpo_obsolete = {}
        self.mp_current = {}
        self.mp_obsolete = {}
        self.ensembl_current = {}
        self.hgnc_current = {}
        self.eco_current = {}
        self.symbols = {}

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

        # for module in [ 'cttv.model.core', 'cttv.model.evidence.core', 'cttv.model.evidence.association_score' ]:
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
        # for module in [ 'cttv.model.core', 'cttv.model.evidence.core', 'cttv.model.evidence.association_score' ]:
        rootLogger = logging.getLogger()

        # Restore logging level (if any)
        if self.oldLogLevel:
            rootLogger.setLevel(self.oldLogLevel)

        rootLogger.removeHandler(self.logHandler)

        self.logHandler.flush()
        self.buffer.flush()

        return self.buffer.getvalue()

    def load_HGNC(self):
        '''
        Load HGNC information from the last version in database
        :return: None
        '''
        logging.info("Loading HGNC entries and mapping from Ensembl Gene Id to UniProt")
        c = 0
        for row in self.session.query(HgncInfoLookup).order_by(desc(HgncInfoLookup.last_updated)).limit(1):
            # data = json.loads(row.data);
            for doc in row.data["response"]["docs"]:
                gene_symbol = None;
                ensembl_gene_id = None;
                if "symbol" in doc:
                    gene_symbol = doc["symbol"];
                    if gene_symbol not in self.symbols:
                        logging.info("Adding missing symbol from Ensembl %s" % gene_symbol)
                        self.symbols[gene_symbol] = {};

                    self.symbols[gene_symbol]["hgnc_id"] = doc["hgnc_id"]
                    self.hgnc_current = doc

                if "ensembl_gene_id" in doc:
                    ensembl_gene_id = doc["ensembl_gene_id"];

                if "uniprot_ids" in doc:
                    if "uniprot_ids" not in self.symbols[gene_symbol]:
                        self.symbols[gene_symbol]["uniprot_ids"] = [];
                    for uniprot_id in doc["uniprot_ids"]:
                        if uniprot_id in self.uniprot_current and ensembl_gene_id is not None and ensembl_gene_id in self.ensembl_current:
                            # print uniprot_id, " ", ensembl_gene_id, "\n"
                            if "gene_ids" not in self.uniprot_current[uniprot_id]:
                                self.uniprot_current[uniprot_id]["gene_ids"] = [ensembl_gene_id];
                            elif ensembl_gene_id not in self.uniprot_current[uniprot_id]["gene_ids"]:
                                self.uniprot_current[uniprot_id]["gene_ids"].append(ensembl_gene_id);

                        if uniprot_id not in self.symbols[gene_symbol]["uniprot_ids"]:
                            self.symbols[gene_symbol]["uniprot_ids"].append(uniprot_id)

        logging.info("%i entries parsed for HGNC" % len(row.data["response"]["docs"]))

    def load_Uniprot(self):
        logging.info("Loading Uniprot identifiers and mappings to Gene Symbol and Ensembl Gene Id")
        c = 0
        for row in self.session.query(UniprotInfo).yield_per(1000):
            # get the symbol too
            uniprot_accession = row.uniprot_accession
            self.uniprot_current[uniprot_accession] = {};
            root = ElementTree.fromstring(row.uniprot_entry)
            protein_name = None
            for name in root.findall("./ns0:name", {'ns0': 'http://uniprot.org/uniprot'}):
                protein_name = name.text
                break;
            gene_symbol = None
            for gene_name_el in root.findall(".//ns0:gene/ns0:name[@type='primary']",
                                             {'ns0': 'http://uniprot.org/uniprot'}):
                gene_symbol = gene_name_el.text
                break;

            if gene_symbol is not None:
                # some protein have to be remapped to another symbol
                # PLSCR3 => TMEM256-PLSCR3
                # LPPR4 => PLPPR4
                # Q9Y328 => NSG2 => Nsg2 => HMP19 (in Human)
                #
                if gene_symbol == 'PLSCR3':
                    gene_symbol = 'TMEM256-PLSCR3';
                    logging.info("Mapping protein entry to correct symbol %s" % gene_symbol)
                elif gene_symbol == 'LPPR4':
                    gene_symbol = 'PLPPR4';
                    logging.info("Mapping protein entry to correct symbol %s" % gene_symbol)
                elif gene_symbol == 'NSG2':
                    gene_symbol = 'HMP19';
                    logging.info("Mapping protein entry to correct symbol %s" % gene_symbol)

                self.uniprot_current[uniprot_accession]["gene_symbol"] = gene_symbol;

                if gene_symbol not in self.symbols:
                    self.symbols[gene_symbol] = {};
                if "uniprot_ids" not in self.symbols[gene_symbol]:
                    self.symbols[gene_symbol]["uniprot_ids"] = [uniprot_accession]
                elif uniprot_accession not in self.symbols[gene_symbol]["uniprot_ids"]:
                    self.symbols[gene_symbol]["uniprot_ids"].append(uniprot_accession)

            gene_id = None
            for crossref in root.findall(".//ns0:dbReference[@type='Ensembl']/ns0:property[@type='gene ID']",
                                         {'ns0': 'http://uniprot.org/uniprot'}):
                ensembl_gene_id = crossref.get("value")
                if ensembl_gene_id in self.ensembl_current:
                    # print uniprot_accession, " ", ensembl_gene_id, "\n"
                    if "gene_ids" not in self.uniprot_current[uniprot_accession]:
                        self.uniprot_current[uniprot_accession]["gene_ids"] = [ensembl_gene_id];
                    elif ensembl_gene_id not in self.uniprot_current[uniprot_accession]["gene_ids"]:
                        self.uniprot_current[uniprot_accession]["gene_ids"].append(ensembl_gene_id)

            # create a mapping from the symbol instead to link to Ensembl
            if "gene_ids" not in self.uniprot_current[uniprot_accession]:
                if gene_symbol and gene_symbol in self.symbols:
                    if ("ensembl_primary_id" in self.symbols[gene_symbol] and
                            ("gene_ids" not in self.uniprot_current[uniprot_accession] or
                                     self.symbols[gene_symbol]["ensembl_primary_id"] not in
                                     self.uniprot_current[uniprot_accession]["gene_ids"]
                             )
                        ):
                        self.uniprot_current[uniprot_accession]["gene_ids"] = [
                            self.symbols[gene_symbol]["ensembl_primary_id"]];
                    elif ("ensembl_secondary_id" in self.symbols[gene_symbol] and
                              ("gene_ids" not in self.uniprot_current[uniprot_accession] or
                                       self.symbols[gene_symbol]["ensembl_secondary_id"] not in
                                       self.uniprot_current[uniprot_accession]["gene_ids"]
                               )
                          ):
                        self.uniprot_current[uniprot_accession]["gene_ids"] = [
                            self.symbols[gene_symbol]["ensembl_secondary_id"]];

            # seqrec = UniprotIterator(StringIO(row.uniprot_entry), 'uniprot-xml').next()
            c += 1
            if c % 5000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
                # for accession in seqrec.annotations['accessions']:
                #    self.uniprot_current.append(accession)
        logging.info("%i entries retrieved for uniprot" % c)

    def load_Ensembl(self):

        logging.info("Loading ES Ensembl {0} assembly genes and non reference assembly".format(
            Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))

        for row in self.esquery.get_all_ensembl_genes():
            self.ensembl_current[row["id"]] = row
            # put the ensembl_id in symbols too
            display_name = row["display_name"]
            if display_name not in self.symbols:
                self.symbols[display_name] = {}
                self.symbols[display_name]["assembly_name"] = row["assembly_name"]
                self.symbols[display_name]["ensembl_release"] = row["ensembl_release"]
            if row["is_reference"]:
                self.symbols[display_name]["ensembl_primary_id"] = row["id"]
            else:
                if "ensembl_secondary_id" not in self.symbols[display_name] or row["id"] < \
                        self.symbols[display_name]["ensembl_secondary_id"]:
                    self.symbols[display_name]["ensembl_secondary_id"] = row["id"];
                if "ensembl_secondary_ids" not in self.symbols[display_name]:
                    self.symbols[display_name]["ensembl_secondary_ids"] = []
                self.symbols[display_name]["ensembl_secondary_ids"].append(row["id"])

        logging.info("Loading ES Ensembl finished")


    def store_gene_mapping(self):
        '''
            Stores the relation between UniProt, Ensembl, HGNC and the gene symbols
            in one table called gene_mapping_lookup.
            It's a snapshot of the UniProt, Ensembl and HGNC information at a given time
        '''
        logging.info("Stitching everything together")
        data = {'symbols': self.symbols, 'uniprot': self.uniprot_current, 'ensembl': self.ensembl_current}
        print(json.dumps(data, indent=4));

        now = datetime.utcnow()
        today = datetime.strptime("{:%Y-%m-%d}".format(datetime.now()), '%Y-%m-%d')
        # Truncate table
        self.session.query(GeneMappingLookup).delete()
        hr = GeneMappingLookup(
                last_updated=today,
                data=data
        )
        self.session.add(hr)
        self.session.commit()
        logging.info(
            "inserted gene mapping information in JSONB format in the gene_mapping_lookup table at {:%d, %b %Y}".format(
                today))

    def load_gene_mapping(self):
        '''
            Loads the relation between UniProt, Ensembl, HGNC and the gene symbols
            from one table called gene_mapping_lookup.
        '''

        logging.info("Loading mapping between Ensembl, UniProt, HGNC and gene symbols")
        for row in self.session.query(GeneMappingLookup).order_by(desc(GeneMappingLookup.last_updated)).limit(1):
            data = row.data
            self.symbols = data['symbols']
            self.uniprot_current = data['uniprot']
            #logging.info("Uniprot dictionary contains {0} entries".format(len(self.uniprot_current.keys())))
            #logging.info(json.dumps(self.uniprot_current.keys()))
            self.ensembl_current = data['ensembl']

    def load_eco(self):

        logging.info("Loading ECO current valid terms")
        for row in self.session.query(ECONames):
            self.eco_current[row.uri] = row.label

    def load_ontology(self, name, base_class, current, obsolete):
        '''
        Load ontology to accept phenotype terms that are not
        :return:
        '''
        sparql_query = '''
        SELECT DISTINCT ?ont_node ?label
        FROM <http://purl.obolibrary.org/obo/%s.owl>
        {
        ?ont_node rdfs:subClassOf* <%s> .
        ?ont_node rdfs:label ?label
        }
        '''
        self.sparql.setQuery(sparql_query%(name, base_class))
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        for result in results["results"]["bindings"]:
            uri = result['ont_node']['value']
            label = result['label']['value']
            current[uri] = label
            #print(json.dumps(result, indent=4))
            #print("%s %s"%(uri, label))

        sparql_query = '''
        PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        SELECT DISTINCT ?hp_node ?label ?id ?hp_new
         FROM <http://purl.obolibrary.org/obo/%s.owl>
         FROM <http://purl.obolibrary.org/obo/>
         {
            ?hp_node owl:deprecated true .
            ?hp_node oboInOwl:id ?id .
            ?hp_node obo:IAO_0100001 ?hp_new .
            ?hp_node rdfs:label ?label

         }
        '''
        self.sparql.setQuery(sparql_query%name)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        obsolete_classes = {}

        for result in results["results"]["bindings"]:
            uri = result['hp_node']['value']
            label = result['label']['value']
            id = result['label']['value']
            hp_new = result['hp_new']['value']
            new_label = ''
            if (not re.match('http:\/\/purl.obolibrary\.org', hp_new)):
                hp_new = "http://purl.obolibrary.org/obo/%s"%hp_new.replace(':','_')
            obsolete_classes[uri] = hp_new
        for uri in obsolete_classes:
            next_uri = obsolete_classes[uri]
            while next_uri in obsolete_classes:
                next_uri = obsolete_classes[next_uri]
            new_label = current[next_uri]
            obsolete[uri] = "Use %s label:%s"%(next_uri, new_label)
            print "%s %s"%(uri, obsolete[uri])

    def load_hpo(self):
        '''
        Load HPO to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology('hp', 'http://purl.obolibrary.org/obo/HP_0000118', self.hpo_current, self.hpo_obsolete)

    def load_mp(self):
        '''
        Load MP to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology('mp', 'http://purl.obolibrary.org/obo/MP_0000001', self.mp_current, self.mp_obsolete)

    def load_efo(self):
        # Change this in favor of paths
        logging.info("Loading EFO current terms")
        efo_pattern = "http://www.ebi.ac.uk/efo/EFO_%"
        orphanet_pattern = "http://www.orpha.net/ORDO/Orphanet_%"
        hpo_pattern = "http://purl.obolibrary.org/obo/HP_%"
        mp_pattern = "http://purl.obolibrary.org/obo/MP_%"
        do_pattern = "http://purl.obolibrary.org/obo/DOID_%"
        go_pattern = "http://purl.obolibrary.org/obo/GO_%"
        omim_pattern = "http://purl.bioontology.org/omim/OMIM_%"

        '''
        Temp: store the uncharactered diseases to filter them
        https://alpha.targetvalidation.org/disease/genetic_disorder_uncategorized
        '''
        for row in self.session.query(EFOPath):
            if any(map(lambda x: x["uri"] == 'http://www.targetvalidation.org/genetic_disorder_uncategorized',
                       row.tree_path)):
                self.efo_uncat.append(row.uri);

        for row in self.session.query(EFONames).filter(
                or_(
                        EFONames.uri.like(efo_pattern),
                        EFONames.uri.like(mp_pattern),
                        EFONames.uri.like(orphanet_pattern),
                        EFONames.uri.like(hpo_pattern),
                        EFONames.uri.like(do_pattern),
                        EFONames.uri.like(go_pattern),
                        EFONames.uri.like(omim_pattern)
                )
        ):
            #logging.info(row.uri)
            # if row.uri not in uncat:
            self.efo_current[row.uri] = row.label
        # logging.info(len(self.efo_current))
        # logging.info("Loading EFO obsolete terms")
        for row in self.session.query(EFOObsoleteClass):
            #print "obsolete %s"%(row.uri)
            self.efo_obsolete[row.uri] = row.reason

    def get_reference_gene_from_Ensembl(self, ensembl_gene_id):
        '''
        Given an ensembl gene id return the corresponding reference assembly gene id if it exists.
        It get the gene external name and get the corresponding primary assembly gene id.
        :param self:
        :param ensembl_gene_id: ensembl gene identifier to check
        :return: a string indicating if the gene is mapped to a reference assembly or an alternative assembly only
        '''
        symbol = self.ensembl_current[ensembl_gene_id]['display_name']
        if symbol in self.symbols and 'ensembl_primary_id' in self.symbols[symbol]:
            return self.symbols[symbol]['ensembl_primary_id'] + " " + symbol + " (reference assembly)"
        return symbol + " (non reference assembly)"

    def map_genes(self):
        '''
            This is the preamble step of the pipeline
            It's loading all the Ensembl, UniProt, HGNC and symbol information
            and will store it in the back-end to be shared and used by the validation
            workers
        '''
        self.load_Ensembl()
        self.load_Uniprot()
        self.load_HGNC()
        self.store_gene_mapping()

    def check_all(self):
        '''
        Check every given evidence string
        :return:
        '''

        #self.load_mp()
        #return;

        self.load_gene_mapping()
        # return;
        self.load_efo()
        self.load_hpo()
        self.load_mp()
        self.load_eco()
        self.adapter.close()

        '''
        Create queues
        '''
        file_q = multiprocessing.Queue(maxsize=NB_JSON_FILES + 1)
        evidence_q = multiprocessing.Queue(maxsize=MAX_NB_EVIDENCE_CHUNKS + 1)
        audit_q = multiprocessing.Queue(maxsize=MAX_NB_EVIDENCE_CHUNKS + 1)

        '''
        Create events
        '''
        input_file_loading_finished = multiprocessing.Event()
        input_file_processing_finished = multiprocessing.Event()
        input_file_validation_finished = multiprocessing.Event()
        input_file_auditing_finished = multiprocessing.Event()

        '''
        Create counters (shared memory objects)
        '''
        input_file_count = multiprocessing.Value('i', 0)
        input_file_processed_count = multiprocessing.Value('i', 0)
        input_file_validated_count = multiprocessing.Value('i', 0)
        evidence_loaded_count = multiprocessing.Value('i', 0)
        evidence_validated_count = multiprocessing.Value('i', 0)
        input_file_audited_count = multiprocessing.Value('i', 0)

        '''create locks'''
        file_processing_lock = multiprocessing.Lock()
        log_file_lock = multiprocessing.Lock()
        audit_lock = multiprocessing.Lock()

        workers_number = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        '''
        Start crawling the FTP directory
        '''
        directory_crawler = DirectoryCrawlerProcess(
                file_q,
                self.es,
                input_file_loading_finished,
                input_file_count,
                file_processing_lock)

        directory_crawler.start()

        '''
        Start processing the evidence file from the data providers
        '''
        readers = [FileReaderProcess(file_q,
                                     evidence_q,
                                     self.es,
                                     input_file_loading_finished,
                                     input_file_processing_finished,
                                     input_file_count,
                                     input_file_processed_count,
                                     evidence_loaded_count,
                                     file_processing_lock
                                     ) for i in range(1)]
        # ) for i in range(2)]
        for w in readers:
            w.start()

        '''
        Start processing the evidence in chunks
        on the evidence queue
        '''

        validators = [ValidatorProcess(evidence_q,
                                       audit_q,
                                       self.es,
                                       self.efo_current,
                                       self.efo_uncat,
                                       self.efo_obsolete,
                                       self.hpo_current,
                                       self.hpo_obsolete,
                                       self.mp_current,
                                       self.mp_obsolete,
                                       self.uniprot_current,
                                       self.ensembl_current,
                                       input_file_processing_finished,
                                       input_file_validation_finished,
                                       input_file_processed_count,
                                       input_file_validated_count,
                                       evidence_loaded_count,
                                       evidence_validated_count,
                                       log_file_lock
                                       ) for i in range(workers_number+1)]
        # ) for i in range(2)]
        for w in validators:
            w.start()

        '''
        Audit the whole process and send e-mails
        '''
        auditor = AuditTrailProcess(
                audit_q,
                self.es,
                self.ensembl_current,
                self.uniprot_current,
                self.symbols,
                self.efo_current,
                self.efo_obsolete,
                self.hpo_current,
                self.hpo_obsolete,
                self.mp_current,
                self.mp_obsolete,
                input_file_validation_finished,
                input_file_auditing_finished,
                input_file_validated_count,
                input_file_audited_count,
                audit_lock)

        auditor.start()

        '''wait for other processes to finish'''
        while not input_file_auditing_finished.is_set():
            time.sleep(1)

        if directory_crawler.is_alive():
            directory_crawler.terminate()

        for w in readers:
            if w.is_alive():
                w.terminate()

        for w in validators:
            if w.is_alive():
                w.terminate()

        if auditor.is_alive():
            auditor.terminate()

        return
