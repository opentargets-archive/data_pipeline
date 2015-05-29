import os
import sys
import re
import gzip
import logging
from StringIO import StringIO
import json
from json import JSONDecoder
from json import JSONEncoder
from datetime import datetime
from sqlalchemy import and_, table, column, select, update, insert
from common import Actions
from common.PGAdapter import *
import cttv.model.core as cttv
from settings import Config
import hashlib
BLOCKSIZE = 65536

__author__ = 'gautierk'

logger = logging.getLogger(__name__)

class EvidenceValidationActions(Actions):
    CHECKFILES='checkfiles'
    VALIDATE='validate'

class EvidenceValidationFileChecker():

    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session
        #formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        #self.buffer = StringIO()
        #streamhandler = logging.StreamHandler(self.buffer)
        #streamhandler.setFormatter(formatter)
        #memoryhandler = logging.handlers.MemoryHandler(1024*10, logging.DEBUG, streamhandler)        
        #LOGGER = logging.getLogger('cttv.model.core')
        #LOGGER.setLevel(logging.ERROR)
        #LOGGER.addHandler(memoryhandler)
        #LOGGER = logging.getLogger('cttv.model.evidence.core')
        #LOGGER.setLevel(logging.ERROR)
        #LOGGER.addHandler(memoryhandler)        
        #LOGGER = logging.getLogger('cttv.model.evidence.association_score')
        #LOGGER.setLevel(logging.ERROR)
        #LOGGER.addHandler(memoryhandler)

    def startCapture(self, newLogLevel = None):
        """ Start capturing log output to a string buffer.
        
        http://docs.python.org/release/2.6/library/logging.html
        
        @param newLogLevel: Optionally change the global logging level, e.g. logging.DEBUG
        """
        self.buffer = StringIO()
        self.logHandler = logging.StreamHandler(self.buffer)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        self.logHandler.setFormatter(formatter)
        
        #print >> self.buffer, "Log output"
        
        #for module in [ 'cttv.model.core', 'cttv.model.evidence.core', 'cttv.model.evidence.association_score' ]:
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
        #for module in [ 'cttv.model.core', 'cttv.model.evidence.core', 'cttv.model.evidence.association_score' ]:
        rootLogger = logging.getLogger()        

        # Restore logging level (if any)
        if self.oldLogLevel:
            rootLogger.setLevel(self.oldLogLevel)

        rootLogger.removeHandler(self.logHandler)
        
        self.logHandler.flush()
        self.buffer.flush()
        
        return self.buffer.getvalue()
        
    def check_all(self):
    
        for dirname, dirnames, filenames in os.walk(Config.EVIDENCEVALIDATION_FTP_SUBMISSION_PATH):
            for subdirname in dirnames:
                #cttv_match = re.match("^(cttv[0-9]{3})$", subdirname)
                cttv_match = re.match("^(cttv010)$", subdirname)
                if cttv_match:
                    provider_id = cttv_match.groups()[0]
                    cttv_dir = os.path.join(dirname, subdirname)
                    print(cttv_dir)
                    for cttv_dirname, cttv_dirs, filenames in os.walk(os.path.join(cttv_dir, "upload/submissions")):
                        for filename in filenames:
                            if filename.endswith(('.json.gz')):
                                cttv_file = os.path.join(cttv_dirname, filename)
                                m = re.match("^(.+).json.gz$", filename)
                                logfile = os.path.join(cttv_dirname, m.groups()[0] + ".log")
                                print(cttv_file)
                                md5_hash = self.check_gzipfile(cttv_file)
                                self.validate_gzipfile(cttv_file, provider_id, md5_hash, logfile = logfile)

        self.session.commit()
        
    def validate_gzipfile(self, filename, provider_id, md5_hash, logfile = None):
        fh = gzip.GzipFile(filename, "r")
        lfh = open(logfile, 'w')
        cc = 0
        lc = 0
        nb_errors = 0
        for line in fh:
            #logging.info(line)
            python_raw = json.loads(line)
            # now validate
            obj = None
            if ('label' in python_raw  or 'type' in python_raw) and 'validated_against_schema_version' in python_raw and python_raw['validated_against_schema_version'] == "1.2":
                if 'label' in python_raw:
                    python_raw['type'] = python_raw.pop('label', None)
                data_type = python_raw['type']
                logging.info('type %s'%data_type)
                if data_type == 'genetics_evidence_string':
                    obj = cttv.Genetics.fromMap(python_raw)
                elif data_type == 'expression_evidence_string':
                    obj = cttv.Expression.fromMap(python_raw)
                elif data_type in ["genetics_curated_literature_evidence_string", "affected_pathways_curated_literature_evidence_string", "somatic_mutations_curated_literature_evidence_string"]:
                    obj = cttv.Literature_Curated.fromMap(python_raw)
                elif data_type == 'drug_evidence_string':
                    obj = cttv.Drug.fromMap(python_raw)
                    logging.info(obj.evidence.association_score.__class__.__name__)
                    logging.info(obj.evidence.target2drug.association_score.__class__.__name__)
                    logging.info(obj.evidence.drug2clinic.association_score.__class__.__name__)
                elif data_type == 'literature_mining_evidence_string':
                    obj = cttv.Literature_Mining.fromMap(python_raw)
                self.startCapture(logging.ERROR)
                logging.error("line {0} - {1}".format(lc+1, json.dumps(obj.unique_association_fields)))
                validation_result = obj.validate(logger)
                logs = self.stopCapture()
                if validation_result > 0:
                    lfh.write(logs)
                
                nb_errors = nb_errors + validation_result
            lc += 1
            cc += len(line)
        logging.info('nb line parsed %i (size %i)'% (lc, cc))
        fh.close()
        lfh.close()

        now = datetime.utcnow()
        count = self.session.query(EvidenceValidation.filename).filter_by(filename=filename).count()
        print(count)
        if count == 0:
            # insert
            f = EvidenceValidation(
                provider_id = provider_id, 
                filename = filename,
                md5 = md5_hash,
                date_created = now,
                date_modified = now,
                date_validated = now,
                nb_submission = 1,
                nb_records = lc,
                nb_errors = nb_errors,
                successfully_validated = (nb_errors == 0)
            )
            self.session.add(f)
            logging.info('inserted %s file in the validation table'%filename)
        else:
            # retrieve existing md5
            #stmt = select([EvidenceValidation.md5]).filter_by(filename=filename)
            for row in self.session.query(EvidenceValidation).filter_by(filename=filename):
                if row.md5 == md5_hash:
                    logging.info('%s == %s'% (row.md5, md5_hash))
                    logging.info('%s file already recorded'%filename)
                else:
                    logging.info('%s file has to reprocessed'%filename)
                # update database
                row.nb_records = lc
                row.nb_errors = nb_errors
                row.date_modified = now
                row.date_validated = now
                row.successfully_validated = (nb_errors == 0)
                self.session.add(row)
    
    def check_gzipfile(self, filename):
    
        hasher = hashlib.md5()
        with gzip.open(filename,'rb') as afile:
        #with open(filename, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        md5_hash = hasher.hexdigest() 
        print(md5_hash)
        afile.close()
        return md5_hash

                