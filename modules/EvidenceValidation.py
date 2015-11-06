import os
import sys
reload(sys);
sys.setdefaultencoding("utf8")
import re
import gzip
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.MIMEBase import MIMEBase
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
#import cttv.model.flatten as flat
from settings import Config
import hashlib
from lxml.etree import tostring
from xml.etree import cElementTree as ElementTree
from sqlalchemy.dialects.postgresql import JSON

BLOCKSIZE = 65536


__author__ = 'gautierk'

logger = logging.getLogger(__name__)

# figlet -c "Validation Passed"
messagePassed='''
__     __    _ _     _       _   _               ____                        _
\ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __   |  _ \ __ _ ___ ___  ___  __| |
 \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \  | |_) / _` / __/ __|/ _ \/ _` |
  \ V / (_| | | | (_| | (_| | |_| | (_) | | | | |  __/ (_| \__ \__ \  __/ (_| |
   \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_| |_|   \__,_|___/___/\___|\__,_|
   
'''

messageFailed='''
   __     __    _ _     _       _   _               _____     _ _          _
   \ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __   |  ___|_ _(_) | ___  __| |
    \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \  | |_ / _` | | |/ _ \/ _` |
     \ V / (_| | | | (_| | (_| | |_| | (_) | | | | |  _| (_| | | |  __/ (_| |
      \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_| |_|  \__,_|_|_|\___|\__,_|

'''

cttv_data_providers_e_mails = {
# "cttv001" : [ 'gautierk@targetvalidation.org', 'mmaguire@ebi.ac.uk', 'samiulh@targetvalidation.org', 'andreap@targetvalidation.org' ],
 "cttv001" : [ 'gautierk@targetvalidation.org', 'samiulh@targetvalidation.org' ],
 "cttv006" : [ 'fabregat@ebi.ac.uk' ],
 "cttv007" : [ 'kl1@sanger.ac.uk' ],
 "cttv008" : [ 'mpaulam@ebi.ac.uk', 'patricia@ebi.ac.uk' ],
 "cttv009" : [ 'cleroy@ebi.ac.uk' ],
 "cttv010" : [ 'mkeays@ebi.ac.uk' ],
 "cttv011" : [ 'eddturner@ebi.ac.uk' ],
 "cttv012" : [ 'fjlopez@ebi.ac.uk', 'garys@ebi.ac.uk' ],
 "cttv025" : [ 'kafkas@ebi.ac.uk', 'ftalo@ebi.ac.uk' ] 
}

efo_current = {}
efo_obsolete = {}
ensembl_current = {}
uniprot_current = {}
eco_current = {}

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
    
    def send_email(self, bSend, provider_id, filename, bValidated, nb_records, errors, when, extra_text, logfile):
        me = "support@targetvalidation.org"
        you = ",".join(cttv_data_providers_e_mails[provider_id])
        status = "passed"
        if not bValidated:
            status = "failed"
        # Create message container - the correct MIME type is multipart/alternative.
        #msg = MIMEMultipart('alternative')
        msg = MIMEMultipart()
        msg['Subject'] = "CTTV: Validation of submitted file {0} {1}".format(filename, status)
        msg['From'] = me
        msg['To'] = you
        rcpt = cttv_data_providers_e_mails[provider_id]
        if provider_id != 'cttv001':
            rcpt.extend(cttv_data_providers_e_mails['cttv001'])
            msg['Cc'] = ",".join(cttv_data_providers_e_mails['cttv001'])

        text = "This is an automated message generated by the CTTV Core Platform Pipeline on {0}\n".format(when)

        if bValidated:
            text += messagePassed
            text += "Congratulations :)\n"
        else:
            text += messageFailed
            text += "See details in the attachment {0}\n\n".format(os.path.basename(logfile))
        text += "JSON schema version:\t1.2.1\n"
        text += "Number of evidence strings:\t{0}\n".format(nb_records)
        for key in errors:
            text += "Number of {0}:\t{1}\n".format(key, errors[key])
        text += "\n"
        text += extra_text
        text += "\nYours\nThe CTTV Core Platform Team"
        #text += signature
        print text
        
        if bSend:
            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(text, 'plain')
            
            # Attach parts into message container.
            # According to RFC 2046, the last part of a multipart message, in this case
            # the HTML message, is best and preferred.
            msg.attach(part1)
            
            if not bValidated:
                part2 = MIMEBase('application', "octet-stream")
                part2.set_payload( open(logfile,"rb").read() )
                Encoders.encode_base64(part2)
                part2.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(logfile)))
                msg.attach(part2)        
            
            # Send the message via local SMTP server.
            mail = smtplib.SMTP('smtp.office365.com', 587)

            mail.ehlo()

            mail.starttls()

            mail.login(me, 'P@ssword')
            mail.sendmail(me, rcpt, msg.as_string())
            mail.quit()
        return 0;
    
    def load_HGNC(self):
        logging.info("Loading HGNC entries and mapping from Ensembl Gene Id to UniProt")
        c = 0
        for row in self.session.query(HgncInfoLookup).order_by(desc(HgncInfoLookup.last_updated)).limit(1):
            #data = json.loads(row.data);
            for doc in row.data["response"]["docs"]:
                if "ensembl_gene_id" in doc and "uniprot_ids" in doc:
                    ensembl_gene_id = doc["ensembl_gene_id"];
                    for uniprot_id in doc["uniprot_ids"]:
                        if uniprot_id in uniprot_current and ensembl_gene_id in ensembl_current:
                            #print uniprot_id, " ", ensembl_gene_id, "\n"
                            if uniprot_current[uniprot_id] is None:
                                uniprot_current[uniprot_id] = [ensembl_gene_id];
                            else:
                                uniprot_current[uniprot_id].append(ensembl_gene_id);
           
        logging.info("%i entries parsed for HGNC" % len(row.data["response"]["docs"]))
        
    def load_Uniprot(self):
        logging.info("Loading Uniprot identifiers and mappings to Ensembl Gene Id")
        c = 0
        for row in self.session.query(UniprotInfo).yield_per(1000):
            #
            uniprot_accession = row.uniprot_accession
            uniprot_current[uniprot_accession] = None;
            root = ElementTree.fromstring(row.uniprot_entry)
            gene_id = None
            for crossref in root.findall(".//ns0:dbReference[@type='Ensembl']/ns0:property[@type='gene ID']", { 'ns0' : 'http://uniprot.org/uniprot'} ):        
                ensembl_gene_id = crossref.get("value")
                if ensembl_gene_id in ensembl_current:
                    #print uniprot_accession, " ", ensembl_gene_id, "\n"
                    if uniprot_current[uniprot_accession] is None:
                        uniprot_current[uniprot_accession] = [ensembl_gene_id];
                    else:
                        uniprot_current[uniprot_accession].append(ensembl_gene_id)
                    
            #seqrec = UniprotIterator(StringIO(row.uniprot_entry), 'uniprot-xml').next()
            c += 1
            if c % 5000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
            #for accession in seqrec.annotations['accessions']:
            #    uniprot_current.append(accession)
        logging.info("%i entries retrieved for uniprot" % c)
    
    def load_Ensembl(self):
        logging.info("Loading Ensembl {0} assembly genes and non reference assembly".format(Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
        for row in self.session.query(EnsemblGeneInfo).all(): #filter_by(assembly_name = Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY).all():
            #print "%s %s"%(row.ensembl_gene_id, row.external_name)
            ensembl_current[row.ensembl_gene_id] = row
            
    def load_eco(self):

        logging.info("Loading ECO current valid terms")
        for row in self.session.query(ECONames):
            eco_current[row.uri] = row.label
    
    def load_efo(self):
        # Change this in favor of paths
        logging.info("Loading EFO current terms")
        efo_pattern = "http://www.ebi.ac.uk/efo/EFO_%"
        orphanet_pattern = "http://www.orpha.net/ORDO/Orphanet_%"
        hpo_pattern = "http://purl.obolibrary.org/obo/HP_%"
        mp_pattern = "http://purl.obolibrary.org/obo/MP_%"
        do_pattern = "http://purl.obolibrary.org/obo/DOID_%"
        go_pattern = "http://purl.obolibrary.org/obo/GO_%" 
        
        for row in self.session.query(EFONames).filter(
                or_(
                    EFONames.uri.like(efo_pattern), 
                    EFONames.uri.like(mp_pattern), 
                    EFONames.uri.like(orphanet_pattern), 
                    EFONames.uri.like(hpo_pattern), 
                    EFONames.uri.like(do_pattern), 
                    EFONames.uri.like(go_pattern)
                    )
                ):
            #logging.info(row.uri)
            efo_current[row.uri] = row.label
        #logging.info(len(efo_current))
        #logging.info("Loading EFO obsolete terms")
        for row in self.session.query(EFOObsoleteClass):
            #print "obsolete %s"%(row.uri)
            efo_obsolete[row.uri] = row.reason
            
    def get_reference_gene_from_list(self, genes):
        for ensembl_gene_id in genes:
            if ensembl_current[ensembl_gene_id].is_reference:
                return ensembl_gene_id + " " + ensembl_current[ensembl_gene_id].external_name + " (reference assembly)"
        return ", ".join(genes) + " (non reference assembly)"

    def get_reference_gene_from_Ensembl(self, ensembl_gene_id):
        symbol = ensembl_current[ensembl_gene_id].external_name;
        for row in self.session.query(EnsemblGeneInfo).filter_by(assembly_name = Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, is_reference=True, external_name = symbol).all():
            return row.ensembl_gene_id + " " + row.external_name + " (reference assembly)"
        return symbol + " (non reference assembly)"
        
    def check_all(self):
 
        self.load_Ensembl(); 
        self.load_Uniprot();
        self.load_HGNC();

        self.load_efo();
        self.load_eco();
        
        for dirname, dirnames, filenames in os.walk(Config.EVIDENCEVALIDATION_FTP_SUBMISSION_PATH):
            dirnames.sort()
            for subdirname in dirnames:
                cttv_match = re.match("^(cttv[0-9]{3})$", subdirname)
                #cttv_match = re.match("^(cttv025)$", subdirname)
                if cttv_match:
                    provider_id = cttv_match.groups()[0]
                    cttv_dir = os.path.join(dirname, subdirname)
                    logging.info(cttv_dir)
                    for cttv_dirname, cttv_dirs, filenames in os.walk(os.path.join(cttv_dir, "upload/submissions")):
                        for filename in filenames:
                            logging.info(filename);
                            if filename.endswith(('.json.gz')): # and filename == 'cttv025-03-11-2015.json.gz':
                                cttv_file = os.path.join(cttv_dirname, filename)
                                logging.info(cttv_file);
                                last_modified = os.path.getmtime(cttv_file)
                                #july = time.strptime("01 Jul 2015", "%d %b %Y")
                                #julyseconds = time.mktime(july)
                                sep = time.strptime("20 Oct 2015", "%d %b %Y")
                                sepseconds = time.mktime(sep)
                                if( last_modified - sepseconds ) > 0:
                                    m = re.match("^(.+).json.gz$", filename)
                                    logfile = os.path.join(cttv_dirname, m.groups()[0] + "_log.txt")
                                    logging.info(cttv_file)
                                    md5_hash = self.check_gzipfile(cttv_file)
                                    self.validate_gzipfile(cttv_file, filename, provider_id, md5_hash, logfile = logfile)

        self.session.commit()
        
    def validate_gzipfile(self, file_on_disk, filename, provider_id, md5_hash, logfile = None):
        '''
        check if the file was already processed
        '''
        bValidate = False
        bGivingUp = False
        rowToUpdate = None
        count = self.session.query(EvidenceValidation.filename).filter_by(filename=file_on_disk).count()
        logging.info('Was the file parsed already? %i'%(count))
        if count == 0:
            bValidate = True
        else:
            for row in self.session.query(EvidenceValidation).filter_by(filename=file_on_disk):
                if row.md5 == md5_hash:
                    logging.info('%s == %s'% (row.md5, md5_hash))
                    logging.info('%s file already recorded. Won\'t parse'%file_on_disk)
                    return;
                else:
                    logging.info('%s != %s'% (row.md5, md5_hash))
                    bValidate = True
                    rowToUpdate = row
                    break;
        logging.info('bValidate %r'% (bValidate))
        # Check EFO overrepresentation
        # Check target overrepresentation
        diseases = {}
        top_diseases = []
        targets = {}
        top_targets = []
        obsolete_diseases = {}
        invalid_diseases = {}
        invalid_ensembl_ids = {}
        nonref_ensembl_ids = {}
        invalid_uniprot_ids = {}
        invalid_uniprot_id_mappings = {}
        
        if bValidate == True:
            logging.info('Starting validation of %s'% (file_on_disk))
            fh = gzip.GzipFile(file_on_disk, "r")
            #lfh = gzip.open(logfile, 'wb', compresslevel=5)
            lfh = open(logfile, 'wb')
            cc = 0
            lc = 0
            nb_errors = 0
            nb_duplicates = 0
            nb_efo_invalid = 0
            nb_efo_obsolete = 0
            nb_ensembl_invalid = 0
            nb_ensembl_nonref = 0
            nb_uniprot_invalid = 0
            nb_uniprot_invalid_mapping = 0
            hexdigest_map = {}
            for line in fh:
                #logging.info(line)
                python_raw = json.loads(line)
                # now validate
                obj = None
                validation_result = 0
                validation_failed = False
                disease_failed = False
                gene_failed = False
                gene_mapping_failed = False
                
                
                if ('label' in python_raw  or 'type' in python_raw) and 'validated_against_schema_version' in python_raw and python_raw['validated_against_schema_version'] == "1.2.1":
                    if 'label' in python_raw:
                        python_raw['type'] = python_raw.pop('label', None)
                    data_type = python_raw['type']
                    #logging.info('type %s'%data_type)
                    if data_type in ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']:
                        if data_type == 'genetic_association':
                            obj = cttv.Genetics.fromMap(python_raw)
                        elif data_type == 'rna_expression':
                            obj = cttv.Expression.fromMap(python_raw)
                        elif data_type in ['genetic_literature', 'affected_pathway', 'somatic_mutation']:
                            obj = cttv.Literature_Curated.fromMap(python_raw)
                        elif data_type == 'known_drug':
                            obj = cttv.Drug.fromMap(python_raw)
                            #logging.info(obj.evidence.association_score.__class__.__name__)
                            #logging.info(obj.evidence.target2drug.association_score.__class__.__name__)
                            #logging.info(obj.evidence.drug2clinic.association_score.__class__.__name__)
                        elif data_type == 'literature':
                            obj = cttv.Literature_Mining.fromMap(python_raw)
                        elif data_type == 'animal_model':
                            obj = cttv.Animal_Models.fromMap(python_raw)
                        
                        if obj:

                            if obj.target.id:
                                for id in obj.target.id: 
                                    if id in targets:
                                        targets[id] +=1
                                    else:
                                        targets[id] = 1
                                    if not id in top_targets:
                                        if len(top_targets) < Config.EVIDENCEVALIDATION_NB_TOP_TARGETS:
                                            top_targets.append(id)
                                        else:
                                            # map,reduce
                                            for n in range(0,len(top_targets)):
                                                if targets[top_targets[n]] < targets[id]:
                                                    top_targets[n] = id;
                                                    break;
                                            
                            if obj.disease.id:
                                for id in obj.disease.id:
                                    if id in diseases:
                                        diseases[id] +=1
                                    else:
                                        diseases[id] =1
                                    if not id in top_diseases:
                                        if len(top_diseases) < Config.EVIDENCEVALIDATION_NB_TOP_DISEASES:
                                            top_diseases.append(id)
                                        else:
                                            # map,reduce
                                            for n in range(0,len(top_diseases)):
                                                if diseases[top_diseases[n]] < diseases[id]:
                                                    top_diseases[n] = id;
                                                    break;
                                            
                            if not bGivingUp:  
                                self.startCapture(logging.ERROR)
                                
                            uniq_elements = obj.unique_association_fields
                            uniq_elements_flat = flat.DatatStructureFlattener(uniq_elements)
                            uniq_elements_flat_hexdig = uniq_elements_flat.get_hexdigest()
                            if not uniq_elements_flat_hexdig in hexdigest_map:
                                hexdigest_map[uniq_elements_flat_hexdig] = [ lc+1 ]
                            else:
                                hexdigest_map[uniq_elements_flat_hexdig].append(lc+1)                          
                                logger.error("Line {0}: Duplicated unique_association_fields on lines {1}".format(lc+1, ",".join(map(lambda x: "%i"%x,  hexdigest_map[uniq_elements_flat_hexdig]))))
                                nb_duplicates = nb_duplicates + 1
                                validation_failed = True
      
                            validation_result = obj.validate(logger)
                            nb_errors = nb_errors + validation_result
                            
                            '''
                            Check EFO
                            '''
                            if obj.disease.id:
                                for disease_id in obj.disease.id:
                                    if disease_id not in efo_current:
                                        logger.error("Line {0}: Invalid disease term detected {1}. Please provide the correct EFO disease term".format(lc+1, disease_id))
                                        disease_failed = True
                                        if disease_id not in invalid_diseases:
                                            invalid_diseases[disease_id] = 1
                                        else:
                                            invalid_diseases[disease_id] += 1
                                        nb_efo_invalid +=1
                                    if disease_id in efo_obsolete:
                                        logger.error("Line {0}: Obsolete disease term detected {1} ('{2}'): {3}".format(lc+1, disease_id, efo_current[disease_id], efo_obsolete[disease_id]))
                                        disease_failed = True
                                        if disease_id not in obsolete_diseases:
                                            obsolete_diseases[disease_id] = 1
                                        else:
                                            obsolete_diseases[disease_id] += 1
                                        nb_efo_obsolete +=1
                                    
                            '''
                            Check Ensembl ID, UniProt ID and UniProt ID mapping to a Gene ID
                            '''
                            if obj.target.id:
                                for id in obj.target.id:
                                    # http://identifiers.org/ensembl/ENSG00000178573
                                    ensemblMatch = re.match('http://identifiers.org/ensembl/(ENSG\d+)', id)
                                    uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', id)
                                    if ensemblMatch:
                                        ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
                                        if not ensembl_id in ensembl_current:
                                            gene_failed = True
                                            logger.error("Line {0}: Unknown Ensembl gene detected {1}. Please provide a correct gene identifier on the reference genome assembly {2}".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not ensembl_id in invalid_ensembl_ids:
                                                invalid_ensembl_ids[ensembl_id] = 1
                                            else:
                                                invalid_ensembl_ids[ensembl_id] += 1
                                            nb_ensembl_invalid +=1
                                        elif not ensembl_current[ensembl_id].is_reference:
                                            gene_mapping_failed = True
                                            logger.error("Line {0}: Human Alternative sequence Ensembl Gene detected {1}. We will attempt to map it to a gene identifier on the reference genome assembly {2} or choose a Human Alternative sequence Ensembl Gene Id".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not ensembl_id in invalid_ensembl_ids:
                                                nonref_ensembl_ids[ensembl_id] = 1
                                            else:
                                                nonref_ensembl_ids[ensembl_id] += 1
                                            nb_ensembl_nonref +=1                                            
                                    elif uniprotMatch:
                                        uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                                        if not uniprot_id in uniprot_current:
                                            gene_failed = True
                                            logger.error("Line {0}: Invalid UniProt entry detected {1}. Please provide a correct identifier".format(lc+1, uniprot_id))
                                            if not uniprot_id in invalid_uniprot_ids:
                                                invalid_uniprot_ids[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_ids[uniprot_id] += 1
                                            nb_uniprot_invalid +=1
                                        elif uniprot_current[uniprot_id] is None:
                                            gene_failed = True
                                            logger.error("Line {0}: Invalid UniProt entry detected {1}. This UniProt entry does not have any cross-reference to an Ensembl Gene Id.".format(lc+1, uniprot_id))
                                            if not uniprot_id in invalid_uniprot_ids:
                                                invalid_uniprot_ids[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_ids[uniprot_id] += 1
                                            nb_uniprot_invalid +=1
                                            #This identifier is not in the current EnsEMBL database 
                                        elif not reduce( (lambda x, y: x or y), map(lambda x: ensembl_current[x].is_reference, uniprot_current[uniprot_id]) ):
                                            gene_mapping_failed = True
                                            logger.error("Line {0}: The UniProt entry {1} does not have a cross-reference to an Ensembl Gene Id on the reference genome assembly {2}. It will be mapped to a Human Alternative sequence Ensembl Gene Id.".format(lc+1, uniprot_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not uniprot_id in invalid_uniprot_id_mappings:
                                                invalid_uniprot_id_mappings[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_id_mappings[uniprot_id] += 1
                                            nb_uniprot_invalid_mapping +=1
                                            
                            if not bGivingUp:  
                                logs = self.stopCapture()
                                
                        else:
                            if not bGivingUp:
                                self.startCapture(logging.ERROR)
                                logger.error("Line {0}: Not a valid 1.2.1 evidence string - There was an error parsing the JSON document. The document may contain an invalid field".format(lc+1))
                                logs = self.stopCapture()
                            nb_errors += 1
                            validation_failed = True
                                                
                    else:
                        if not bGivingUp:
                            self.startCapture(logging.ERROR)
                            logger.error("Line {0}: '{1}' is not a valid 1.2.1 evidence string type".format(lc+1, data_type))
                            logs = self.stopCapture()
                        nb_errors += 1
                        validation_failed = True
                    
                elif not 'validated_against_schema_version' in python_raw or ('validated_against_schema_version' in python_raw and python_raw['validated_against_schema_version'] != "1.2.1"):
                    if not bGivingUp:
                        self.startCapture(logging.ERROR)
                        logger.error("Line {0}: Not a valid 1.2.1 evidence string - please check the 'validated_against_schema_version' mandatory attribute".format(lc+1))
                        logs = self.stopCapture()
                    nb_errors += 1
                    validation_failed = True
                else:
                    if not bGivingUp:
                        self.startCapture(logging.ERROR)
                        logger.error("Line {0}: Not a valid 1.2.1 evidence string - please add the mandatory 'type' attribute".format(lc+1))
                        logs = self.stopCapture()
                    nb_errors += 1
                    validation_failed = True
                    
                if (validation_failed or validation_result > 0 or disease_failed or gene_failed or gene_mapping_failed) and not bGivingUp:
                    if obj:
                        lfh.write("line {0} - {1}".format(lc+1, json.dumps(obj.unique_association_fields)))
                    else:
                        lfh.write("line {0} ".format(lc+1))
                    lfh.write(logs)
                    if nb_errors > Config.EVIDENCEVALIDATION_MAX_NB_ERRORS_REPORTED or nb_duplicates > Config.EVIDENCEVALIDATION_MAX_NB_ERRORS_REPORTED:
                        lfh.write("Too many errors: giving up.\n")
                        bGivingUp = True
                    
                lc += 1
                cc += len(line)
            logging.info('nb line parsed %i (size %i)'% (lc, cc))
            fh.close()
            lfh.close()
            
            # write top diseases / top targets
            text = ""
            if top_diseases:
                text +="Top %i diseases:\n"%(Config.EVIDENCEVALIDATION_NB_TOP_DISEASES)
                for n in range(0,len(top_diseases)):
                    if top_diseases[n] in efo_current:
                        text +="\t-{0}:\t{1} ({2:.1f}%) {3}\n".format(top_diseases[n], diseases[top_diseases[n]], diseases[top_diseases[n]]*100/lc, efo_current[top_diseases[n]])
                    else:
                        text +="\t-{0}:\t{1} ({2:.1f}%)\n".format(top_diseases[n], diseases[top_diseases[n]], diseases[top_diseases[n]]*100/lc)
                text +="\n"
            if top_targets:
                text +="Top %i targets:\n"%(Config.EVIDENCEVALIDATION_NB_TOP_TARGETS)
                for n in range(0,len(top_targets)):
                    id = top_targets[n];
                    id_text = None
                    ensemblMatch = re.match('http://identifiers.org/ensembl/(ENSG\d+)', id)
                    uniprotMatch = re.match('http://identifiers.org/uniprot/(.{4,})$', id)
                    if ensemblMatch:
                        ensembl_id = ensemblMatch.groups()[0].rstrip("\s")   
                        id_text = self.get_reference_gene_from_Ensembl(ensembl_id);
                    elif uniprotMatch:
                        uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                        id_text = self.get_reference_gene_from_list(uniprot_current[uniprot_id]);
                    text +="\t-{0}:\t{1} ({2:.1f}%) {3}\n".format(top_targets[n], targets[top_targets[n]], targets[top_targets[n]]*100/lc, id_text)
                text +="\n"

            # report invalid/obsolete EFO term
            if nb_efo_invalid > 0:
                text +="Errors:\n"
                text +="\t%i invalid EFO term(s) found in %i (%.1f%s) of the records.\n"%(len(invalid_diseases), nb_efo_invalid, nb_efo_invalid*100/lc, '%' )
                for disease_id in invalid_diseases:
                    if invalid_diseases[disease_id] == 1:
                        text += "\t%s\t(reported once)\n"%(disease_id)
                    else:
                        text += "\t%s\t(reported %i times)\n"%(disease_id, invalid_diseases[disease_id])
                      
                text +="\n"
            if nb_efo_obsolete > 0:
                text +="Errors:\n"
                text +="\t%i obsolete EFO term(s) found in %i (%.1f%s) of the records.\n"%(len(obsolete_diseases), nb_efo_obsolete, nb_efo_obsolete*100/lc, '%' )
                for disease_id in obsolete_diseases:
                    if obsolete_diseases[disease_id] == 1:
                        text += "\t%s\t(reported once)\t%s\n"%(disease_id, efo_obsolete[disease_id].replace("\n", " "))
                    else:
                        text += "\t%s\t(reported %i times)\t%s\n"%(disease_id, obsolete_diseases[disease_id], efo_obsolete[disease_id].replace("\n", " "))
                text +="\n"

            # report invalid Ensembl genes
            if nb_ensembl_invalid > 0:
                text +="Errors:\n"
                text +="\t%i unknown Ensembl identifier(s) found in %i (%.1f%s) of the records.\n"%(len(invalid_ensembl_ids), nb_ensembl_invalid, nb_ensembl_invalid*100/lc, '%' )
                for ensembl_id in invalid_ensembl_ids:
                    if invalid_ensembl_ids[ensembl_id] == 1:
                        text += "\t%s\t(reported once)\n"%(ensembl_id)
                    else:
                        text += "\t%s\t(reported %i times)\n"%(ensembl_id, invalid_ensembl_ids[ensembl_id])
                text +="\n"

            # report Ensembl genes not on reference assembly
            if nb_ensembl_nonref > 0:
                text +="Warnings:\n"
                text +="\t%i Ensembl Human Alternative sequence Gene identifier(s) not mapped to the reference genome assembly %s found in %i (%.1f%s) of the records.\n"%(len(nonref_ensembl_ids), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, nb_ensembl_nonref, nb_ensembl_nonref*100/lc, '%' )
                text +="\tPlease map them to a reference assembly gene if possible.\n"
                text +="\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifier.\n"
                for ensembl_id in nonref_ensembl_ids:
                    if nonref_ensembl_ids[ensembl_id] == 1:
                        text += "\t%s\t(reported once) maps to %s\n"%(ensembl_id, self.get_reference_gene_from_Ensembl(ensembl_id))
                    else:
                        text += "\t%s\t(reported %i times) maps to %s\n"%(ensembl_id, nonref_ensembl_ids[ensembl_id], self.get_reference_gene_from_Ensembl(ensembl_id))
                text +="\n"
                
            # report invalid Uniprot entries
            if nb_uniprot_invalid > 0:
                text +="Errors:\n"
                text +="\t%i invalid UniProt identifier(s) found in %i (%.1f%s) of the records.\n"%(len(invalid_uniprot_ids), nb_uniprot_invalid, nb_uniprot_invalid*100/lc, '%' )
                for uniprot_id in invalid_uniprot_ids:
                    if invalid_uniprot_ids[uniprot_id] == 1:
                        text += "\t%s\t(reported once)\n"%(uniprot_id)
                    else:
                        text += "\t%s\t(reported %i times)\n"%(uniprot_id, invalid_uniprot_ids[uniprot_id])
                text +="\n"

            # report invalid Uniprot mapping entries
            if nb_uniprot_invalid_mapping > 0:
                text +="Warnings:\n"
                text +="\t%i distinct UniProt identifier(s) not mapped to Ensembl reference genome assembly %s gene identifiers found in %i (%.1f%s) of the records.\n"%(len(invalid_uniprot_id_mappings), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, nb_uniprot_invalid_mapping, nb_uniprot_invalid_mapping*100/lc, '%' )
                text +="\tIf you think that might be an error in your submission, please use a UniProt identifier that will map to a reference assembly gene identifier.\n"
                text +="\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifiers.\n"
                for uniprot_id in invalid_uniprot_id_mappings:
                    if invalid_uniprot_id_mappings[uniprot_id] == 1:
                        text += "\t%s\t(reported once) maps to %s\n"%(uniprot_id, self.get_reference_gene_from_list(uniprot_current[uniprot_id]))
                    else:
                        text += "\t%s\t(reported %i times) maps to %s\n"%(uniprot_id, invalid_uniprot_id_mappings[uniprot_id], self.get_reference_gene_from_list(uniprot_current[uniprot_id]))
                text +="\n"
                
            now = datetime.utcnow()

            # A file is successfully validated if it meets the following conditions
            successfully_validated = (nb_errors == 0 and nb_duplicates == 0 and nb_efo_invalid == 0 and nb_efo_obsolete == 0 and nb_ensembl_invalid == 0 and nb_uniprot_invalid == 0)
            
            if count == 0:
                # insert
                f = EvidenceValidation(
                    provider_id = provider_id, 
                    filename = file_on_disk,
                    md5 = md5_hash,
                    date_created = now,
                    date_modified = now,
                    date_validated = now,
                    nb_submission = 1,
                    nb_records = lc,
                    nb_errors = nb_errors,
                    nb_duplicates = nb_duplicates,
                    successfully_validated = successfully_validated
                )
                self.session.add(f)
                logging.info('inserted %s file in the validation table'%file_on_disk)
            else:
                # update database
                rowToUpdate.md5 = md5_hash
                rowToUpdate.nb_records = lc
                rowToUpdate.nb_errors = nb_errors
                rowToUpdate.nb_duplicates = nb_duplicates
                rowToUpdate.date_modified = now
                rowToUpdate.date_validated = now
                rowToUpdate.successfully_validated = successfully_validated
                self.session.add(rowToUpdate)
    
            self.send_email(
                Config.EVIDENCEVALIDATION_SEND_EMAIL, 
                provider_id, 
                filename, 
                successfully_validated, 
                lc, 
                { 'JSON errors': nb_errors, 'duplicates': nb_duplicates, 'invalid EFO terms': nb_efo_invalid, 'obsolete EFO terms': nb_efo_obsolete, 'invalid Ensembl ids': nb_ensembl_invalid, 'Human Alternative sequence Gene Ensembl ids (warning)': nb_ensembl_nonref, 'invalid Uniprot ids': nb_uniprot_invalid, 'Uniprot ids not mapped to a reference assembly Ensembl gene (warning)': nb_uniprot_invalid_mapping }, 
                now, 
                text, 
                logfile
                )
                                
    
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

                