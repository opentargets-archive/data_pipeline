import os
import sys
# This bit is necessary for text mining data
reload(sys);
sys.setdefaultencoding("utf8");
#blahblah
import re
import gzip
import smtplib
import time
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

eva_curated = {
"http://www.orpha.net/ORDO/Orphanet_1838" : "http://www.orpha.net/ORDO/Orphanet_175",
"http://www.orpha.net/ORDO/Orphanet_68381" : "http://www.orpha.net/ORDO/Orphanet_183497",
"http://www.orpha.net/ORDO/Orphanet_120487" : "http://www.orpha.net/ORDO/Orphanet_903",
"http://www.orpha.net/ORDO/Orphanet_91414" : "http://www.orpha.net/ORDO/Orphanet_183487",
"http://www.orpha.net/ORDO/Orphanet_1047" : "http://www.orpha.net/ORDO/Orphanet_98362",
"http://www.orpha.net/ORDO/Orphanet_121066" : "http://www.orpha.net/ORDO/Orphanet_818", #http://www.orpha.net/ORDO/Orphanet_818
"http://www.orpha.net/ORDO/Orphanet_216675" : "http://www.ebi.ac.uk/efo/EFO_0005269",
"http://www.orpha.net/ORDO/Orphanet_117623" : "http://www.orpha.net/ORDO/Orphanet_827",
"http://www.orpha.net/ORDO/Orphanet_306773" : "http://www.orpha.net/ORDO/Orphanet_3197",
"http://www.orpha.net/ORDO/Orphanet_137646" : "http://www.orpha.net/ORDO/Orphanet_110",
"http://www.orpha.net/ORDO/Orphanet_121730" : "http://www.orpha.net/ORDO/Orphanet_84",
"http://www.orpha.net/ORDO/Orphanet_211247" : "http://www.ebi.ac.uk/efo/EFO_0006888",
"http://www.orpha.net/ORDO/Orphanet_98867" : "http://www.orpha.net/ORDO/Orphanet_288", # obsolete
"http://www.orpha.net/ORDO/Orphanet_99977" : "http://www.ebi.ac.uk/efo/EFO_0005922",
"http://www.orpha.net/ORDO/Orphanet_118746" : "http://www.orpha.net/ORDO/Orphanet_59", # Allan-Herndon-Dudley syndrome
"http://www.orpha.net/ORDO/Orphanet_86834" : "http://www.ebi.ac.uk/efo/EFO_1000309",
"http://www.orpha.net/ORDO/Orphanet_120431" : "http://www.orpha.net/ORDO/Orphanet_79277",
"http://www.orpha.net/ORDO/Orphanet_120935" : "http://www.orpha.net/ORDO/Orphanet_379",
"http://www.orpha.net/ORDO/Orphanet_121722" : "http://www.orpha.net/ORDO/Orphanet_84",
"http://www.orpha.net/ORDO/Orphanet_120345" : "http://www.orpha.net/ORDO/Orphanet_794",
"http://www.orpha.net/ORDO/Orphanet_364559" : "http://www.orpha.net/ORDO/Orphanet_183524",
"http://www.orpha.net/ORDO/Orphanet_122492" : "http://www.orpha.net/ORDO/Orphanet_79430",
"http://www.orpha.net/ORDO/Orphanet_123772" : "http://www.orpha.net/ORDO/Orphanet_636",
"http://www.orpha.net/ORDO/Orphanet_120309": "http://www.orpha.net/ORDO/Orphanet_805",
"http://www.orpha.net/ORDO/Orphanet_117820": "http://www.orpha.net/ORDO/Orphanet_397596", #not mapped. -- a gene only valid this time Activated PI3K-delta syndrome Orphanet_397596
"http://www.orpha.net/ORDO/Orphanet_449": "http://www.ebi.ac.uk/efo/EFO_1000292",
"http://www.orpha.net/ORDO/Orphanet_52416": "http://www.ebi.ac.uk/efo/EFO_0000096", # Mantle cell lymphoma (climbed up ontology)
"http://www.orpha.net/ORDO/Orphanet_251612": "http://www.ebi.ac.uk/efo/EFO_0000272", #(climbed up ontology)
"http://www.orpha.net/ORDO/Orphanet_2965": "http://www.ebi.ac.uk/efo/EFO_0004125", #(climbed up ontology)
"http://www.orpha.net/ORDO/Orphanet_121715": "http://www.orpha.net/ORDO/Orphanet_84",
"http://www.orpha.net/ORDO/Orphanet_121719": "http://www.orpha.net/ORDO/Orphanet_84", #(this and record above are mapped to versions of complementation group)
"http://www.orpha.net/ORDO/Orphanet_767": "http://www.ebi.ac.uk/efo/EFO_0006803", #(Polyarteritis nodosa ->climbed up ontology)
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
"http://www.orpha.net/ORDO/Orphanet_88" : "http://www.ebi.ac.uk/efo/EFO_0006926", # 
"http://www.orpha.net/ORDO/Orphanet_120353" : "http://www.orpha.net/ORDO/Orphanet_79431", # tyrosinase, oculocutaneous albinism IA => Oculocutaneous albinism type 1A
"http://www.orpha.net/ORDO/Orphanet_120795" : "http://www.orpha.net/ORDO/Orphanet_157", # carnitine palmitoyltransferase 2 => Carnitine palmitoyltransferase II deficiency
"http://www.orpha.net/ORDO/Orphanet_121802" : "http://www.orpha.net/ORDO/Orphanet_710", # fibroblast growth factor receptor 1 => Pfeiffer syndrome
"http://www.orpha.net/ORDO/Orphanet_178826" : "http://www.orpha.net/ORDO/Orphanet_65", # spermatogenesis associated 7 => Leber congenital amaurosis
#"Orphanet_132262" : "Orphanet_93262" # Crouzon syndrome with acanthosis nigricans, Crouzon syndrome => need intermediate Crouzon Syndrome in EFO!!!
"http://www.orpha.net/ORDO/Orphanet_123572" : "http://www.orpha.net/ORDO/Orphanet_2170", # 5-methyltetrahydrofolate-homocysteine methyltransferase => METHYLCOBALAMIN DEFICIENCY, cblG TYPE => Methylcobalamin deficiency type cblG
"http://www.orpha.net/ORDO/Orphanet_189330" : "http://www.orpha.net/ORDO/Orphanet_250", # ALX3 mutations leading to Frontonasal dysplasia
"http://www.orpha.net/ORDO/Orphanet_121400" : "http://www.orpha.net/ORDO/Orphanet_976", # APRT mutations leading to Adenine phosphoribosyltransferase deficiency
"http://www.orpha.net/ORDO/Orphanet_1984" : "http://www.orpha.net/ORDO/Orphanet_182050", # obsolete Fechtner syndrome => http://www.orpha.net/ORDO/Orphanet_182050 with label: MYH9-related disease
"http://www.orpha.net/ORDO/Orphanet_79354" : "http://www.orpha.net/ORDO/Orphanet_183426", # map to upper term as EFO does not have this one, should request it!
"http://www.orpha.net/ORDO/Orphanet_850" : "http://www.orpha.net/ORDO/Orphanet_182050", # May-Hegglin thrombocytopenia deprecated => The preferred class is use http://www.orpha.net/ORDO/Orphanet_182050 with label: MYH9-related disease
"http://www.orpha.net/ORDO/Orphanet_123524" : "http://www.orpha.net/ORDO/Orphanet_379", # Granulomatous disease, chronic, autosomal recessive, cytochrome b-negative => Chronic granulomatous disease
"http://www.orpha.net/ORDO/Orphanet_69" : "http://www.orpha.net/ORDO/Orphanet_2118", # wrong eVA mapping!! should be 4-Hydroxyphenylpyruvate dioxygenase deficiency 
"http://www.orpha.net/ORDO/Orphanet_123334" : "http://www.orpha.net/ORDO/Orphanet_99901", # wrong mapping should be Acyl-CoA dehydrogenase 9 deficiency 
"http://www.orpha.net/ORDO/Orphanet_123421" : "http://www.orpha.net/ORDO/Orphanet_361", # wrong mapping in eVA for this gene: mutations lead to ACTH resistance
#"http://www.orpha.net/ORDO/Orphanet_171059" # needs to be refined variant by variant, terms exists in EFO for each of the conditions : methylmalonic aciduria (cobalamin deficiency) cblD type, with homocystinuria in CLinVAR: Homocystinuria, cblD type, variant 1 OR Methylmalonic aciduria, cblD type, variant 2
"http://www.orpha.net/ORDO/Orphanet_158032" : "http://www.orpha.net/ORDO/Orphanet_540", # STX11, STXBP2 => Familial hemophagocytic lymphohistiocytosis 
"http://www.orpha.net/ORDO/Orphanet_159550" : "http://www.orpha.net/ORDO/Orphanet_35698" #POLG : Mitochondrial DNA depletion syndrome
} 

efo_current = {}
efo_obsolete = {}
efo_uncat = []
ensembl_current = {}
uniprot_current = {}
eco_current = {}
symbols = {}

class EvidenceValidationActions(Actions):
    CHECKFILES='checkfiles'
    VALIDATE='validate'

class EvidenceStringStorage():

    @staticmethod
    def delete_prev_data_in_pg(session, data_source_name):
        rows_deleted = session.query(
            EvidenceString10).filter(
                EvidenceString10.data_source_name == data_source_name).delete(synchronize_session=False)
        if rows_deleted:
            logging.info('deleted %i rows from evidence_string' % rows_deleted)

    @staticmethod
    def store_to_pg(session,
                    data_source_name,
                    data,
                    delete_prev=True,
                    autocommit=True,
                    quiet = False):
        if delete_prev:
            EvidenceStringStorage.delete_prev_data_in_pg(session, data_source_name)
        c = 0
        for key, value in data.iteritems():
            c += 1

            session.add(value)
            if c % 1000 == 0:
                logging.debug("%i rows of %s inserted to evidence_string" %(c, data_source_name))
                session.flush()
        session.flush()
        if autocommit:
            session.commit()
        if not quiet:
            logging.info('inserted %i rows of %s inserted in evidence_string' %(c, data_source_name))
        return c

    @staticmethod
    def store_to_pg_core(adapter,
                    data_source_name,
                    data,
                    delete_prev=True,
                    autocommit=True,
                    quiet = False):
        if delete_prev:
            EvidenceStringStorage.delete_prev_data_in_pg(adapter.session, data_source_name)
        rows_to_insert =[]
        for key, value in data.iteritems():
            rows_to_insert.append(dict(uniq_assoc_fields_hashdig = key,
                                        json_doc_hashdig = value.json_doc_hashdig,
                                        evidence_string = value.evidence_string,
                                        data_source_name = value.data_source_name,
                                        json_schema_version = value.json_schema_version,
                                        json_doc_version = value.json_doc_version,
                                        release_date = value.release_date
                                      ))
        adapter.engine.execute(EvidenceString10.__table__.insert(),rows_to_insert)

        # if autocommit:
        #     adapter.session.commit()
        if not quiet:
            logging.info('inserted %i rows of %s inserted in evidence_string' %(len(rows_to_insert), data_source_name))
        return len(rows_to_insert)

class EvidenceValidationFileChecker():

    def __init__(self, adapter, chunk_size=1e4):
        self.adapter = adapter
        self.session = adapter.session
        self.chunk_size = chunk_size
        self.cache = {}
        self.counter = 0        
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

    def storage_reset(self):
        self.cache = {}
        self.counter = 0
    
    def storage_add(self, id, evidence_string, data_source_name):

        self.cache[id] = evidence_string
        self.counter +=1
        if (len(self.cache) % self.chunk_size) == 0:
            self.storage_flush(data_source_name)

    def storage_delete(self, data_source_name):
        EvidenceStringStorage.delete_prev_data_in_pg(self.adapter.session, data_source_name)
        self.session.commit()
        self.cache = {}
        self.counter = 0
        
    def storage_flush(self, data_source_name):

        if self.cache:
            EvidenceStringStorage.store_to_pg_core(self.adapter,
                                              data_source_name,
                                              self.cache,
                                              delete_prev=False,
                                              quiet=True
                                             )
            self.counter+=len(self.cache)
            # if (self.counter % global_reporting_step) == 0:
            #     logging.info("%s precalculated scores inserted in elasticsearch_load table" %(millify(self.counter)))

            self.session.flush()
            self.cache = {}

    def storage_commit(self):
        self.session.flush()
        self.session.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.storage_commit()

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
        you = ",".join(Config.EVIDENCEVALIDATION_PROVIDER_EMAILS[provider_id])
        status = "passed"
        if not bValidated:
            status = "failed"
        # Create message container - the correct MIME type is multipart/alternative.
        #msg = MIMEMultipart('alternative')
        msg = MIMEMultipart()
        msg['Subject'] = "CTTV: Validation of submitted file {0} {1}".format(filename, status)
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
        text += "JSON schema version:\t1.2.1\n"
        text += "Number of records parsed:\t{0}\n".format(nb_records)
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
                gene_symbol = None;
                ensembl_gene_id = None;
                if "symbol" in doc:
                    gene_symbol = doc["symbol"];
                    if gene_symbol not in symbols:
                        logging.info("Adding missing symbol from Ensembl %s" % gene_symbol)
                        symbols[gene_symbol] = {};
                        
                if "ensembl_gene_id" in doc: 
                    ensembl_gene_id = doc["ensembl_gene_id"];
                        
                if "uniprot_ids" in doc:
                    if "uniprot_ids" not in symbols[gene_symbol]:
                        symbols[gene_symbol]["uniprot_ids"] = [];
                    for uniprot_id in doc["uniprot_ids"]:
                        if uniprot_id in uniprot_current and ensembl_gene_id is not None and ensembl_gene_id in ensembl_current:
                            #print uniprot_id, " ", ensembl_gene_id, "\n"
                            if "gene_ids" not in uniprot_current[uniprot_id]:
                                uniprot_current[uniprot_id]["gene_ids"] = [ensembl_gene_id];
                            else:
                                uniprot_current[uniprot_id]["gene_ids"].append(ensembl_gene_id);

                        if uniprot_id not in symbols[gene_symbol]["uniprot_ids"]:
                            symbols[gene_symbol]["uniprot_ids"].append(uniprot_id)               
                
        logging.info("%i entries parsed for HGNC" % len(row.data["response"]["docs"]))
        
    def load_Uniprot(self):
        logging.info("Loading Uniprot identifiers and mappings to Gene Symbol and Ensembl Gene Id")
        c = 0
        for row in self.session.query(UniprotInfo).yield_per(1000):
            # get the symbol too
            uniprot_accession = row.uniprot_accession
            uniprot_current[uniprot_accession] = {};
            root = ElementTree.fromstring(row.uniprot_entry)
            protein_name = None
            for name in root.findall("./ns0:name", { 'ns0' : 'http://uniprot.org/uniprot'} ):        
                protein_name = name.text
                break;
            gene_symbol = None
            for gene_name_el in root.findall(".//ns0:gene/ns0:name[@type='primary']", { 'ns0' : 'http://uniprot.org/uniprot'} ):        
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
                    
                uniprot_current[uniprot_accession]["gene_symbol"] = gene_symbol;
                
                if gene_symbol not in symbols:
                    symbols[gene_symbol] = {};
                if "uniprot_ids" not in symbols[gene_symbol]:
                    symbols[gene_symbol]["uniprot_ids"] = [ uniprot_accession ]
                elif uniprot_accession not in symbols[gene_symbol]["uniprot_ids"]:
                    symbols[gene_symbol]["uniprot_ids"].append(uniprot_accession)
                
            gene_id = None
            for crossref in root.findall(".//ns0:dbReference[@type='Ensembl']/ns0:property[@type='gene ID']", { 'ns0' : 'http://uniprot.org/uniprot'} ):
                ensembl_gene_id = crossref.get("value")
                if ensembl_gene_id in ensembl_current:
                    #print uniprot_accession, " ", ensembl_gene_id, "\n"
                    if "gene_ids" not in uniprot_current[uniprot_accession]:
                        uniprot_current[uniprot_accession]["gene_ids"] = [ensembl_gene_id];
                    else:
                        uniprot_current[uniprot_accession]["gene_ids"].append(ensembl_gene_id)
                        
            # create a mapping from the symbol instead to link to Ensembl
            if "gene_ids" not in uniprot_current[uniprot_accession]:
                if gene_symbol and gene_symbol in symbols:
                    if "ensembl_primary_id" in symbols[gene_symbol]:
                        uniprot_current[uniprot_accession]["gene_ids"] = [symbols[gene_symbol]["ensembl_primary_id"]];
                    elif "ensembl_secondary_id" in symbols[gene_symbol]:
                        uniprot_current[uniprot_accession]["gene_ids"] = [symbols[gene_symbol]["ensembl_secondary_id"]];

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
            # put the ensembl_id in symbols too
            if row.external_name not in symbols:
                symbols[row.external_name] = {}
            if row.is_reference:
                symbols[row.external_name]["ensembl_primary_id"] = row.ensembl_gene_id
            else:
                if "ensembl_secondary_id" not in symbols[row.external_name] or row.ensembl_gene_id < symbols[row.external_name]["ensembl_secondary_id"]:
                    symbols[row.external_name]["ensembl_secondary_id"] = row.ensembl_gene_id;
                
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
        omim_pattern = "http://purl.bioontology.org/omim/OMIM_%"
        
        '''
        Temp: store the uncharactered diseases to fitler them
        https://alpha.targetvalidation.org/disease/genetic_disorder_uncategorized
        '''
        for row in self.session.query(EFOPath):
            if any(map(lambda x: x["uri"] == 'http://www.targetvalidation.org/genetic_disorder_uncategorized', row.tree_path)):
                efo_uncat.append(row.uri);
        
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
            #if row.uri not in uncat:
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
        #return;
        self.load_efo();
        self.load_eco();
        
        for dirname, dirnames, filenames in os.walk(Config.EVIDENCEVALIDATION_FTP_SUBMISSION_PATH):
            dirnames.sort()
            for subdirname in dirnames:
                cttv_match = re.match("^(cttv[0-9]{3})$", subdirname)
                #cttv_match = re.match("^(cttv012|cttv011)$", subdirname)
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
                            logging.info(filename);
                            cttv_filename_match = re.match(Config.EVIDENCEVALIDATION_FILENAME_REGEX, filename);
                            # cttv_filename_match = re.match("cttv025-03-11-2015.json.gz", filename);
                            if cttv_filename_match:
                                cttv_file = os.path.join(cttv_dirname, filename)
                                logging.info(cttv_file);
                                data_source_name = Config.JSON_FILE_TO_DATASOURCE_MAPPING[cttv_filename_match.groups()[0]]
                                logging.info(data_source_name);
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
                                    self.validate_gzipfile(cttv_file, filename, provider_id, data_source_name, md5_hash, logfile = logfile)

        self.session.commit()
        
    def validate_gzipfile(self, file_on_disk, filename, provider_id, data_source_name, md5_hash, logfile = None):
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
        missing_uniprot_id_xrefs = {}
        invalid_uniprot_id_mappings = {}
        
        if bValidate == True:

            logging.info('Delete previous data for %s'% (data_source_name))
            self.storage_delete(data_source_name);
            
            logging.info('Starting validation of %s'% (file_on_disk))
            
            fh = gzip.GzipFile(file_on_disk, "r")
            #lfh = gzip.open(logfile, 'wb', compresslevel=5)
            lfh = open(logfile, 'wb')
            cc = 0
            lc = 0
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
                uniq_elements_flat_hexdig = None
                
                if ('label' in python_raw  or 'type' in python_raw) and 'validated_against_schema_version' in python_raw and python_raw['validated_against_schema_version'] == "1.2.1":
                    if 'label' in python_raw:
                        python_raw['type'] = python_raw.pop('label', None)
                    data_type = python_raw['type']
                    #logging.info('type %s'%data_type)
                    if data_type in ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']:
                        try:
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
                        except:
                            obj = None

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
                                self.startCapture(logging.WARNING)
                            
                            # flatten 
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
                                index = 0
                                for disease_id in obj.disease.id:
                                    # fix for EVA data
                                    if disease_id in eva_curated:
                                        obj.disease.id[index] = eva_curated[disease_id];
                                        disease_id = obj.disease.id[index];
                                    index +=1
                                    if disease_id not in efo_current or disease_id in efo_uncat:
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
                                            logger.warning("Line {0}: Human Alternative sequence Ensembl Gene detected {1}. We will attempt to map it to a gene identifier on the reference genome assembly {2} or choose a Human Alternative sequence Ensembl Gene Id".format(lc+1, ensembl_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
                                            if not ensembl_id in invalid_ensembl_ids:
                                                nonref_ensembl_ids[ensembl_id] = 1
                                            else:
                                                nonref_ensembl_ids[ensembl_id] += 1
                                            nb_ensembl_nonref +=1                                            
                                    elif uniprotMatch:
                                        uniprot_id = uniprotMatch.groups()[0].rstrip("\s")
                                        if uniprot_id not in uniprot_current:
                                            gene_failed = True
                                            logger.error("Line {0}: Invalid UniProt entry detected {1}. Please provide a correct identifier".format(lc+1, uniprot_id))
                                            if uniprot_id not in invalid_uniprot_ids:
                                                invalid_uniprot_ids[uniprot_id] = 1
                                            else:
                                                invalid_uniprot_ids[uniprot_id] += 1
                                            nb_uniprot_invalid +=1
                                        elif "gene_ids" not in uniprot_current[uniprot_id]:
                                            # check symbol mapping (get symbol first)
                                            gene_mapping_failed = True
                                            logger.warning("Line {0}: UniProt entry {1} does not have any cross-reference to Ensembl.".format(lc+1, uniprot_id))
                                            if not uniprot_id in missing_uniprot_id_xrefs:
                                                missing_uniprot_id_xrefs[uniprot_id] = 1
                                            else:
                                                missing_uniprot_id_xrefs[uniprot_id] += 1
                                            nb_missing_uniprot_id_xrefs +=1
                                            #This identifier is not in the current EnsEMBL database
                                        elif not reduce( (lambda x, y: x or y), map(lambda x: ensembl_current[x].is_reference, uniprot_current[uniprot_id]["gene_ids"]) ):
                                            gene_mapping_failed = True
                                            logger.warning("Line {0}: The UniProt entry {1} does not have a cross-reference to an Ensembl Gene Id on the reference genome assembly {2}. It will be mapped to a Human Alternative sequence Ensembl Gene Id.".format(lc+1, uniprot_id, Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))
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
                if not validation_failed and validation_result == 0 and not disease_failed and not gene_failed:
                    nb_valid +=1;
                    # flatten data structure
                    json_doc_hashdig = flat.DatatStructureFlattener(python_raw).get_hexdigest();
                    self.storage_add(uniq_elements_flat_hexdig, 
                                    EvidenceString10(uniq_assoc_fields_hashdig = uniq_elements_flat_hexdig,
                                                    json_doc_hashdig = json_doc_hashdig,
                                                    evidence_string = python_raw,
                                                    data_source_name = data_source_name,
                                                    json_schema_version = "1.2.1", 
                                                    json_doc_version = 1,
                                                    release_date = datetime.utcnow()), 
                                    data_source_name);
                    
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
                        id_text = self.get_reference_gene_from_list(uniprot_current[uniprot_id]["gene_ids"]);
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

            # report UniProt ids with no mapping to Ensembl
            # missing_uniprot_id_xrefs
            if nb_missing_uniprot_id_xrefs > 0:            
                text +="Warnings:\n"
                text +="\t%i UniProt identifier(s) without cross-references to Ensembl found in %i (%.1f%s) of the records.\n"%(len(missing_uniprot_id_xrefs), nb_missing_uniprot_id_xrefs, nb_missing_uniprot_id_xrefs*100/lc, '%' )
                text +="\tThe corresponding evidence strings have been discarded.\n"
                for uniprot_id in missing_uniprot_id_xrefs:
                    if missing_uniprot_id_xrefs[uniprot_id] == 1:
                        text += "\t%s\t(reported once)\n"%(uniprot_id)
                    else:
                        text += "\t%s\t(reported %i times)\n"%(uniprot_id, missing_uniprot_id_xrefs[uniprot_id])
                text +="\n"            
            # report invalid Uniprot mapping entries
            if nb_uniprot_invalid_mapping > 0:
                text +="Warnings:\n"
                text +="\t%i UniProt identifier(s) not mapped to Ensembl reference genome assembly %s gene identifiers found in %i (%.1f%s) of the records.\n"%(len(invalid_uniprot_id_mappings), Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY, nb_uniprot_invalid_mapping, nb_uniprot_invalid_mapping*100/lc, '%' )
                text +="\tIf you think that might be an error in your submission, please use a UniProt identifier that will map to a reference assembly gene identifier.\n"
                text +="\tOtherwise we will map them automatically to a reference genome assembly gene identifier or one of the alternative gene identifiers.\n"
                for uniprot_id in invalid_uniprot_id_mappings:
                    if invalid_uniprot_id_mappings[uniprot_id] == 1:
                        text += "\t%s\t(reported once) maps to %s\n"%(uniprot_id, self.get_reference_gene_from_list(uniprot_current[uniprot_id]["gene_ids"]))
                    else:
                        text += "\t%s\t(reported %i times) maps to %s\n"%(uniprot_id, invalid_uniprot_id_mappings[uniprot_id], self.get_reference_gene_from_list(uniprot_current[uniprot_id]["gene_ids"]))
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
                    #nb_valid = nb_valid,
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

            # write
            self.storage_flush(data_source_name);
            self.storage_commit();
            
            self.send_email(
                Config.EVIDENCEVALIDATION_SEND_EMAIL, 
                provider_id, 
                filename, 
                successfully_validated, 
                lc, 
                {   'valid records': nb_valid,
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

                