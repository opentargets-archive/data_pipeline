import requests
import httplib
import time
import optparse
from tqdm import tqdm
from mrtarget.common import TqdmToLogger
import logging
import os
import json
import re
import hashlib
import datetime
from mrtarget.common import Actions
from mrtarget.Settings import Config
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.modules.Ontology import OntologyClassReader
from EvidenceValidation import EvidenceValidationFileChecker
import elasticsearch
from elasticsearch import Elasticsearch, helpers
from SPARQLWrapper import SPARQLWrapper, JSON
import opentargets.model.core as cttv
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.phenotype as evidence_phenotype
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.association_score as association_score


__copyright__ = "Copyright 2014-2017, Open Targets"
__credits__ = ["Gautier Koscielny", "Damian Smedley"]
__license__ = "Apache 2.0"
__version__ = "1.2.6"
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

class MouseModelsActions(Actions):
    UPDATE_CACHE = 'updatecache'
    UPDATE_GENES = 'updategenes'
    GENERATE_EVIDENCE = 'generateevidence'

class Phenodigm():

    def __init__(self,es, r_server):
        self.es = es
        self.r_server = r_server
        self.efo = OntologyClassReader()
        self.hpo = OntologyClassReader()
        self.mp = OntologyClassReader()
        self.cache = {}
        self.counter = 0
        self.mmGenes = {}
        self.hsGenes = {}
        self.omim_to_efo_map = dict()
        self.hgnc2mgis = {}
        self.symbol2hgncids = {}
        self.mgi2mouse_models = {}
        self.mouse_model2diseases = {}
        self.disease_gene_locus = {}
        self.mouse_models = {}
        self.diseases = {}
        self.hashkeys = {}
        self._logger = logging.getLogger(__name__)
        tqdm_out = TqdmToLogger(self._logger,level=logging.INFO)

    def get_omim_to_efo_mappings(self):
        self._logger.info("OMIM to EFO parsing - requesting from URL %s" % Config.OMIM_TO_EFO_MAP_URL)
        response = requests.get(Config.OMIM_TO_EFO_MAP_URL)
        self._logger.info("OMIM to EFO parsing - response code %s" % response.code)
        lines = response.readlines()

        for line in lines:
            '''
            omim	efo_uri	efo_label	source	status

            '''
            (omim, efo_uri, efo_label, source, status) = line.split("\t")
            if omim not in self.omim_to_efo_map:
                self.omim_to_efo_map[omim] = []
            self.omim_to_efo_map[omim].append({'efo_uri': efo_uri, 'efo_label': efo_label})

    def list_files(self, path):
        '''
        returns a list of names (with extension, without full path) of all files
        in folder path
        '''
        files = []
        for name in os.listdir(path):
            if os.path.isfile(os.path.join(path, name)) and re.match("part[0-9]+", name):
                files.append(os.path.join(path, name))
        return files

    def load_mouse_genes(self, path):

        with open(os.path.join(path, "mmGenes.json"), "r") as mmGenesFile:
            content = mmGenesFile.read()
            self.mmGenes = json.loads(content)
        mmGenesFile.close()

        self._logger.info("Loaded {0} mm genes".format(len(self.mmGenes)))

    def load_human_genes(self, path):

        with open(os.path.join(path, "hsGenes.json"), "r") as hsGenesFile:
            content = hsGenesFile.read()
            self.hsGenes = json.loads(content)
        hsGenesFile.close()

        self._logger.info("Loaded {0} hs genes".format(len(self.mmGenes)))

    def update_cache(self):
        hdr = { 'User-Agent' : 'open targets bot @ targetvalidation.org' }
        conn = httplib.HTTPConnection(Config.MOUSEMODELS_PHENODIGM_SOLR)

        for dir in [Config.MOUSEMODELS_CACHE_DIRECTORY]:
            if not os.path.exists(dir):
                os.makedirs(dir)

        start = 0
        rows = 10000
        nbItems = rows
        counter = 0

        while (nbItems == rows):
            counter+=1
            #print start
            uri = '/solr/phenodigm/select?q=*:*&wt=python&indent=true&start=%i&rows=%i' %(start,rows)
            self._logger.info("REQUEST {0}. {1}".format(counter, uri))
            conn.request('GET', uri, headers=hdr)
            raw = conn.getresponse().read()
            rsp = eval (raw)
            nbItems = len(rsp['response']['docs'])
            phenodigmFile = open(Config.MOUSEMODELS_CACHE_DIRECTORY + "/part{0}".format(counter), "w")
            phenodigmFile.write(raw)
            phenodigmFile.close()
            start+=nbItems

        conn.close()

    def update_genes(self):

        self._logger.info("Get all human and mouse genes")

        conn = httplib.HTTPConnection('rest.ensembl.org')
        mm_buffer = []
        hs_buffer = []

        #parts = self.list_files(Config.MOUSEMODELS_CACHE_DIRECTORY)
        #parts.sort()
        parts = list()
        parts.append(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "all_types.json"))
        #parts.append(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "disease_model_association.json"))
        tick = 0
        for part in parts:
            self._logger.info("processing {0}\n".format(part))
            with open(part, "r") as myfile:
                print "Load array from file %s"%part
                rsp = json.loads(myfile.read())
                for doc in rsp:
                    if doc['type'] == 'gene' and 'hgnc_gene_id' in doc:
                        hgnc_gene_symbol = doc['hgnc_gene_symbol']
                        if hgnc_gene_symbol and not hgnc_gene_symbol in self.hsGenes and hgnc_gene_symbol not in hs_buffer:
                            hs_buffer.append(hgnc_gene_symbol)
                            if len(hs_buffer) == 100:
                                self.request_human_genes(conn, hs_buffer)
                                hs_buffer = []
                        marker_symbol = doc['marker_symbol']
                        if marker_symbol and marker_symbol not in self.mmGenes and marker_symbol not in mm_buffer:
                            mm_buffer.append(marker_symbol)
                            if len(mm_buffer) == 100:
                                self.request_mouse_genes(conn, mm_buffer)
                                mm_buffer = []

                if len(hs_buffer)>0:
                    self.request_human_genes(conn, hs_buffer)
                    hs_buffer = []

                if len(mm_buffer)>0:
                    self.request_mouse_genes(conn, mm_buffer)
                    mm_buffer = []

            myfile.close()
            conn.close()

        self._logger.info("writing {0}".format(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "mmGenes.json")))
        with open(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "mmGenes.json"), "w") as mmGenesFile:
            json.dump(self.mmGenes, mmGenesFile, sort_keys=True, indent=2)
            mmGenesFile.flush()
            mmGenesFile.close()
        self._logger.info("writing {0}".format(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "hsGenes.json")))
        with open(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "hsGenes.json"), "w") as hsGenesFile:
            json.dump(self.hsGenes, hsGenesFile, sort_keys=True, indent=2)
            hsGenesFile.flush()
            hsGenesFile.close()

    def request_human_genes(self, conn, buffer):
        hdr = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": "open_targets bot by /open/targets"}
        self._logger.info('hs Request "%s"...' % '","'.join(buffer))
        body = '{ "symbols": ["%s"] }' % '","'.join(buffer)
        conn.request('POST', '/lookup/symbol/homo_sapiens/', headers=hdr, body=body)
        ensemblMap = json.loads(conn.getresponse().read())
        # self._logger.info( json.dumps(ensemblMap, indent=4) )
        for hs_symbol, value in ensemblMap.items():
            if value["object_type"] == "Gene":
                self.hsGenes[hs_symbol] = value['id']
            else:
                self.hsGenes[hs_symbol] = None
            ensemblId = self.hsGenes[hs_symbol]
            self._logger.info("hs {0} {1}".format(hs_symbol, ensemblId))
        self._logger.info(json.dumps(self.hsGenes, indent=2))

    def request_mouse_genes(self, conn, buffer):
        hdr = { "Content-Type" : "application/json", "Accept" : "application/json", "User-Agent" : "cttv bot by /center/for/therapeutic/target/validation" }
        self._logger.info('mm Request "%s"...'%'","'.join(buffer))
        body = '{ "symbols": ["%s"] }'%'","'.join(buffer)
    #conn.request('GET', '/xrefs/symbol/mus_musculus/%s?content-type=application/json;external_db=MGI' %(marker_symbol), headers=hdr)
        conn.request('POST', '/lookup/symbol/mus_musculus/', headers=hdr, body= body)
        ensemblMap = json.loads(conn.getresponse().read())
        #self._logger.info( json.dumps(ensemblMap, indent=4) )
        for marker_symbol, value in ensemblMap.iteritems():
            if value["object_type"] == "Gene":
                self.mmGenes[marker_symbol] = value['id']
            else:
                self.mmGenes[marker_symbol] = None
            ensemblId = self.mmGenes[marker_symbol]
            self._logger.info("mm {0} {1}".format(marker_symbol, ensemblId))

    def parse_phenodigm_files(self):
        #parts = self.list_files(Config.MOUSEMODELS_CACHE_DIRECTORY)
        #parts.sort()
        parts = list()
        parts.append(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "all_types.json"))
        parts.append(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "disease_model_association.json"))
        for part in parts:
            self._logger.info("Processing PhenoDigm chunk {0}".format(part))
            with open (part, "r") as myfile:
                rsp = json.loads(myfile.read())
                for doc in rsp:
                    if doc['type'] == 'gene' and 'hgnc_gene_id' in doc:
                        hgnc_gene_id = doc['hgnc_gene_id']
                        hgnc_gene_symbol = doc['hgnc_gene_symbol']
                        self.symbol2hgncids[hgnc_gene_symbol] = hgnc_gene_id
                        marker_symbol = doc['marker_symbol']
                        if hgnc_gene_id and not hgnc_gene_id in self.hgnc2mgis:
                            self.hgnc2mgis[hgnc_gene_id] = []
                        self.hgnc2mgis[hgnc_gene_id].append(marker_symbol)
                    elif doc['type'] == 'mouse_model':
                        marker_symbol = doc['marker_symbol']
                        model_id = doc['model_id']
                        if not marker_symbol in self.mgi2mouse_models:
                            self.mgi2mouse_models[marker_symbol] = []
                        self.mgi2mouse_models[marker_symbol].append(model_id)
                        if not model_id in self.mouse_models:
                            self.mouse_models[model_id] = doc
                    elif doc['type'] == 'disease_model_association':
                        model_id = doc['model_id']
                        if not model_id in self.mouse_model2diseases:
                            self.mouse_model2diseases[model_id] = []
                        self.mouse_model2diseases[model_id].append(doc)
                    elif doc['type'] == 'disease_gene_summary' and doc['in_locus'] is True and 'disease_id' in doc:
                        hgnc_gene_id = doc['hgnc_gene_id']
                        hgnc_gene_symbol = doc['hgnc_gene_symbol']
                        marker_symbol = doc['marker_symbol']
                        try:
                            disease_id = doc['disease_id']
                            #if 'disease_id' in doc:
                            #    raise KeyError()
                        except Exception, error:

                            if isinstance(error, KeyError):
                                self._logger.error("Error checking disease in document: %s" % (str(error)))
                                self._logger.error(json.dumps(doc, indent=4))
                                raise Exception()
                        if not disease_id in self.disease_gene_locus:
                            self.disease_gene_locus[disease_id] = { hgnc_gene_id: [ marker_symbol ] }
                        elif not hgnc_gene_id in self.disease_gene_locus[disease_id]:
                            self.disease_gene_locus[disease_id][hgnc_gene_id] = [ marker_symbol ]
                        else:
                            self.disease_gene_locus[disease_id][hgnc_gene_id].append(marker_symbol)
                    elif doc['type'] == 'disease' or (doc['type'] == 'disease_gene_summary' and 'disease_id' in doc and not doc['disease_id'] in self.diseases):
                        '''and doc['disease_id'].startswith('ORPHANET')):'''
                        disease_id = doc['disease_id']
                        self.diseases[disease_id] = doc
                        #matchOMIM = re.match("^OMIM:(.+)$", disease_id)
                        #if matchOMIM:
                        #    terms = efo.getTermsByDbXref(disease_id)
                        #    if terms == None:
                        #        terms = OMIMmap[disease_id]
                        #    if terms == None:
                        #        self._logger.error("{0} '{1}' not in EFO".format(disease_id, doc['disease_term']))
                    #else:
                    #    self._logger.error("Never heard of this '%s' type of documents in PhenoDigm. Exiting..."%(doc['type']))
                    #    sys.exit(1)
            myfile.close()

    def generate_phenodigm_evidence_strings(self, upper_limit=0):
        '''
         Once you have retrieved all the genes,and mouse models   
         Create an evidence string for every gene to disease relationship
        '''
        now = datetime.datetime.now()
        efoMapping = {}
        index_g = 0
        for hs_symbol in self.hsGenes:
            index_g +=1
            hgnc_gene_id = self.symbol2hgncids[hs_symbol]
            hs_ensembl_gene_id = self.hsGenes[hs_symbol]

            '''
            Useful if you want to test on a subset of the data
            '''
            if upper_limit > 0 and len(self.hashkeys) > upper_limit:
                break

            if hgnc_gene_id and hs_ensembl_gene_id and re.match('^ENSG.*', hs_ensembl_gene_id) and hgnc_gene_id in self.hgnc2mgis:

                self._logger.info("processing human gene {0} {1} {2} {3}".format(index_g, hs_symbol, hs_ensembl_gene_id, hgnc_gene_id ))

                '''
                 Retrieve mouse models
                '''
                for marker_symbol in self.hgnc2mgis[hgnc_gene_id]:

                    #if not marker_symbol == "Il13":
                    #    continue;
                    print marker_symbol;
                    self._logger.info("\tProcessing mouse gene symbol %s" % (marker_symbol))

                    '''
                    Some mouse symbol are not mapped in Ensembl
                    '''
                    if marker_symbol in self.mmGenes:
                        mmEnsemblId = self.mmGenes[marker_symbol]
                        '''
                         Some genes don't have any mouse models...
                        '''
                        if marker_symbol in self.mgi2mouse_models:

                            self._logger.info("\tFound %i mouse models for this marker" % (len(self.mgi2mouse_models[marker_symbol])))

                            '''
                             if the marker is associated to mouse models
                            '''
                            for model_id in self.mgi2mouse_models[marker_symbol]:
                                '''
                                 loop through every model
                                '''
                                mouse_model = self.mouse_models[model_id]
                                marker_accession = mouse_model['marker_accession']
                                allelic_composition = mouse_model['allelic_composition']
                                self._logger.info("\t\tMouse model {0} for {1}".format(model_id, marker_accession))
                                '''
                                 Check the model_id is in the dictionary containing all the models 
                                '''
                                if model_id in self.mouse_model2diseases:

                                    self._logger.info("\t\tMouse model {0} is in the dictionary containing all the models".format(model_id))

                                    for mouse_model2disease in self.mouse_model2diseases[model_id]:
                                        '''
                                         get the disease identifier
                                        '''
                                        disease_id = mouse_model2disease['disease_id']
                                        '''
                                         Check if there are any HPO terms
                                        '''
                                        hp_matched_terms = None
                                        if 'hp_matched_terms' in mouse_model2disease:
                                            hp_matched_terms = mouse_model2disease['hp_matched_terms']
                                        '''
                                         Check if there are any MP terms
                                         There is a bug in the current PhenoDigm.
                                        
                                        '''
                                        mp_matched_ids = None
                                        if 'mp_matched_ids' in mouse_model2disease:
                                            mp_matched_ids = mouse_model2disease['mp_matched_ids']
                                        '''
                                         Retrieve the disease document
                                        '''
                                        disease = None
                                        if disease_id in self.diseases:
                                            disease = self.diseases[disease_id]
                                            self._logger.info("\t\t\tdisease: %s %s"%(disease_id, disease['disease_term']))
                                        else:
                                            self._logger.info("\t\t\tdisease: %s"%(disease_id))

                                        '''
                                        Map the disease ID to EFO
                                        Can be a one to many mapping
                                        '''
                                        disease_terms = list()

                                        diseaseName = None
                                        if not disease_id in efoMapping:
                                            # find corresponding EFO disease term
                                            matchOMIM = re.match("^OMIM:(.+)$", disease_id)
                                            matchORPHANET = re.match("^ORPHANET:(.+)$", disease_id)
                                            if matchOMIM:
                                                (source, omim_id) = disease_id.split(':')
                                                self._logger.info("\t\t\tCheck OMIM id = %s is in efo"%omim_id)
                                                if omim_id in self.omim_to_efo_map:
                                                    efoMapping[disease_id] = self.omim_to_efo_map[omim_id]
                                                if len(disease_terms) > 0:
                                                    #for disease_term in disease_terms:
                                                    #    if disease_term['efo_uri'] in self.efo.current_classes:
                                                    #        self._logger.info("{0} => {1} {2}".format(disease_id, disease_term['efo_uri'], self.efo.current_classes[disease_term['efo_uri']]))
                                                    #    else:
                                                    #        self._logger.info("{0} => {1} (no EFO mapping)".format(disease_id, disease_term['efo_uri']))
                                                    disease_terms = efoMapping[disease_id]

                                            elif matchORPHANET:
                                                    suffix = matchORPHANET.groups()[0]
                                                    orphanetId = "Orphanet:{0}".format(suffix)
                                                    orphanet_uri = "http://www.orpha.net/ORDO/Orphanet_{0}".format(suffix)
                                                    if orphanet_uri in self.efo.current_classes:
                                                        efoMapping[disease_id] = [ {'efo_uri': orphanet_uri, 'efo_label': self.diseases[disease_id]['disease_term'] } ]
                                                        disease_terms = efoMapping[disease_id]
                                        else:
                                            disease_terms = efoMapping[disease_id]

                                        '''
                                        OK, we have a disease mapped to EFO
                                        we can proceed to the next stage
                                        we don't filter on the score anymore.
                                        this will be adjusted in Open Targets
                                        If the score >= 0.5 or in_locus for the same disease
                                        and mouse_model2disease['model_to_disease_score'] >= 50
                                        '''
                                        if disease_terms is not None and (('model_to_disease_score' in mouse_model2disease ) or
                                            (disease_id in self.disease_gene_locus and hgnc_gene_id in self.disease_gene_locus[disease_id] and marker_symbol in self.disease_gene_locus[disease_id][hgnc_gene_id])):

                                            for disease_term in disease_terms:

                                                '''
                                                Create a new evidence string
                                                '''



                                                # 1.2.6 create an Animal_Models class
                                                evidenceString = cttv.Animal_Models()
                                                evidenceString.validated_against_schema_version = '1.2.6'
                                                evidenceString.access_level = "public"
                                                evidenceString.type = "animal_model"
                                                evidenceString.sourceID = "phenodigm"
                                                evidenceString.unique_association_fields = {}
                                                evidenceString.unique_association_fields['projectName'] = 'otar_external_mousemodels'
                                                evidenceString.evidence = cttv.Animal_ModelsEvidence()
                                                evidenceString.evidence.date_asserted = now.isoformat()
                                                evidenceString.evidence.is_associated = True

                                                '''
                                                Target
                                                '''
                                                evidenceString.target = bioentity.Target(
                                                    id="http://identifiers.org/ensembl/{0}".format(hs_ensembl_gene_id),
                                                    activity="http://identifiers.org/cttv.activity/predicted_damaging",
                                                    target_type="http://identifiers.org/cttv.target/gene_evidence"
                                                    )

                                                '''
                                                Disease
                                                '''
                                                name = 'N/A'
                                                if disease_term['efo_uri'] in self.efo.current_classes:
                                                    name = self.efo.current_classes[disease_term['efo_uri']]
                                                self._logger.info("Disease id is %s"%(disease_term['efo_uri']))
                                                evidenceString.disease = bioentity.Disease(
                                                    id=disease_term['efo_uri'],
                                                    name=name,
                                                    source_name=self.diseases[disease_id]['disease_term']
                                                    )

                                                '''
                                                Evidence Codes
                                                '''
                                                if 'lit_model' in mouse_model2disease and mouse_model2disease['lit_model'] == True:
                                                    #evidenceString.evidence.evidence_codes.append("http://identifiers.org/eco/ECO:0000057")

                                                    evidenceString.unique_association_fields['predictionModel'] = 'mgi_predicted'
                                                else:
                                                    #evidenceString.evidence.evidence_codes.append("http://identifiers.org/eco/ECO:0000057")
                                                    evidenceString.unique_association_fields['predictionModel'] = 'impc_predicted'

                                                evidenceString.unique_association_fields['disease'] = disease_term
                                                evidenceString.unique_association_fields['targetId'] = "http://identifiers.org/ensembl/{0}".format(hs_ensembl_gene_id)

                                                '''
                                                Orthologs
                                                Human gene => Mouse marker
                                                '''
                                                evidenceString.evidence.orthologs = evidence_phenotype.Orthologs(
                                                    evidence_codes = ["http://identifiers.org/eco/ECO:0000265"],
                                                    provenance_type= evidence_core.BaseProvenance_Type(database=evidence_core.BaseDatabase(id="MGI", version="2016")),
                                                    resource_score= association_score.Pvalue(type="pvalue", method= association_score.Method(description ="orthology from MGI"), value=0.0),
                                                    date_asserted= now.isoformat(),
                                                    human_gene_id = "http://identifiers.org/ensembl/{0}".format(hs_ensembl_gene_id),
                                                    model_gene_id = "http://identifiers.org/ensembl/{0}".format(mmEnsemblId),
                                                    species = "mouse"
                                                    )

                                                '''
                                                Biological Model
                                                'allele_ids':'MGI:1862010|MGI:1862010',
                                                'allelic_composition':'Pmp22<Tr-1H>/Pmp22<+>',
                                                'genetic_background':'involves: BALB/cAnNCrl * C3H/HeN',
                                                '''

                                                evidenceString.evidence.biological_model = evidence_phenotype.Biological_Model(
                                                    evidence_codes = ["http://identifiers.org/eco/ECO:0000179"],
                                                    resource_score= association_score.Pvalue(type="pvalue", method= association_score.Method(description =""), value=0),
                                                    date_asserted= now.isoformat(),
                                                    model_id = "{0}".format(mouse_model['model_id']),
                                                    zygosity = mouse_model['hom_het'],
                                                    genetic_background = mouse_model['genetic_background'],
                                                    allelic_composition = mouse_model['allelic_composition'],
                                                    model_gene_id = "http://identifiers.org/ensembl/{0}".format(mmEnsemblId),
                                                    species = "mouse"
                                                    )
                                                if mouse_model2disease['lit_model'] == True:
                                                    evidenceString.evidence.biological_model.provenance_type= evidence_core.BaseProvenance_Type(database=evidence_core.BaseDatabase(id="MGI", version="2015"))
                                                else:
                                                    evidenceString.evidence.biological_model.provenance_type= evidence_core.BaseProvenance_Type(database=evidence_core.BaseDatabase(id="IMPC", version="2015"))
                                                if 'allele_ids' in mouse_model:
                                                    evidenceString.evidence.biological_model.allele_ids = mouse_model['allele_ids']
                                                else:
                                                    evidenceString.evidence.biological_model.allele_ids = ""
                                                ''' add all mouse phenotypes '''
                                                evidenceString.evidence.biological_model.phenotypes = []
                                                mpheno = {}
                                                for raw_mp in mouse_model['phenotypes']:
                                                    a = raw_mp.split("_")
                                                    #self._logger.info("Add MP term to mouse model {0} {1}".format(a[0], a[1]))
                                                    evidenceString.evidence.biological_model.phenotypes.append(
                                                        bioentity.Phenotype(
                                                            id = a[0],
                                                            term_id = "http://purl.obolibrary.org/obo/" + a[0].replace(":", "_"),
                                                            label = a[1]
                                                            )
                                                        )
                                                    mpheno[a[0]] = a[1]

                                                ''' get all human phenotypes '''
                                                human_phenotypes = []
                                                if hp_matched_terms:
                                                    for hp in hp_matched_terms:
                                                        term_id = "http://purl.obolibrary.org/obo/" + hp.replace(":", "_")
                                                        if term_id in self.hpo.current_classes:
                                                            term_name = self.hpo.current_classes[term_id]
                                                        else:
                                                            term_name = 'NOT FOUND'
                                                        #self._logger.info("HPO term is {0} {1}".format(hp, hp in hpo.terms))
                                                        #self._logger.info("HPO term retrieved is {0} {1}".format( termId, termName ))
                                                        human_phenotypes.append(
                                                            bioentity.Phenotype(
                                                                id = hp,
                                                                term_id = term_id,
                                                                label = term_name
                                                                )
                                                            )
                                                ''' 
                                                get all matched mouse phenotypes
                                                Format is: 
                                                '''
                                                mouse_phenotypes = []
                                                if mp_matched_ids:
                                                    for mp in mp_matched_ids:
                                                        term_id = "http://purl.obolibrary.org/obo/" + mp.replace(":", "_")
                                                        term_name = None
                                                        if term_id in self.mp.current_classes:
                                                            term_name = self.mp.current_classes[term_id]
                                                        elif term_id in self.mp.obsolete_classes:
                                                            new_id = self.mp.get_new_from_obsolete_uri(term_id)
                                                            if new_id and new_id in self.mp.current_classes:
                                                                term_name = self.mp.current_classes[new_id]
                                                            else:
                                                                term_name = self.mp.obsoletes[term_id]['label']

                                                        #termId = mp
                                                        #termName = "TO BE DERTERMINED"
                                                        #self._logger.info("MP term is {0}".format(mp))
                                                        #term = mpo.getTermById(mp)
                                                        #termId = term['tags']['id'][0]
                                                        #termName = term['tags']['name'][0]
                                                        mouse_phenotypes.append(
                                                            bioentity.Phenotype(
                                                                id = mp,
                                                                term_id = term_id,
                                                                label = term_name
                                                                )
                                                            )

                                                '''
                                                Disease model association
                                                '''
                                                score = 1
                                                method = 'Unknown method'
                                                if 'model_to_disease_score' in mouse_model2disease:
                                                    score = (mouse_model2disease['model_to_disease_score'])/100
                                                    method = 'phenodigm_model_to_disease_score'
                                                self._logger.info("score: {0}\n".format(score))
                                                evidenceString.unique_association_fields['score'] = score

                                                evidenceString.evidence.disease_model_association = evidence_phenotype.Disease_Model_Association(
                                                    disease_id = disease_term['efo_uri'],
                                                    model_id = "{0}".format(mouse_model['model_id']),
                                                    provenance_type= evidence_core.BaseProvenance_Type(database=evidence_core.BaseDatabase(id="PhenoDigm", version="June 2016")),
                                                    evidence_codes = ["http://identifiers.org/eco/ECO:0000057"],
                                                    resource_score= association_score.Summed_Total(type = "summed_total", method = association_score.Method(description = method), value = score),
                                                    date_asserted= now.isoformat(),
                                                    model_phenotypes = mouse_phenotypes,
                                                    human_phenotypes = human_phenotypes
                                                )

                                                '''
                                                Make sure we don't create duplicates
                                                If the model is associated already to a disease
                                                take the best score
                                                '''
                                                hashkey = hashlib.md5(json.dumps(evidenceString.unique_association_fields)).hexdigest()
                                                if not hashkey in self.hashkeys:
                                                    self.hashkeys[hashkey] = evidenceString
                                                else:
                                                    self._logger.warn("Doc {0} - Duplicated mouse model {1} to disease {2} URI: {3}".format(mouse_model2disease['id'],model_id, disease_id, disease_term))
                                                    if self.hashkeys[hashkey].unique_association_fields['score'] > evidenceString.unique_association_fields['score']:
                                                        self.hashkeys[hashkey] = evidenceString
                                        else:

                                            self._logger.error("Unable to incorpate this strain for this disease: {0}".format(disease_id))
                                            # self._logger.error("No disease id {0}".format(disease_term_uris == None))
                                            self._logger.error("model_to_disease_score in mouse_model2disease: {0}".format( 'model_to_disease_score' in mouse_model2disease) )
                                            self._logger.error("disease_id in disease_gene_locus: {0}".format(disease_id in self.disease_gene_locus))
                                            # self._logger.error("hs_symbol in disease_gene_locus[disease_id]: {0}".format(not disease_term_uris == None and disease_id in self.disease_gene_locus and hgnc_gene_id in self.disease_gene_locus[disease_id]))
                                            # self._logger.error("marker_symbol in disease_gene_locus[disease_id][hgnc_gene_id]): {0}".format(disease_term_uris is not None and disease_id in self.disease_gene_locus and marker_symbol in self.disease_gene_locus[disease_id][hgnc_gene_id]))

    def write_phenodigm_evidence_strings(self, path):
        cttvFile = open(os.path.join(path, "phenodigm.json"), "wb")
        #cttvFile.write("[\n")
        countExported = 0
        self._logger.info("Processing %i records" % (len(self.hashkeys)))
        for hashkey in self.hashkeys:
            self._logger.info("Processing key %s"%(hashkey))
            evidenceString = self.hashkeys[hashkey]

            error = evidenceString.validate(self._logger)

            if error == 0:
    #        and (evidenceString.evidence.association_score.probability.value >= 0.5 || evidenceString.evidence.in_locus):
                #print(evidenceString.to_JSON())
                if countExported > 0:
                    cttvFile.write("\n")
                cttvFile.write(evidenceString.to_JSON(indentation=None))
                #cttvFile.write(evidenceString.to_JSON(indentation=2))
                countExported+=1

        cttvFile.close()

    def generate_evidence(self):

        bar = tqdm(desc='Generate PhenoDigm evidence strings',
                   total=8,
                   unit='steps',
                   # file=tqdm_out,
                   )

        self._logger.info("Load MP classes")
        self.mp.load_mp_classes()
        bar.update()
        self._logger.info("Load HP classes")
        self.hpo.load_hpo_classes()
        bar.update()
        self._logger.info("Load EFO classes")
        self.efo.load_efo_classes()
        bar.update()
        self._logger.info("Get all OMIM x-refs")
        self.get_omim_to_efo_mappings()

        #self.omim_to_efo_map["OMIM:191390"] = ["http://www.ebi.ac.uk/efo/EFO_0003767"]
        #self.omim_to_efo_map["OMIM:266600"] = ["http://www.ebi.ac.uk/efo/EFO_0003767"]
        #self.omim_to_efo_map["OMIM:612278"] = ["http://www.ebi.ac.uk/efo/EFO_0003767"]
        #self.omim_to_efo_map["OMIM:608049"] = ["http://www.ebi.ac.uk/efo/EFO_0003756"]
        #self.omim_to_efo_map["OMIM:300494"] = ["http://www.ebi.ac.uk/efo/EFO_0003757"]
        bar.update()
        self._logger.info("Load all mouse and human")
        self.load_mouse_genes(Config.MOUSEMODELS_CACHE_DIRECTORY)
        self.load_human_genes(Config.MOUSEMODELS_CACHE_DIRECTORY)
        bar.update()
        self._logger.info("Parse phenodigm files")
        self.parse_phenodigm_files()
        bar.update()
        self._logger.info("Build evidence")
        self.generate_phenodigm_evidence_strings()
        bar.update()
        self._logger.info("write evidence strings")
        self.write_phenodigm_evidence_strings(Config.MOUSEMODELS_CACHE_DIRECTORY)
        bar.update()
        return

def main():

    ph = Phenodigm()
    ph.generate_evidence()
    #g2p.generate_evidence_strings(G2P_FILENAME)

if __name__ == "__main__":
    main()