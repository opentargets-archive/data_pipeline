import sys
import httplib
import time
import optparse
import logging
import os
from common import Actions
from settings import Config
from EvidenceValidation import EvidenceValidationFileChecker
import elasticsearch
from elasticsearch import Elasticsearch, helpers

class MouseModelsActions(Actions):
    UPDATE_CACHE = 'updatecache'
    UPDATE_GENES = 'updategenes'
    GENERATE_EVIDENCE = 'generateevidence'

class Phenodigm():

    def __init__(self, adapter, es, sparql):
        self.adapter = adapter
        self.session = adapter.session
        self.es = es
        self.sparql = sparql
        self.cache = {}
        self.counter = 0

    def list_files(self, path):
        '''
        returns a list of names (with extension, without full path) of all files
        in folder path
        '''
        files = []
        for name in os.listdir(path):
            if os.path.isfile(os.path.join(path, name)):
                files.append(os.path.join(path, name))
        return files

    def update_cache(self):
        hdr = { 'User-Agent' : 'cttv bot by /center/for/therapeutic/target/validation' }
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
            logging.info("REQUEST {0}. {1}".format(counter, uri))
            conn.request('GET', uri, headers=hdr)
            raw = conn.getresponse().read()
            rsp = eval (raw)
            nbItems = len(rsp['response']['docs']);
            phenodigmFile = open(options.directory + "/part{0}".format(counter), "w")
            phenodigmFile.write(raw)
            phenodigmFile.close()
            start+=nbItems

        conn.close()

    def update_genes(self):

        hsGenes = {}
        mmGenes = {}
        ev = EvidenceValidationFileChecker(self.adapter, self.es, self.sparql)
        ev.load_gene_mapping()
        logging.info("Get all mouse genes")
        hdr = { 'User-Agent' : 'cttv bot by /center/for/therapeutic/target/validation' }
        conn = httplib.HTTPConnection('rest.ensembl.org')

        parts = self.list_files(Config.MOUSEMODELS_CACHE_DIRECTORY)
        parts.sort()
        tick = 0
        for part in parts:
            logging.info("processing {0}\n".format(part))
            with open (part, "r") as myfile:
                rsp = eval(myfile.read())
                for doc in rsp['response']['docs']:
                    if doc['type'] == 'gene' and 'hgnc_id' in doc:
                        #tick+=1
                        #hgnc_id = doc['hgnc_id']
                        marker_symbol = doc['marker_symbol']
                        #if hgnc_id and not hgnc_id in hsGenes:
                        #    conn.request('GET', '/xrefs/symbol/homo_sapiens/%s?content-type=application/json;external_db=HGNC' %(hgnc_id), headers=hdr)
                        #    ensemblArray = eval( conn.getresponse().read() )
                        #    for eItem in ensemblArray:
                        #        if eItem['type'] == 'gene':
                        #            hsGenes[hgnc_id] = eItem['id']
                        #        else:
                        #            hsGenes[hgnc_id] = None
                        #    ensemblId = hsGenes[hgnc_id]
                        #        print "Human Gene: {0} {1}\n".format(hgnc_id, ensemblId)
                        if marker_symbol and not marker_symbol in mmGenes:
                            tick+=1
                            logging.info("Request %s..."%marker_symbol)
                            conn.request('GET', '/xrefs/symbol/mus_musculus/%s?content-type=application/json;external_db=MGI' %(marker_symbol), headers=hdr)
                            ensemblArray = eval( conn.getresponse().read() )

                            for eItem in ensemblArray:
                                if eItem['type'] == 'gene':
                                    mmGenes[marker_symbol] = eItem['id']
                                else:
                                    mmGenes[marker_symbol] = None
                                ensemblId = mmGenes[marker_symbol]
                                print "Mouse Gene: {0} {1}\n".format(marker_symbol, ensemblId)
                        if tick % 7 == 0:
                           time.sleep(2)
            myfile.close()

        with open(os.path.join(Config.MOUSEMODELS_CACHE_DIRECTORY, "mmGenes.json"), "w") as mmGenesFile:
            json.dump(mmGenes, mmGenesFile, indent=2)
        mmGenesFile.close()