from __future__ import division
from builtins import object
from past.utils import old_div
import logging

from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Match,Bool

import cachetools
import sys

#TODO remove this class, migrate each of these to where they are actually used

class HPALookUpTable(object):

    def __init__(self, es, index, cachesize):
        self._es = es
        self._es_index = index
        self.cache = cachetools.LRUCache(cachesize, getsizeof=sys.getsizeof)
        self.cache.hits = 0
        self.cache.queries = 0

    def get_hpa(self, hpa_id):

        self.cache.queries += 1
        if hpa_id in self.cache:
            self.cache.hits += 1
            return self.cache[hpa_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=hpa_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if response.hits.total.value == 0:
            #no hit, return None
            self.cache[hpa_id] = None
            return None
        else:
            #exactly one hit, return it
            val = response.hits[0].to_dict()
            self.cache[hpa_id] = val
            return val
        #can't have multiple hits, primary key!

    def __del__(self):
        logger = logging.getLogger(__name__+".HPALookUpTable")
        if self.cache.queries == 0:
            if self.cache.maxsize > 0:
                logger.debug("cache {} occupied 100 hitrate".format(
                    old_div((self.cache.currsize*100),self.cache.maxsize)))
        else:
            logger.debug("cache {} occupied {} hitrate".format(
                old_div((self.cache.currsize*100),self.cache.maxsize),
                old_div((self.cache.hits*100),self.cache.queries) ))

class GeneLookUpTable(object):

    def __init__(self, es, es_index, cache_gene_size, cache_u2e_size, cache_contains_size):
        self._es = es
        self._es_index = es_index

        self.cache_gene = cachetools.LRUCache(cache_gene_size, getsizeof=sys.getsizeof)
        self.cache_gene.hits = 0
        self.cache_gene.queries = 0
        self.cache_u2e = cachetools.LRUCache(cache_u2e_size, getsizeof=sys.getsizeof)
        self.cache_u2e.hits = 0
        self.cache_u2e.queries = 0
        self.cache_contains = cachetools.LRUCache(cache_contains_size, getsizeof=sys.getsizeof)
        self.cache_contains.hits = 0
        self.cache_contains.queries = 0

    def get_gene(self, gene_id):
        assert gene_id is not None

        self.cache_gene.queries += 1
        if gene_id in self.cache_gene:
            self.cache_gene.hits += 1
            return self.cache_gene[gene_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if response.hits.total.value == 0:
            #no hit, return None
            self.cache_gene[gene_id] = None
            return None
        else:
            #exactly one hit, return it
            val = response.hits[0].to_dict()
            self.cache_gene[gene_id] = val
            return val
        #can't have multiple hits, primary key!

    def get_uniprot2ensembl(self, uniprot_id):
        assert uniprot_id is not None

        self.cache_u2e.queries += 1
        if uniprot_id in self.cache_u2e:
            self.cache_u2e.hits += 1
            return self.cache_u2e[uniprot_id]

        response = Search().using(self._es).index(self._es_index).query(
            Bool(should=[
                Match(uniprot_id=uniprot_id),
                Match(uniprot_accessions=uniprot_id)
            ]))[0:1].source(includes=["ensembl_gene_id"]).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html            
        if response.hits.total.value == 0:
            #no hit, return None
            self.cache_u2e[uniprot_id] = None
            return None
        elif response.hits.total.value == 1:
            #exactly one hit, return it
            val = response.hits[0].ensembl_gene_id
            self.cache_u2e[uniprot_id] = val
            return val
        else:
            #more then one hit, throw error
            raise ValueError("Multiple genes with uniprot %s" %(uniprot_id))

    def __contains__(self, gene_id):

        self.cache_contains.queries += 1
        if gene_id in self.cache_contains:
            self.cache_contains.hits += 1
            return self.cache_contains[gene_id]

        #check the gene cache too
        if gene_id in self.cache_gene:
            if self.cache_gene[gene_id] is None:
                return False
            else:
                return True

        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].source(False).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if response.hits.total.value > 0:
            #exactly one hit
            self.cache_contains[gene_id] = True
            return True
        else:
            #no hit
            self.cache_contains[gene_id] = False
            return False
        #can't have multiple hits, primary key!

    def __del__(self):
        logger = logging.getLogger(__name__+".GeneLookUpTable")

        if self.cache_gene.queries == 0:
            if self.cache_gene.maxsize > 0:
                logger.debug("cache_gene {} occupied 100 hitrate".format(
                    old_div((self.cache_gene.currsize*100),self.cache_gene.maxsize)))
        else:
            logger.debug("cache_gene {} occupied {} hitrate".format(
                old_div((self.cache_gene.currsize*100),self.cache_gene.maxsize),
                old_div((self.cache_gene.hits*100),self.cache_gene.queries) ))

        if self.cache_u2e.queries == 0:
            if self.cache_u2e.maxsize > 0:
                logger.debug("cache_u2e {} occupied 100 hitrate".format(
                    old_div((self.cache_u2e.currsize*100),self.cache_u2e.maxsize)))
        else:
            logger.debug("cache_u2e {} occupied {} hitrate".format(
                old_div((self.cache_u2e.currsize*100),self.cache_u2e.maxsize),
                old_div((self.cache_u2e.hits*100),self.cache_u2e.queries) ))

        if self.cache_contains.queries == 0:
            if self.cache_contains.maxsize > 0:
                logger.debug("cache_contains {} occupied 100 hitrate".format(
                    old_div((self.cache_contains.currsize*100),self.cache_contains.maxsize)))
        else:
            logger.debug("cache_contains {} occupied {} hitrate".format(
                old_div((self.cache_contains.currsize*100),self.cache_contains.maxsize),
                old_div((self.cache_contains.hits*100),self.cache_contains.queries) ))

class ECOLookUpTable(object):
    def __init__(self, es, es_index, cache_size):
        self._es = es
        self._es_index = es_index
        #TODO configure size
        self.cache = cachetools.LRUCache(cache_size, getsizeof=sys.getsizeof)
        self.cache.hits = 0
        self.cache.queries = 0

    def get_eco(self, eco_id):

        self.cache.queries += 1
        if eco_id in self.cache:
            self.cache.hits += 1
            return self.cache[eco_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=eco_id))[0:1].execute()
        if response.hits.total.value > 0:
            val = response.hits[0].to_dict()
            self.cache[eco_id] = val
            return val
        else:
            #more then one hit, throw error
            raise ValueError("Multiple eco %s" %(eco_id))

    def __del__(self):
        logger = logging.getLogger(__name__+".ECOLookUpTable")
        if self.cache.queries == 0:
            if self.cache.maxsize > 0:
                logger.debug("cache {} occupied 100 hitrate".format(
                    old_div((self.cache.currsize*100),self.cache.maxsize)))
        else:
            logger.debug("cache {} occupied {} hitrate".format(
                old_div((self.cache.currsize*100),self.cache.maxsize),
                old_div((self.cache.hits*100),self.cache.queries) ))

class EFOLookUpTable(object):

    def __init__(self, es, index, cache_efo_size, cache_contains_size):
        self._es = es
        self._es_index = index
        #TODO configure size
        self.cache_efo = cachetools.LRUCache(cache_efo_size, getsizeof=sys.getsizeof)
        self.cache_efo.hits = 0
        self.cache_efo.queries = 0
        self.cache_contains = cachetools.LRUCache(cache_contains_size, getsizeof=sys.getsizeof)
        self.cache_contains.hits = 0
        self.cache_contains.queries = 0

    @staticmethod
    def get_ontology_code_from_url(url):
        #note, this is not a guaranteed solution
        #to do it properly, it has to be from the actual
        #ontology file or from OLS API
        if '/' in url:
            return url.split('/')[-1]
        else:
            #assume already a short code
            return url

    def get_efo(self, efo_id):
        
        self.cache_efo.queries += 1
        if efo_id in self.cache_efo:
            self.cache_efo.hits += 1
            return self.cache_efo[efo_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if response.hits.total.value == 0:
            #no hit, return None
            self.cache_efo[efo_id] = None
            return None
        else:
            #exactly one hit, return it
            val = response.hits[0].to_dict()
            self.cache_efo[efo_id] = val
            return val
        #can't have multiple hits, primary key!

    def __contains__(self, efo_id):

        self.cache_contains.queries += 1
        if efo_id in self.cache_contains:
            self.cache_contains.hits += 1
            return self.cache_contains[efo_id]

        #check the main cache too
        if efo_id in self.cache_efo:
            if self.cache_efo[efo_id] is None:
                return False
            else:
                return True

        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].source(False).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if response.hits.total.value == 0:
            #no hit
            self.cache_contains[efo_id] = False
            return False
        else:
            #exactly one hit
            self.cache_contains[efo_id] = True
            return True
        #can't have multiple hits, primary key!

    def __del__(self):
        logger = logging.getLogger(__name__+".EFOLookUpTable")

        if self.cache_efo.queries == 0:
            if self.cache_efo.maxsize > 0:
                logger.debug("cache_efo {} occupied 100 hitrate".format(
                    old_div((self.cache_efo.currsize*100),self.cache_efo.maxsize)))
        else:
            logger.debug("cache_efo {} occupied {} hitrate".format(
                old_div((self.cache_efo.currsize*100),self.cache_efo.maxsize),
                old_div((self.cache_efo.hits*100),self.cache_efo.queries) ))

        if self.cache_contains.queries == 0:
            if self.cache_contains.maxsize > 0:
                logger.debug("cache_contains {} occupied 100 hitrate".format(
                    old_div((self.cache_contains.currsize*100),self.cache_contains.maxsize)))
        else:
            logger.debug("cache_contains {} occupied {} hitrate".format(
                old_div((self.cache_contains.currsize*100),self.cache_contains.maxsize),
                old_div((self.cache_contains.hits*100),self.cache_contains.queries) ))
