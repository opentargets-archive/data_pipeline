import logging

from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Match,Bool

import cachetools

#TODO remove this class, migrate each of these to where they are actually used

class HPALookUpTable(object):

    def __init__(self, es, index):
        self._es = es
        self._es_index = index
        #TODO configure size
        self.cache = cachetools.LRUCache(4096)

    def get_hpa(self, hpa_id):

        if hpa_id in self.cache:
            return self.cache[hpa_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=hpa_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if int(response.hits.total.value) == 0 or len(response.hits) == 0:
            #no hit, return None
            self.cache[hpa_id] = None
            return None
        else:
            #exactly one hit, return it
            val = response.hits[0].to_dict()
            self.cache[hpa_id] = val
            return val
        #can't have multiple hits, primary key!

class GeneLookUpTable(object):

    def __init__(self, es, es_index,):
        self._es = es
        self._es_index = es_index
        #TODO configure size
        self.cache_gene = cachetools.LRUCache(4096)
        self.cache_u2e = cachetools.LRUCache(8192)
        self.cache_contains = cachetools.LRUCache(8192)

    def get_gene(self, gene_id):
        assert gene_id is not None

        if gene_id in self.cache_gene:
            return self.cache_gene[gene_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if int(response.hits.total.value) == 0 or len(response.hits) == 0:
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

        if uniprot_id in self.cache_u2e:
            return self.cache_u2e[uniprot_id]

        response = Search().using(self._es).index(self._es_index).query(
            Bool(should=[
                Match(uniprot_id=uniprot_id),
                Match(uniprot_accessions=uniprot_id)
            ]))[0:1].source(includes=["ensembl_gene_id"]).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html            
        if int(response.hits.total.value) == 0 or len(response.hits) == 0:
            #no hit, return None
            self.cache_u2e[uniprot_id] = None
            return None
        elif int(response.hits.total.value) == 1 or len(response.hits) == 1:
            #exactly one hit, return it
            val = response.hits[0].ensembl_gene_id
            self.cache_u2e[uniprot_id] = val
            return val
        else:
            #more then one hit, throw error
            raise ValueError("Multiple genes with uniprot %s" %(uniprot_id))

    def __contains__(self, gene_id):

        if gene_id in self.cache_u2e:
            return self.cache_contains[gene_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].source(False).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if int(response.hits.total.value) > 0 or len(response.hits) > 0:
            #exactly one hit
            self.cache_contains[gene_id] = True
            return True
        else:
            #no hit
            self.cache_contains[gene_id] = False
            return False
        #can't have multiple hits, primary key!

class ECOLookUpTable(object):
    def __init__(self, es, es_index):
        self._es = es
        self._es_index = es_index
        #TODO configure size
        self.cache_eco = cachetools.LRUCache(4096)

    def get_eco(self, eco_id):

        if eco_id in self.cache_eco:
            return self.cache_eco[eco_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=eco_id))[0:1].execute()
        val = response.hits[0].to_dict()
        self.cache_eco[eco_id] = val
        return val

class EFOLookUpTable(object):

    def __init__(self, es, index):
        self._es = es
        self._es_index = index
        #TODO configure size
        self.cache_efo = cachetools.LRUCache(4096)
        self.cache_contains = cachetools.LRUCache(4096)

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

        if efo_id in self.cache_efo:
            return self.cache_efo[efo_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if int(response.hits.total.value) == 0 or len(response.hits) == 0:
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

        if efo_id in self.cache_contains:
            return self.cache_contains[efo_id]

        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].source(False).execute()
        #see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-request-track-total-hits.html
        if int(response.hits.total.value) == 0 or len(response.hits) == 0:
            #no hit
            self.cache_contains[efo_id] = False
            return False
        else:
            #exactly one hit
            self.cache_contains[efo_id] = True
            return True
        #can't have multiple hits, primary key!
