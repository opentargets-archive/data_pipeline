import logging

from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Match,Bool

from cachetools import cached, LFUCache

class HPALookUpTable(object):

    def __init__(self, es, index):
        self._es = es
        self._es_index = index

#    @cached(cache=LFUCache(512))
    def get_hpa(self, hpa_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=hpa_id))[0:1].execute()
        if response.hits.total == 0:
            #no hit, return None
            return None
        else:
            #exactly one hit, return it
            return response.hits[0].to_dict()
        #can't have multiple hits, primary key!

class GeneLookUpTable(object):

    def __init__(self, es, es_index,):
        self._es = es
        self._es_index = es_index

#    @cached(cache=LFUCache(512))
    def get_gene(self, gene_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].execute()
        if response.hits.total == 0:
            #no hit, return None
            return None
        else:
            #exactly one hit, return it
            return response.hits[0].to_dict()
        #can't have multiple hits, primary key!

#    @cached(cache=LFUCache(512))
    def get_uniprot2ensembl(self, uniprot_id):
        response = Search().using(self._es).index(self._es_index).query(
            Bool(should=[
                Match(uniprot_id=uniprot_id),
                Match(uniprot_accessions=uniprot_id)
            ]))[0:1].execute()
        if response.hits.total == 0:
            #no hit, return None
            return None
        elif response.hits.total == 1:
            #exactly one hit, return it
            return response.hits[0].to_dict()
        else:
            #more then one hit, throw error
            raise ValueError("Multiple genes with uniprot %s" %(uniprot_id))

#    @cached(cache=LFUCache(512))
    def __contains__(self, gene_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=gene_id))[0:1].source(False).execute()
        if response.hits.total == 0:
            #no hit
            return False
        else:
            #exactly one hit
            return True
        #can't have multiple hits, primary key!

class ECOLookUpTable(object):
    def __init__(self, es, es_index):
        self._es = es
        self._es_index = es_index

#    @cached(cache=LFUCache(512))
    def get_eco(self, eco_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=eco_id))[0:1].execute()
        return response.hits[0].to_dict()

class EFOLookUpTable(object):

    def __init__(self, es, index):
        self._es = es
        self._es_index = index

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

#    @cached(cache=LFUCache(512))
    def get_efo(self, efo_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].execute()
        if response.hits.total == 0:
            #no hit, return None
            return None
        else:
            #exactly one hit, return it
            return response.hits[0].to_dict()
        #can't have multiple hits, primary key!

#    @cached(cache=LFUCache(512))
    def __contains__(self, efo_id):
        response = Search().using(self._es).index(self._es_index).query(Match(_id=efo_id))[0:1].source(False).execute()
        if response.hits.total == 0:
            #no hit
            return False
        else:
            #exactly one hit
            return True
        #can't have multiple hits, primary key!
