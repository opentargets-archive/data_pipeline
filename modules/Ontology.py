import os
import re
import sys
reload(sys);
sys.setdefaultencoding("utf8");
import gzip
import cttv.model.core as cttv
import logging
import json
from common import Actions
from SPARQLWrapper import SPARQLWrapper, JSON

__author__ = 'gautierk'

logger = logging.getLogger(__name__)

SPARQL_PATH_QUERY = '''
PREFIX efo: <http://www.ebi.ac.uk/efo/>
SELECT ?node_uri ?parent_uri ?parent_label ?dist ?path
FROM <http://www.ebi.ac.uk/efo/>
WHERE
  {
    {
      SELECT *
      WHERE
        {
          ?node_uri rdfs:subClassOf ?y .
          ?node_uri rdfs:label ?parent_label
        }
    }
    OPTION ( TRANSITIVE, t_min(1), t_in (?y), t_out (?node_uri), t_step (?y) as ?parent_uri, t_step ('step_no') as ?dist, t_step ('path_id') as ?path ) .
    FILTER ( ?y = efo:EFO_0000408 )
  }
'''

'''
PREFIX obo: <http://purl.obolibrary.org/obo/>

select ?superclass where {
  obo:HP_0003074 rdfs:subClassOf* ?superclass
}
'''

SINGLE_CLASS_PATH_QUERY = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>

select ?class ?parent_label count(?mid) AS ?count
FROM <http://purl.obolibrary.org/obo/hp.owl>
where {
  obo:HP_0003074 rdfs:subClassOf* ?mid .
  ?mid rdfs:subClassOf* ?class .
  ?class rdfs:label ?parent_label .
}
group by ?class ?parent_label
order by ?count
'''

class OntologyActions(Actions):
    PHENOTYPESLIM = 'phenotypeslim'

class PhenotypeSlim():

    def __init__(self, sparql):

        self.sparql = sparql

        self.hpo_current = {}
        self.hpo_obsolete = {}
        self.mp_current = {}
        self.mp_obsolete = {}

        self.hp_map = {}
        self.mp_map = {}

    def get_ontology_path(self, name, base_class, term):

        sparql_query = '''
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        select ?class ?parent_label count(?mid) AS ?count
        FROM <http://purl.obolibrary.org/obo/%s.owl>
        where {
        obo:%s rdfs:subClassOf* ?mid .
        ?mid rdfs:subClassOf* ?class .
        ?class rdfs:label ?parent_label .
        }
        group by ?class ?parent_label
        order by ?count
        '''
        self.sparql.setQuery(sparql_query%(name, term))
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()


        for result in results["results"]["bindings"]:
            print json.dumps(result)
            uri = result['ont_node']['value']
            label = result['label']['value']
            current[uri] = label

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

    def create_phenotype_slim(self):

        self.load_hpo()
        self.load_mp()
        file_on_disk = '/Users/koscieln/Documents/data/ftp/cttv008/upload/submissions/cttv008-14-03-2016.json.gz'
        self.parse_gzipfile(file_on_disk)

    def parse_gzipfile(self, file_on_disk):

        self.load_hpo()
        self.load_mp()

        logging.info('Starting parsing %s' %file_on_disk)

        fh = gzip.GzipFile(file_on_disk, "r")
        line_buffer = []
        offset = 0
        chunk = 1
        line_number = 0

        for line in fh:
            python_raw = json.loads(line)
            obj = cttv.Drug.fromMap(python_raw)
            if obj.disease.id:
                for id in obj.disease.id:
                    if re.match('http://purl.obolibrary.org/obo/HP_\d+', id):
                        hp_match = re.match('http://purl.obolibrary.org/obo/(HP_\d+)', id)
                        term_id = hp_match.groups()[0]
                        if id not in self.hp_map:
                            ''' get all terms '''
                            self.get_ontology_path('hp', 'http://purl.obolibrary.org/obo/HP_0000118', term_id)

                    elif re.match('http://purl.obolibrary.org/obo/MP_\d+', id):
                        mp_match = re.match('http://purl.obolibrary.org/obo/(MP_\d+)', id)
                        term_id = mp_match.groups()[0]
                        if id not in self.mp_map:
                            ''' get all terms '''
                            self.get_ontology_path('mp', 'http://purl.obolibrary.org/obo/MP_0000001', term_id)


        fh.close()
        return
