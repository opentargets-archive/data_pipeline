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
from settings import Config

__author__ = 'gautierk'

logger = logging.getLogger(__name__)

TOP_LEVELS = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
select *
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
where {
  ?top_level rdfs:subClassOf <%s> .
  ?top_level rdfs:label ?top_level_label
}
'''

DIRECT_ANCESTORS = '''
# %s
PREFIX obo: <http://purl.obolibrary.org/obo/>
SELECT ?dist1 as ?distance ?y as ?ancestor ?ancestor_label ?x as ?direct_child ?direct_child_label
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
   WHERE
    {
       ?x rdfs:subClassOf ?y
       option(transitive, t_max(1), t_in(?x), t_out(?y), t_step("step_no") as ?dist1) .
       ?y rdfs:label ?ancestor_label .
       ?x rdfs:label ?direct_child_label .
       FILTER (?x = <%s>)
    }
order by ?dist1
'''

INDIRECT_ANCESTORS = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
SELECT ?dist1 as ?distance ?y as ?ancestor ?ancestor_label ?z as ?direct_child ?direct_child_label
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
   WHERE
    {
       ?x rdfs:subClassOf ?y
       option(transitive, t_max(20), t_in(?x), t_out(?y), t_step("step_no") as ?dist1) .
       ?y rdfs:label ?ancestor_label .
       ?z rdfs:subClassOf ?y .
       ?z rdfs:label ?direct_child_label .
       {SELECT ?z WHERE { ?x2 rdfs:subClassOf ?z option(transitive) FILTER (?x2 = <%s>) }}
       FILTER (?x = <%s>)
    }
order by ?dist1
'''

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

        self.phenotype_current = {}
        self.phenotype_obsolete = {}
        self.phenotype_map = {}
        self.phenotype_top_levels = {}
        self.phenotype_excluded = set()

        self.disease_current = {}
        self.disease_obsolete = {}
        self.disease_map = {}
        self.disease_top_levels = {}
        self.disease_excluded = set()

    def get_ontology_top_levels(self, base_class, top_level_map):
        sparql_query = TOP_LEVELS
        self.sparql.setQuery(sparql_query%base_class)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        for result in results["results"]["bindings"]:
            #print json.dumps(result)
            top_level_label = result['top_level_label']['value']
            top_level = result['top_level']['value']
            top_level_map[top_level] = top_level_label
            print "%s %s"%(top_level, top_level_label)

    def get_ontology_path(self, base_class, term):

        if term in self.phenotype_map or term in self.disease_map:
            return

        #if term == 'http://purl.obolibrary.org/obo/HP_0001251':
        if True:

            print "---------"
            for sparql_query in [DIRECT_ANCESTORS, INDIRECT_ANCESTORS]:
                self.sparql.setQuery(sparql_query%(term, term))
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.query().convert()
                #print len(results)
                #print json.dumps(results)

                for result in results["results"]["bindings"]:
                    #print json.dumps(result)
                    count = int(result['distance']['value'])
                    parent_label = result['ancestor_label']['value']
                    ancestor = result['ancestor']['value']
                    direct_child = result['direct_child']['value']
                    direct_child_label = result['direct_child_label']['value']
                    if direct_child not in self.phenotype_map:
                        self.phenotype_map[direct_child] = { 'label': direct_child_label , 'superclasses': [] }
                    if ancestor not in self.phenotype_map[direct_child]['superclasses']:
                        self.phenotype_map[direct_child]['superclasses'].append(ancestor)
                        print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)
                    #print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)
            print "---------"

    def load_ontology(self, prefix='', name_space='', base_class=None, current=None, obsolete=None):
        '''
        Load ontology to accept phenotype terms that are not
        :return:
        '''
        sparql_query = '''
        %s
        SELECT DISTINCT ?ont_node ?label
        FROM %s
        {
        ?ont_node rdfs:subClassOf* <%s> .
        ?ont_node rdfs:label ?label
        }
        '''
        self.sparql.setQuery(sparql_query % (prefix, name_space, base_class))
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
         FROM %s
         FROM <http://purl.obolibrary.org/obo/>
         {
            ?hp_node owl:deprecated true .
            ?hp_node oboInOwl:id ?id .
            ?hp_node obo:IAO_0100001 ?hp_new .
            ?hp_node rdfs:label ?label

         }
        '''
        self.sparql.setQuery(sparql_query % name_space)
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

    def load_hpo(self, base_class):
        '''
        Load HPO to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            name_space='<http://purl.obolibrary.org/obo/hp.owl>',
            base_class=base_class,
            current=self.phenotype_current,
            obsolete=self.phenotype_obsolete)
        self.get_ontology_top_levels(base_class, top_level_map=self.phenotype_top_levels)

    def load_mp(self, root):
        '''
        Load MP to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            name_space='<http://purl.obolibrary.org/obo/mp.owl>',
            base_class=root,
            current=self.phenotype_current,
            obsolete=self.phenotype_obsolete)
        self.get_ontology_top_levels(root, top_level_map=self.phenotype_top_levels)


    def load_efo(self, root):
        '''
        Load EFO to accept rare disease terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            prefix='PREFIX efo: <http://www.ebi.ac.uk/efo/>',
            name_space='<http://www.ebi.ac.uk/efo/>',
            base_class=root,
            current=self.disease_current,
            obsolete=self.disease_obsolete)
        self.get_ontology_top_levels(root, top_level_map=self.disease_top_levels)

    def exclude_phenotypes(self, l):
        '''
        :param l:
        :return:
        '''
        for p in l:
            if p not in self.phenotype_excluded:
                self.phenotype_excluded.add(p)
                print "Excluding %s"%p
                # get parents
                sparql_query = DIRECT_ANCESTORS
                self.sparql.setQuery(sparql_query%(p, p))
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.query().convert()
                al = []
                for result in results["results"]["bindings"]:
                    count = int(result['distance']['value'])
                    parent_label = result['ancestor_label']['value']
                    ancestor = result['ancestor']['value']
                    al.append(ancestor)
                    self.exclude_phenotypes(al)

    def generate_ttl_query(self, filename):

        with open(filename, 'w') as hfile:
            # create restricted list
            print ",".join(self.phenotype_top_levels.keys())
            for p in self.phenotype_top_levels:
                if p in self.phenotype_map:
                    self.exclude_phenotypes(self.phenotype_map[p]['superclasses'])
            #return

            hfile.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n")

            for k,v  in self.phenotype_map.iteritems():
                count = 0
                if k not in self.phenotype_excluded:
                    hfile.write("<%s> rdfs:label \"%s\" .\n"%(k, v['label']))
                    if k in self.phenotype_top_levels:
                        hfile.write("<%s> rdfs:subClassOf <http://www.ebi.ac.uk/efo/EFO_0000651> .\n"%k)
                    else:
                        for p in v['superclasses']:
                            hfile.write("<%s> rdfs:subClassOf <%s> .\n"%(k, p))

        hfile.close()

    def create_phenotype_slim(self):

        self.load_hpo(base_class='http://purl.obolibrary.org/obo/HP_0000118')
        self.load_mp(root='http://purl.obolibrary.org/obo/MP_0000001')
        #self.load_efo(root='http://www.ebi.ac.uk/efo/EFO_0000508')

        for file_on_disk in Config.ONTOLOGY_PREPROCESSING_FILES:

            self.parse_gzipfile(file_on_disk)

        # this is purely a test to see whether it works on Orphanet terms
        #for id in ['http://www.orpha.net/ORDO/Orphanet_188', 'http://www.orpha.net/ORDO/Orphanet_217720', 'http://www.orpha.net/ORDO/Orphanet_251576' ]:
        #    self.get_ontology_path('http://www.ebi.ac.uk/efo/EFO_0000508', id)

        self.generate_ttl_query(Config.ONTOLOGY_SLIM_FILE)

    def parse_gzipfile(self, file_on_disk):

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
                        ''' get all terms '''
                        self.get_ontology_path('http://purl.obolibrary.org/obo/HP_0000118', id)

                    elif re.match('http://purl.obolibrary.org/obo/MP_\d+', id):
                        ''' get all terms '''
                        self.get_ontology_path('http://purl.obolibrary.org/obo/MP_0000001', id)
                    elif re.match('http://www.orpha.net/ORDO/Orphanet_\d+', id):
                        ''' just map to the genetic disorders '''
                        self.get_ontology_path('http://www.ebi.ac.uk/efo/EFO_0000508', id)


        fh.close()
        return
