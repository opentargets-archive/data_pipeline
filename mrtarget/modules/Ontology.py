import copy
import re
import sys
reload(sys)
sys.setdefaultencoding("utf8")
import os
import pysftp
import gzip
import pickle
from paramiko import AuthenticationException
import opentargets.model.core as opentargets
import logging
import json
import rdflib
import requests
from rdflib import URIRef
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.namespace import OWL, RDF, RDFS
from mrtarget.common import Actions
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from mrtarget.common import TqdmToLogger
from datetime import date
from mrtarget.Settings import Config


__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

logger = logging.getLogger(__name__)

EFO_TAS = [
    'http://www.ebi.ac.uk/efo/EFO_1000018', # bladder disease
    'http://www.ebi.ac.uk/efo/EFO_0000319', # cardiovascular disease
    'http://www.ebi.ac.uk/efo/EFO_0000405', # digestive system disease
    'http://www.ebi.ac.uk/efo/EFO_0001379', # endocrine
    'http://www.ebi.ac.uk/efo/EFO_0003966', # eye disease
    'http://www.ebi.ac.uk/efo/EFO_0000508', # genetic disorder
    'http://www.ebi.ac.uk/efo/EFO_0000524', # head disease
    'http://www.ebi.ac.uk/efo/EFO_0005803', # henmatological
    'http://www.ebi.ac.uk/efo/EFO_0000540', # immune system disease
    'http://www.ebi.ac.uk/efo/EFO_0003086', # kidney disease
    'http://www.ebi.ac.uk/efo/EFO_0005741', # infection disease
    'http://www.ebi.ac.uk/efo/EFO_0000589', # metabolic disease
    'http://www.ebi.ac.uk/efo/EFO_0000616', # neoplasm
    'http://www.ebi.ac.uk/efo/EFO_0000618', # nervous system
    'http://www.ebi.ac.uk/efo/EFO_0000512', # reproductive system
    'http://www.ebi.ac.uk/efo/EFO_0000684', # respiratory system
    'http://www.ebi.ac.uk/efo/EFO_0002461', # skeletal system
    'http://www.ebi.ac.uk/efo/EFO_0000701', # skin disease
    'http://www.ebi.ac.uk/efo/EFO_0001421', # liver disease
]

HPO_TAS = [
    "http://purl.obolibrary.org/obo/HP_0005386", #"behavior/neurological phenotype",
    "http://purl.obolibrary.org/obo/HP_0005375", #"adipose tissue phenotype",
    "http://purl.obolibrary.org/obo/HP_0005385", #"cardiovascular system phenotype",
    "http://purl.obolibrary.org/obo/HP_0005384", #"cellular phenotype",
    "http://purl.obolibrary.org/obo/HP_0005382", #"craniofacial phenotype",
    "http://purl.obolibrary.org/obo/HP_0005381", #"digestive/alimentary phenotype",
    "http://purl.obolibrary.org/obo/HP_0005380", #"embryo phenotype",
    "http://purl.obolibrary.org/obo/HP_0005379", #"endocrine/exocrine phenotype",
    "http://purl.obolibrary.org/obo/HP_0005378", #"growth/size/body region phenotype",
    "http://purl.obolibrary.org/obo/HP_0005377", #"hearing/vestibular/ear phenotype",
    "http://purl.obolibrary.org/obo/HP_0005397", #"hematopoietic system phenotype",
    "http://purl.obolibrary.org/obo/HP_0005376", #"homeostasis/metabolism phenotype",
    "http://purl.obolibrary.org/obo/HP_0005387", #"immune system phenotype",
    "http://purl.obolibrary.org/obo/HP_0010771", #"integument phenotype",
    "http://purl.obolibrary.org/obo/HP_0005371", #"limbs/digits/tail phenotype",
    "http://purl.obolibrary.org/obo/HP_0005370", #"liver/biliary system phenotype",
    "http://purl.obolibrary.org/obo/HP_0010768", #"mortality/aging",
    "http://purl.obolibrary.org/obo/HP_0005369", #"muscle phenotype",
    "http://purl.obolibrary.org/obo/HP_0002006", #"neoplasm",
    "http://purl.obolibrary.org/obo/HP_0003631", #"nervous system phenotype",
    "http://purl.obolibrary.org/obo/HP_0002873", #"normal phenotype",
    "http://purl.obolibrary.org/obo/HP_0001186", #"pigmentation phenotype",
    "http://purl.obolibrary.org/obo/HP_0005367", #"renal/urinary system phenotype",
    "http://purl.obolibrary.org/obo/HP_0005389", #"reproductive system phenotype",
    "http://purl.obolibrary.org/obo/HP_0005388", #"respiratory system phenotype",
    "http://purl.obolibrary.org/obo/HP_0005390", #"skeleton phenotype",
    "http://purl.obolibrary.org/obo/HP_0005394", #"taste/olfaction phenotype",
    "http://purl.obolibrary.org/obo/HP_0005391", #"vision/eye phenotype"
]


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

'''
subclass generators: yield a series of values
'''
def _get_subclass_of(arg, graph):
    #logger.debug("[_get_subclass_of] %i"%len(arg))
    node = arg[0]
    depth = arg[1]
    path = arg[2]
    level = arg[3]
    #logger.debug("Superclass: %s; label: %s; depth: %i"%(str(node.identifier), node.value(RDFS.label), depth))

    if level > 0 and depth == level:
        return
    for c in graph.subjects(predicate=RDFS.subClassOf, object=node.identifier):
        cr = rdflib.resource.Resource(graph, c)
        label = cr.value(RDFS.label)
        #logger.debug("\tSubClass: %s; label: %s"%(str(cr.identifier), label))

        #synonyms = []
        # get synonyms by filtering on triples
        #for subject, predicate, obj in graph.triples(
        #        (c,
        #         rdflib.term.URIRef('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym'),
        #         None)):
        #    print subject, predicate, obj
        # create the path here
        yield (cr, depth+1, path + (node,), level)

'''
superclass generators: yield a series of values
'''
def write_superclasses(arg, source_graph):
    # print "[write_superclasses] %i"%len(arg)
    node = arg[0]
    destination_graph = arg[1]
    # print node
    # print node.qname()
    # print node.value(RDFS.label)
    if (node, None, None) not in destination_graph:
        superclasses = list(node.transitive_objects(RDFS.subClassOf))
        for c in superclasses:
            if node.qname() != c.qname():
                # print c
                label = c.value(RDFS.label)
                destination_graph.add((node, RDFS.subClassOf, c))
                yield (c, destination_graph)


class OntologyActions(Actions):
    PHENOTYPESLIM = 'phenotypeslim'
    DISEASEPHENOTYPES = 'diseasephenotypes'

class OntologyClassReader():

    def __init__(self):
        """Initialises the class

        Declares an RDF graph that will contain an ontology representation.
        Current classes are extracted in the current_classes dictionary
        Obsolete classes are extracted in the obsolete_classes dictionary
        """
        self.rdf_graph = rdflib.Graph()
        self.current_classes = dict()
        self.obsolete_classes = dict()
        self.top_level_classes = dict()
        self.classes_paths = dict()
        self.obsoletes = dict()
        self.children = dict()

    def load_ontology_graph(self, uri):
        """Loads the ontology from a URI in a RDFLib graph.

        Given a uri pointing to a OWL file, load the ontology representation in a graph.

        Args:
            uri (str): the URI for the ontology representation in OWL.

        Returns:
            None

        Raises:
            None

        """
        self.rdf_graph.parse(uri, format='xml')

    def get_deprecated_classes(self, obsoleted_in_version=False):

        count = 0

        # do this once
        if len(self.obsoletes) == 0:

            sparql_query = '''
            SELECT DISTINCT ?ont_node ?label ?label ?ont_new
             {
                ?ont_node owl:deprecated true .
                ?ont_node obo:IAO_0100001 ?ont_new_id .
                ?ont_new oboInOwl:id ?ont_new_id .
                ?ont_node rdfs:label ?label
             }
            '''

            if obsoleted_in_version:

                sparql_query = '''
                                SELECT DISTINCT ?ont_node ?label ?reason ?ont_new_id
                                 {
                                    ?ont_node rdfs:subClassOf oboInOwl:ObsoleteClass . 
                                    ?ont_node obo:IAO_0100001 ?ont_new_id .
                                    ?ont_node rdfs:label ?label . 
                                    ?ont_node efo:reason_for_obsolescence ?reason
                                 }
                                '''


                '''
                <rdfs:subClassOf rdf:resource="http://www.geneontology.org/formats/oboInOwl#ObsoleteClass"/>
                <obo:IAO_0100001 rdf:datatype="http://www.w3.org/2001/XMLSchema#string">http://www.ebi.ac.uk/efo/EFO_0002067</obo:IAO_0100001>
                <efo:obsoleted_in_version>2.44</efo:obsoleted_in_version>
                <efo:reason_for_obsolescence>Duplicate with class K562 http://www.ebi.ac.uk/efo/EFO_0002067</efo:reason_for_obsolescence>
                '''

            qres = self.rdf_graph.query(sparql_query)

            for (ont_node, ont_label, ont_reason, ont_new_id) in qres:
                uri = str(ont_node)
                label = str(ont_label)
                reason_for_obsolescence = str(ont_reason)
                # unfortunately there may be some trailing spaces!
                new_uri = str(ont_new_id).strip()
                # point to the new URI
                self.obsoletes[uri] = {'label': label,
                                       'new_uri': new_uri,
                                       'reason_for_obsolescence': reason_for_obsolescence,
                                       'processed': False}
                count +=1
                logger.debug("WARNING: Obsolete %s %s '%s' %s" % (uri, label, reason_for_obsolescence, new_uri))

        """
        Still need to loop over to find the next new class to replace the
        old URI with the latest URI (some intermediate classes can be obsolete too)
        """

        for old_uri, record in self.obsoletes.items():
            if not record['processed']:
                next_uri = self.obsoletes[old_uri]['new_uri']
                next_reason = self.obsoletes[old_uri]['reason_for_obsolescence']
                while next_uri in self.obsoletes.keys():
                    prev_uri = next_uri
                    next_uri = self.obsoletes[prev_uri]['new_uri']
                    if next_uri == prev_uri:
                        break
                    next_reason = self.obsoletes[prev_uri]['reason_for_obsolescence']
                if next_uri in self.current_classes:
                    new_label = self.current_classes[next_uri]
                    logger.warn("%s => %s" % (old_uri, self.obsoletes[old_uri]))
                    self.obsolete_classes[old_uri] = "Use %s label:%s (reason: %s)" % (next_uri, new_label, next_reason)
                else:
                    # load the class
                    self.obsolete_classes[old_uri] = next_reason
                # mark the record as processed
                record['processed'] = True

        return count

    def load_ontology_classes(self, base_class=None):
        """Loads all current and obsolete classes from an ontology stored in RDFLib

        Given a base class in the ontology, extracts the classes and stores the sets of
        current and obsolete classes in dictionaries. This avoids traversing all the graph
        if only a few branches are required.

        Args:
            base_classes (list of str): the root(s) of the ontology to start from.

        Returns:
            None

        Raises:
            None

        """
        sparql_query = '''
        SELECT DISTINCT ?ont_node ?label
        {
        ?ont_node rdfs:subClassOf* <%s> .
        ?ont_node rdfs:label ?label
        }
        '''

        count = 0
        qres = self.rdf_graph.query(sparql_query % base_class)

        for (ont_node, ont_label) in qres:
            uri = str(ont_node)
            label = str(ont_label)
            self.current_classes[uri] = label
            count +=1

            '''
             Add the children too
            '''
            self.get_children(uri=uri)

            # logger.debug("RDFLIB '%s' '%s'" % (uri, label))
        logger.debug("parsed %i classes"%count)

    def get_top_levels(self, base_class=None):

        sparql_query = '''
        select DISTINCT ?top_level ?top_level_label
        {
          ?top_level rdfs:subClassOf <%s> .
          ?top_level rdfs:label ?top_level_label
        }
        '''
        qres = self.rdf_graph.query(sparql_query % base_class)

        for row in qres:
            uri = str(row[0])
            label = str(row[1])
            self.top_level_classes[uri] = label

    def get_children(self, uri):

        disease_uri = URIRef(uri)
        if uri not in self.children:
            self.children[uri] = []
        for c in self.rdf_graph.subjects(predicate=RDFS.subClassOf, object=disease_uri):
            cr = rdflib.resource.Resource(self.rdf_graph, c)
            label = str(cr.value(RDFS.label))
            c_uri = str(cr.identifier)
            (prefix, namespace, id) = self.rdf_graph.namespace_manager.compute_qname(cr.identifier)
            self.children[uri].append({'code': id, 'label': label})

    def get_new_from_obsolete_uri(self, old_uri):

        next_uri = self.obsoletes[old_uri]['new_uri']
        while next_uri in self.obsoletes.keys():
            next_uri = self.obsoletes[next_uri]['new_uri']
        if next_uri in self.current_classes:
            new_label = self.current_classes[next_uri]
            logger.warn("%s => %s" % (old_uri, self.obsoletes[old_uri]))
            return next_uri
        else:
            return None

    def get_classes_paths(self, root_uri, level=0):

        root_node = rdflib.resource.Resource(self.rdf_graph,
                                             rdflib.term.URIRef(root_uri))

        # create an entry for the root:
        root_node_uri = str(root_node.identifier)
        self.classes_paths[root_node_uri] = {'all': [], 'labels': [], 'ids': []}
        self.classes_paths[root_node_uri]['all'].append([{'uri': str(root_node_uri), 'label': root_node.value(RDFS.label)}])
        self.classes_paths[root_node_uri]['labels'].append([root_node.value(RDFS.label)])
        (prefix, namespace, id) = self.rdf_graph.namespace_manager.compute_qname(root_node.identifier)
        self.classes_paths[root_node_uri]['ids'].append([id])

        for entity in self.rdf_graph.transitiveClosure(_get_subclass_of, (root_node, 0, (), level)):

            node = entity[0]
            depth = entity[1]
            path = entity[2]

            node_uri = str(node.identifier)

            if node_uri not in self.classes_paths:
                self.classes_paths[node_uri] = { 'all': [], 'labels': [], 'ids': [] }

            all_struct = []
            labels_struct = []
            ids_struct = []

            for n in path:
                all_struct.append({'uri': str(n.identifier), 'label': n.value(RDFS.label)})
                labels_struct.append( n.value(RDFS.label) )
                (prefix, namespace, id) = self.rdf_graph.namespace_manager.compute_qname(n.identifier)
                ids_struct.append( id )

            all_struct.append( {'uri': str(node_uri), 'label': node.value(RDFS.label)} )
            labels_struct.append( node.value(RDFS.label) )
            (prefix, namespace, id) = self.rdf_graph.namespace_manager.compute_qname(node.identifier)
            ids_struct.append( id )

            self.classes_paths[node_uri]['all'].append(all_struct)
            self.classes_paths[node_uri]['labels'].append(labels_struct)
            self.classes_paths[node_uri]['ids'].append(ids_struct)

            #logger.debug("** %s **"%node_uri)
            #logger.debug( json.dumps(self.classes_paths[node_uri], indent=2) )

        return

    def parse_properties(self, rdf_node):
        logger.debug("parse_properties for rdf_node: {}".format(rdf_node))
        raw_properties = predicate_objects = list(self.rdf_graph.predicate_objects(subject=rdf_node))
        rdf_properties = {}
        logger.debug("raw_properties for rdf_node: {}".format(rdf_node))
        for index, property in enumerate(raw_properties):
            logger.debug("{}. {}".format(index, property))
            property_name = str(property[0])
            property_value = str(property[1])
            # rdf_properties[property_name].append(property_value)
            if property_name in rdf_properties:
                rdf_properties[property_name].append(property_value)
            else:
                rdf_properties[property_name] = [property_value]
        '''
        logger.debug("rdf_properties: {}".format(rdf_properties))
        for index, key in enumerate(rdf_properties):
            rdf_values = list(map(str, rdf_properties[key]))
            logger.debug("{}. k: {}, v: {}, {}".format(index, key, len(rdf_values), rdf_values))
        if 'http://www.ebi.ac.uk/efo/alternative_term' in rdf_properties:
            alternative_terms = rdf_properties['http://www.ebi.ac.uk/efo/alternative_term']
            logger.debug("alternative_terms: {}, {}".format(len(alternative_terms), alternative_terms))
        else:
            logger.debug("no alternative terms")
        if 'http://www.w3.org/2000/01/rdf-schema#label' in rdf_properties:
            label = rdf_properties['http://www.w3.org/2000/01/rdf-schema#label']
            logger.debug("label: {}, {}".format(len(label), label))
        else:
            logger.debug("no label")
        '''
        return rdf_properties

    def load_hpo_classes(self):
        """Loads the HPO graph and extracts the current and obsolete classes.
           Status: production
        """
        logger.debug("load_hpo_classes...")
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))
        base_class = 'http://purl.obolibrary.org/obo/HP_0000118'
        self.load_ontology_classes(base_class=base_class)
        self.get_deprecated_classes()
        self.get_top_levels(base_class= base_class)

    def load_mp_classes(self):
        """Loads the HPO graph and extracts the current and obsolete classes.
           Status: production
        """
        logger.debug("load_mp_classes...")
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'mp'))
        base_class = 'http://purl.obolibrary.org/obo/MP_0000001'
        self.load_ontology_classes(base_class= base_class)
        self.get_deprecated_classes()
        self.get_top_levels(base_class= base_class)

        #self.get_ontology_top_levels(base_class, top_level_map=self.phenotype_top_levels)


    def load_efo_classes(self):
        """Loads the EFO graph and extracts the current and obsolete classes.
           Status: production
        """
        logger.debug("load_efo_classes...")
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'efo'))
        # load disease, phenotype, measurement, biological process, function and injury and mental health
        for base_class in [ 'http://www.ebi.ac.uk/efo/EFO_0000408',
                            'http://www.ebi.ac.uk/efo/EFO_0000651',
                            'http://www.ebi.ac.uk/efo/EFO_0001444',
                            'http://purl.obolibrary.org/obo/GO_0008150',
                            'http://www.ifomis.org/bfo/1.1/snap#Function',
                            'http://www.ebi.ac.uk/efo/EFO_0000546',
                            'http://www.ebi.ac.uk/efo/EFO_0003935' ]:
            self.load_ontology_classes(base_class=base_class)
            self.get_top_levels(base_class=base_class)
        self.get_deprecated_classes()


    def load_open_targets_disease_ontology(self):
        """Loads the EFO graph and extracts the current and obsolete classes.
           Generates the therapeutic areas
           Creates the other disease class
           Move injury under other disease
           Status: production
        """
        logger.debug("load_open_targets_disease_ontology...")
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'efo'))
        all_ns = [n for n in self.rdf_graph.namespace_manager.namespaces()]

        self.get_deprecated_classes(obsoleted_in_version=True)

        '''
        get the original top_levels from EFO
        '''
        self.get_top_levels(base_class='http://www.ebi.ac.uk/efo/EFO_0000408')

        '''
        Detach the TAs from the disease node
        and load all the classes
        '''
        disease_uri = URIRef('http://www.ebi.ac.uk/efo/EFO_0000408')
        for base_class in EFO_TAS:
            uri = URIRef(base_class)
            self.rdf_graph.remove((uri, None, disease_uri))
            self.load_ontology_classes(base_class=base_class)
            self.get_classes_paths(root_uri=base_class, level=0)

        '''
        Create an other disease node
        '''
        cttv = Namespace(unicode("http://www.targetvalidation.org/disease"))

        # namespace_manager = NamespaceManager(self.rdf_graph)
        self.rdf_graph.namespace_manager.bind('cttv', cttv)

        '''
        Some diseases have no categories, let's create a category for them
        '''
        other_disease_uri = URIRef('http://www.targetvalidation.org/disease/other')
        self.rdf_graph.add((other_disease_uri, RDF.type, rdflib.term.URIRef(u'http://www.w3.org/2002/07/owl#Class')))
        self.rdf_graph.add([other_disease_uri, RDFS.label, rdflib.Literal('other disease')])

        '''
        Get all children of 'disease' and assign them to 'other disease'
        '''
        for c in self.rdf_graph.subjects(predicate=RDFS.subClassOf, object=disease_uri):
            self.rdf_graph.add([c, RDFS.subClassOf, other_disease_uri])

        '''
        Move 'injury' under 'other disease'
        injuries are treated with medication recorded in ChEMBL
        Move 'mental health' under 'other disease'
        '''
        self.rdf_graph.add([URIRef('http://www.ebi.ac.uk/efo/EFO_0000546'), RDFS.subClassOf, other_disease_uri])
        self.rdf_graph.add([URIRef('http://www.ebi.ac.uk/efo/EFO_0003935'), RDFS.subClassOf, other_disease_uri])


        # other disease, phenotype, measurement, biological process, function
        for base_class in [ 'http://www.targetvalidation.org/disease/other',
                            'http://www.ebi.ac.uk/efo/EFO_0000651',
                            'http://www.ebi.ac.uk/efo/EFO_0001444',
                            'http://purl.obolibrary.org/obo/GO_0008150',
                            'http://www.ifomis.org/bfo/1.1/snap#Function']:

            self.load_ontology_classes(base_class=base_class)
            self.get_classes_paths(root_uri=base_class, level=0)

    def load_human_phenotype_ontology(self):
        """
            Loads the HPO graph and extracts the current and obsolete classes.
            Status: production
        """
        logger.debug("load_human_phenotype_ontology...")
        #self.load_hpo_classes()
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))

        all_ns = [n for n in self.rdf_graph.namespace_manager.namespaces()]

        '''
        Detach the anatomical system from the phenotypic abnormality node
        and load all the classes
        '''

        phenotypic_abnormality_uri = 'http://purl.obolibrary.org/obo/HP_0000118'
        phenotypic_abnormality_uriref = URIRef(phenotypic_abnormality_uri)
        self.get_children(phenotypic_abnormality_uri)

        for child in self.children[phenotypic_abnormality_uri]:
            print "%s %s..."%(child['code'], child['label'])
            uri = "http://purl.obolibrary.org/obo/" + child['code']
            uriref = URIRef(uri)
            self.rdf_graph.remove((uriref, None, phenotypic_abnormality_uriref))
            self.load_ontology_classes(base_class=uri)
            self.get_classes_paths(root_uri=uri, level=0)
            self.get_deprecated_classes()
            print len(self.current_classes)

    def load_mammalian_phenotype_ontology(self):
        """
            Loads the MP graph and extracts the current and obsolete classes.
            Status: production
        """
        logger.debug("load_mammalian_phenotype_ontology...")
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'mp'))

        all_ns = [n for n in self.rdf_graph.namespace_manager.namespaces()]

        '''
        Detach the anatomical system from the mammalian phenotype node
        and load all the classes
        '''

        mp_root_uri = 'http://purl.obolibrary.org/obo/MP_0000001'
        mp_root_uriref = URIRef(mp_root_uri)
        self.get_children(mp_root_uri)

        for child in self.children[mp_root_uri]:
            print "%s %s..."%(child['code'], child['label'])
            uri = "http://purl.obolibrary.org/obo/" + child['code']
            uriref = URIRef(uri)
            self.rdf_graph.remove((uriref, None, mp_root_uriref))
            self.load_ontology_classes(base_class=uri)
            self.get_classes_paths(root_uri=uri, level=0)
            print len(self.current_classes)

    def load_efo_omim_xrefs(self):
        '''
        Load ontology xref from OMIM
        :return:
        '''
        sparql_query = '''
        PREFIX efo: <http://www.ebi.ac.uk/efo/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#> SELECT DISTINCT ?subject ?label ?omim WHERE {
            ?subject rdfs:subClassOf* efo:EFO_0000408 .
            ?subject rdfs:label ?label .
            ?subject efo:OMIM_definition_citation ?omim
        }
        '''

        OMIMmap = dict()

        qres = self.rdf_graph.query(sparql_query)

        for row in qres:
            uri = str(row[0])
            label = str(row[1])
            omimID = str(row[2])
            if omimID in OMIMmap and not uri in OMIMmap[omimID]:
                OMIMmap[omimID].append(uri)
            elif omimID not in OMIMmap:
                OMIMmap[omimID] = [ uri ]
        return OMIMmap

    def load_evidence_classes(self):
        '''
        Loads evidence from ECO, SO and the Open Targets evidence classes
        :return:
        '''
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'so'))
        self.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'eco'))

        evidence_uri = URIRef('http://purl.obolibrary.org/obo/ECO_0000000')

        # for triple in self.rdf_graph.triples((evidence_uri, None, None)):
        #      logger.debug(triple)

        '''
            Open Targets specific evidence:
            A) Create namespace for OT evidence
            B) Add Open Targets evidence terms to graph
            C) Traverse the graph breadth first
        '''

        cttv = Namespace(unicode("http://www.targetvalidation.org/disease"))
        ot = Namespace(unicode("http://identifiers.org/eco"))

        #namespace_manager = NamespaceManager(self.rdf_graph)
        self.rdf_graph.namespace_manager.bind('cttv', cttv)
        self.rdf_graph.namespace_manager.bind('ot',ot)

        open_targets_terms = {
            'http://www.targetvalidation.org/disease/cttv_evidence':'CTTV evidence',
            'http://identifiers.org/eco/drug_disease':'drug-disease evidence',
            'http://identifiers.org/eco/target_drug':'biological target to drug evidence',
            'http://identifiers.org/eco/clinvar_gene_assignments':'ClinVAR SNP-gene pipeline',
            'http://identifiers.org/eco/cttv_mapping_pipeline':'CTTV-custom annotation pipeline',
            'http://identifiers.org/eco/GWAS':'Genome-wide association study evidence',
            'http://identifiers.org/eco/GWAS_fine_mapping': 'Fine-mapping study evidence',
            'http://identifiers.org/eco/somatic_mutation_evidence':'Somatic mutation evidence',
            'http://www.targetvalidation.org/evidence/genomics_evidence':'genomics evidence',
            'http://targetvalidation.org/sequence/nearest_gene_five_prime_end':'Nearest gene counting from 5&apos; end',
            'http://targetvalidation.org/sequence/regulatory_nearest_gene_five_prime_end':'Nearest regulatory gene from 5&apos; end',
            'http://www.targetvalidation.org/evidence/literature_mining':'Literature mining',
            'http://www.targetvalidation.org/provenance/DatabaseProvenance':'database provenance',
            'http://www.targetvalidation.org/provenance/ExpertProvenance':'expert provenance',
            'http://www.targetvalidation.org/provenance/GWAS_SNP_to_trait_association':'GWAS SNP to trait association',
            'http://www.targetvalidation.org/provenance/LiteratureProvenance':'literature provenance',
            'http://www.targetvalidation.org/provenance/disease_to_phenotype_association':'disease to phenotype association',
            'http://www.targetvalidation.org/provenance/gene_to_disease_association':'gene to disease association'
        }

        for uri, label in open_targets_terms.iteritems():
            u = URIRef(uri)
            self.rdf_graph.add((u, RDF.type, rdflib.term.URIRef(u'http://www.w3.org/2002/07/owl#Class')))
            self.rdf_graph.add([u, RDFS.label, rdflib.Literal(label)])
            self.rdf_graph.add([u, RDFS.subClassOf, evidence_uri])

        u = URIRef('http://identifiers.org/eco/target_drug')
        # for triple in self.rdf_graph.triples((u, None, None)):
        #      logger.debug(triple)

        #(a, b, c) = self.rdf_graph.namespace_manager.compute_qname(unicode('http://identifiers.org/eco/target_drug'))
        #logger.debug(c)
        #return
        # Add the bits specific to Open Targets
        # 'Open Targets evidence' ?
        # 'biological target-disease association via drug' ECO:0000360
        # 'drug-disease evidence' http://identifiers.org/eco/drug_disease SubclassOf 'biological target-disease association via drug'
        # 'biological target to drug evidence' http://identifiers.org/eco/target_drug SubclassOf 'biological target-disease association via drug'
        # ClinVAR SNP-gene pipeline http://identifiers.org/eco/clinvar_gene_assignments SubclassOf ECO:0000246
        # CTTV-custom annotation pipeline http://identifiers.org/eco/cttv_mapping_pipeline SubclassOf ECO:0000246

        for base_class in ['http://purl.obolibrary.org/obo/ECO_0000000', 'http://purl.obolibrary.org/obo/SO_0000400', 'http://purl.obolibrary.org/obo/SO_0001260', 'http://purl.obolibrary.org/obo/SO_0000110', 'http://purl.obolibrary.org/obo/SO_0001060' ]:
            self.load_ontology_classes(base_class=base_class)
            self.get_classes_paths(root_uri=base_class, level=0)


class DiseaseUtils():

    def __init__(self):
        pass

    def get_disease_tas(self, ontologyreader=None, filename=None):

        # get all tas per
        fh = open(filename, 'w')
        for uri, label in ontologyreader.current_classes.items():
            if uri in ontologyreader.classes_paths and uri != "http://www.ebi.ac.uk/efo/EFO_0000408":
                tas = map(lambda x: x[1]['label'], ontologyreader.classes_paths[uri]['all'])
                fh.write("%s\t%s\t%s\n"%(uri, label, ",".join(set(tas))))
        fh.close()

    def check_disease_phenotypes_cache(self):
        return os.path.isfile(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'), "disease_phenotypes.pck"))

    def read_disease_phenotypes_cache(self):
        return pickle.load( open( os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'),"disease_phenotypes.pck"), 'rb'))

    def update_disease_phenotypes_cache(self, disease_phenotypes):
        if not os.path.isdir(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'))):
            os.makedirs(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir')))
        pickle.dump(disease_phenotypes,
            open(os.path.join(Config.ONTOLOGY_CONFIG.get('pickle', 'cache_dir'), "disease_phenotypes.pck"), 'wb'),
                    protocol=pickle.HIGHEST_PROTOCOL)

    def write_disease_phenotypes(self, ontologyreader=None, filename=None):

        pmap = self.get_disease_phenotypes(ontologyreader)

        fh = open(filename, 'w')
        for uri, ds in pmap.items():
            label = ds['label']
            phenotypes = map(lambda x: x['label'], ds['phenotypes'])
            fh.write("%s\t%s\t%s\n" % (uri, label, ",".join(set(phenotypes))))
        fh.close()
        '''
        for uri, label in self.diseases.current_classes.items():
            logger.debug("%s %s"%(uri, label))
            if uri in pmap:
                logger.debug("=> OK")
                tas = []
                phenotypes = map(lambda x: x['label'], map[uri])
                if uri in self.diseases.classes_paths and uri != "http://www.ebi.ac.uk/efo/EFO_0000408":
                    tas = map(lambda x: x[1]['label'], self.diseases.classes_paths[uri]['all'])
                fh.write("%s\t%s\t%s\t%s\n" % (uri, label, ",".join(set(phenotypes)), ",".join(set(tas))))
        '''

    def get_disease_phenotypes(self, ontologyclassreader=None):

        disease_phenotypes_map = {}

        if self.check_disease_phenotypes_cache():
            return self.read_disease_phenotypes_cache()

        # load HPO:
        logger.debug("Merge HPO graph")
        ontologyclassreader.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))
        logger.debug("Merge MP graph")
        ontologyclassreader.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'mp'))

        for key, uri in Config.ONTOLOGY_CONFIG.items('disease_phenotypes_uris'):
            logger.debug("merge phenotypes from %s" % uri)
            ontologyclassreader.rdf_graph.parse(uri, format='xml')

        qres = ontologyclassreader.rdf_graph.query(
            """
            PREFIX obo: <http://purl.obolibrary.org/obo/>
            PREFIX oban: <http://purl.org/oban/>
            select DISTINCT ?disease_id ?disease_label ?phenotype_id ?phenotype_label
            where {
              ?code oban:association_has_subject ?disease_id .
              ?disease_id rdfs:label ?disease_label .
              ?code oban:association_has_object ?phenotype_id .
              ?phenotype_id rdfs:label ?phenotype_label
            }
            """
        )

        '''
        Results are tuple of values
        '''
        for rdisease_uri, rdisease_label, rphenotype_uri, rphenotype_label in qres:
            (disease_uri, disease_label, phenotype_uri, phenotype_label) = (str(rdisease_uri), str(rdisease_label), str(rphenotype_uri), str(rphenotype_label))
            logger.debug("%s (%s) hasPhenotype %s (%s)" % (disease_uri, disease_label, phenotype_uri, phenotype_label))
            if disease_uri not in disease_phenotypes_map:
                disease_phenotypes_map[disease_uri] = { 'label': disease_label, 'phenotypes': [] }
            if phenotype_uri not in map(lambda x: x['uri'], disease_phenotypes_map[disease_uri]['phenotypes']):
                disease_phenotypes_map[disease_uri]['phenotypes'].append({'label': phenotype_label, 'uri': phenotype_uri})

        self.update_disease_phenotypes_cache(disease_phenotypes_map)

        return disease_phenotypes_map

class PhenotypeSlim():

    def __init__(self):

        self.rdf_graph = rdflib.Graph()
        self.phenotypes = None
        #self.efo = OntologyClassReader()
        #self.efo.load_efo_classes()

        self.phenotype_map = {}
        self.phenotype_excluded = set()

        self._remote_filenames = dict()
        self._logger = logging.getLogger(self.__class__.__name__)
        tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)


    def get_ontology_path(self, base_class, term):

        """ if the term is already there or if it is a disease term """
        if term in self.phenotype_map:
            return


        #if term == 'http://purl.obolibrary.org/obo/HP_0001251':
        if True:

            for sparql_query in [DIRECT_ANCESTORS, INDIRECT_ANCESTORS]:
                self.sparql.setQuery(sparql_query%(term, term))
                self.sparql.setReturnFormat(JSON)
                results = None
                n = 0
                while (n < 2):
                    try:
                        results = self.sparql.query().convert()
                        n = 3
                    except SPARQLWrapper.SPARQLExceptions.EndPointNotFound, e:
                        print e
                        self._logger.error(e)
                        if n > 2:
                            raise e
                        else:
                            n=n+1

                #print len(results)
                #print json.dumps(results)

                for result in results["results"]["bindings"]:
                    #print json.dumps(result)
                    count = int(result['distance']['value'])
                    parent_label = result['ancestor_label']['value']
                    ancestor = result['ancestor']['value']
                    direct_child = result['direct_child']['value']
                    direct_child_label = result['direct_child_label']['value']
                    ''' Add the term to the phenotype map '''
                    if direct_child not in self.phenotype_map:
                        self.phenotype_map[direct_child] = { 'label': direct_child_label , 'superclasses': [] }
                    ''' Put all the ancestors to the phenotype map '''
                    if ancestor not in self.phenotype_map[direct_child]['superclasses']:
                        self.phenotype_map[direct_child]['superclasses'].append(ancestor)
                        print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)
                        print "---------"
                    #print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)


    def load_all_phenotypes(self):
        '''
        Load HPO and MP to accept phenotype terms that are not in EFO
        :return:
        '''
        self.phenotypes = OntologyClassReader()
        self.phenotypes.load_hpo_classes()
        self.phenotypes.get_classes_paths(root_uri='http://purl.obolibrary.org/obo/HP_0000118')
        self.phenotypes.load_mp_classes()
        self.phenotypes.get_classes_paths(root_uri='http://purl.obolibrary.org/obo/MP_0000001')

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

    def _store_remote_filename(self, filename):
        # print "%s" % filename
        self._logger.debug("%s" % filename)
        if filename.startswith('/upload/submissions/') and \
            filename.endswith('.json.gz'):
            self._logger.debug("%s" % filename)
            if True:
                version_name = filename.split('/')[3].split('.')[0]
                # print "%s" % filename
                if '-' in version_name:
                    user, day, month, year = version_name.split('-')
                    if '_' in user:
                        datasource = ''.join(user.split('_')[1:])
                        user = user.split('_')[0]
                    else:
                        datasource = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[user]
                    release_date = date(int(year), int(month), int(day))

                    if user not in self._remote_filenames:
                        self._remote_filenames[user]={datasource : dict(date = release_date,
                                                                          file_path = filename,
                                                                          file_version = version_name)
                                                      }
                    elif datasource not in self._remote_filenames[user]:
                        self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                        file_path=filename,
                                                                        file_version=version_name)
                    else:
                        if release_date > self._remote_filenames[user][datasource]['date']:
                            self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                file_path=filename,
                                                                file_version=version_name)
            #except e:
            #    self.logger.error("%s Error checking file %s: %s" % (self.__class__.__name__, filename, e))
            #    print 'error getting remote file%s'%filename
            #    self.logger.debug('error getting remote file%s'%filename)

    def _callback_not_used(self, path):
        self._logger.debug("skipped " + path)

    def create_phenotype_slim_from_selection(self):

        for url in Config.PHENOTYPE_SLIM_INPUT_URLS:
            response = requests.get(url)
            self._logger.info("Read url %s - response code %s" % (url, response.code))
            lines = response.readlines()

            for line in lines:
                self._logger.info(line.rstrip())

    def create_phenotype_slim_from_evidence(self, local_files = []):

        self.load_all_phenotypes()

        if local_files:

            for file_path in local_files:
                self._logger.info("Parsing file %s" % (file_path))
                file_size, file_mod_time = os.path.getsize(file_path), os.path.getmtime(file_path)
                with open(file_path, mode='rb') as f:
                    self.parse_gzipfile(filename=file_path, mode='rb', fileobj=f, mtime=file_mod_time)
        else:

            for u in tqdm(Config.ONTOLOGY_PREPROCESSING_FTP_ACCOUNTS,
                             desc='scanning ftp accounts',
                             # file=tqdm_out,
                             leave=False):
                try:
                    p = Config.EVIDENCEVALIDATION_FTP_ACCOUNTS[u]
                    self._logger.info("%s %s" % (u, p))
                    cnopts = pysftp.CnOpts()
                    cnopts.hostkeys = None  # disable host key checking.
                    with pysftp.Connection(host=Config.EVIDENCEVALIDATION_FTP_HOST['host'],
                                           port=Config.EVIDENCEVALIDATION_FTP_HOST['port'],
                                           username=u,
                                           password=p,
                                           cnopts = cnopts,
                                           ) as srv:
                        srv.walktree('/', fcallback=self._store_remote_filename, dcallback=self._callback_not_used, ucallback=self._callback_not_used)
                        srv.close()
                        for datasource, file_data in tqdm(self._remote_filenames[u].items(),
                                                          desc='scanning available datasource for account %s'%u,
                                                          # file=tqdm_out,
                                                          leave=False,):
                            latest_file = file_data['file_path']
                            file_version = file_data['file_version']
                            self._logger.info("found latest file %s for datasource %s" % (latest_file, datasource))
                            self.parse_gzipfile(latest_file, u, p)
                except AuthenticationException:
                    self._logger.error('cannot connect with credentials: user:%s password:%s' % (u, p))

        for uri, p in self.phenotype_map.iteritems():
            logger.debug(uri)
            logger.debug(json.dumps(p, indent=2))

    def parse_gzipfile(self, filename, mode, fileobj, mtime):

        with gzip.GzipFile(filename=filename,
                           mode=mode,
                           fileobj=fileobj,
                           mtime=mtime) as fh:

            self._logger.info('Starting parsing %s' % filename)

            line_buffer = []
            offset = 0
            chunk = 1
            line_number = 0

            for line in fh:
                python_raw = json.loads(line)
                obj = None
                data_type = python_raw['type']
                if data_type in Config.EVIDENCEVALIDATION_DATATYPES:
                    if data_type == 'genetic_association':
                        obj = opentargets.Genetics.fromMap(python_raw)
                    elif data_type == 'rna_expression':
                        obj = opentargets.Expression.fromMap(python_raw)
                    elif data_type in ['genetic_literature', 'affected_pathway', 'somatic_mutation']:
                        obj = opentargets.Literature_Curated.fromMap(python_raw)
                        if data_type == 'somatic_mutation' and not isinstance(python_raw['evidence']['known_mutations'],
                                                                              list):
                            mutations = copy.deepcopy(python_raw['evidence']['known_mutations'])
                            python_raw['evidence']['known_mutations'] = [mutations]
                            # self.logger.error(json.dumps(python_raw['evidence']['known_mutations'], indent=4))
                            obj = opentargets.Literature_Curated.fromMap(python_raw)
                    elif data_type == 'known_drug':
                        obj = opentargets.Drug.fromMap(python_raw)
                    elif data_type == 'literature':
                        obj = opentargets.Literature_Mining.fromMap(python_raw)
                    elif data_type == 'animal_model':
                        obj = opentargets.Animal_Models.fromMap(python_raw)

                if obj.disease.id:
                    for id in obj.disease.id:
                        if re.match('http://purl.obolibrary.org/obo/HP_\d+', id) or re.match('http://purl.obolibrary.org/obo/MP_\d+', id):
                            ''' get all terms '''
                            self.phenotype_map[id] = self.phenotypes.classes_paths[id]

        fh.close()

    def parse_sftp_gzipfile(self, file_path, u, p):
        # print "---->%s"%file_path
        self._logger.info("%s %s" % (u, p))
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # disable host key checking.
        with pysftp.Connection(host=Config.EVIDENCEVALIDATION_FTP_HOST['host'],
                               port=Config.EVIDENCEVALIDATION_FTP_HOST['port'],
                               username=u,
                               password=p,
                               cnopts=cnopts,
                               ) as srv:

            file_stat = srv.stat(file_path)
            file_size, file_mod_time = file_stat.st_size, file_stat.st_mtime

            with srv.open(file_path, mode='rb', bufsize=1) as f:
                self.parse_gzipfile(filename=file_path.split('/')[1], mode='rb', filepbj=f, mtime=file_mod_time)
            srv.close()
        return

def main():
    #obj = DiseasePhenotypes()
    #obj.parse_owl_url()

    obj = PhenotypeSlim()
    #obj.load_all_phenotypes()
    obj.create_phenotype_slim(local_files=['/Users/koscieln/Documents/work/gitlab/data_pipeline/samples/cttv008-22-07-2016.json.gz'])


if __name__ == "__main__":
    main()
