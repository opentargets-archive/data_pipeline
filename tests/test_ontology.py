'''
Copyright 2014-2016 EMBL - European Bioinformatics Institute, Wellcome
Trust Sanger Institute, GlaxoSmithKline and Biogen

This software was developed as part of Open Targets. For more information please see:

	http://targetvalidation.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from __future__ import absolute_import, print_function
from nose.tools.nontrivial import with_setup
from modules.Ontology import OntologyClassReader, PhenotypeSlim, DiseaseUtils, EFO_TAS
from rdflib import URIRef
from SPARQLWrapper import SPARQLWrapper, JSON
from settings import Config
import logging
import os
from logging.config import fileConfig

__author__ = "Gautier Koscielny"
__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

try:
    fileConfig(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../logging_config.ini'))
except:
    pass
logger = logging.getLogger(__name__)

def setup_module(module):
    logger.info("Setting up the ontology tests")

def teardown_module(module):
    logger.info("Tearing down the ontology tests")

def my_setup_function():
    print("my_setup_function")

def my_teardown_function():
    print("my_teardown_function")


@with_setup(my_setup_function, my_teardown_function)
def test_hpo_load():
    obj = OntologyClassReader()
    assert not obj == None
    obj.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))

@with_setup(my_setup_function, my_teardown_function)
def test_mp_load():
    obj = OntologyClassReader()
    assert not obj == None
    obj.load_ontology_graph(Config.ONTOLOGY_CONFIG.get('uris', 'mp'))

@with_setup(my_setup_function, my_teardown_function)
def test_load_hpo_classes():

    obj = OntologyClassReader()
    assert not obj == None
    obj.load_hpo_classes()

    logger.info(len(obj.current_classes))

    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0008074"] == "Metatarsal periosteal thickening"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0000924"] == "Abnormality of the skeletal system"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0002715"] == "Abnormality of the immune system"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0000118"] == "Phenotypic abnormality"

    # Display obsolete terms
    logger.info(len(obj.obsolete_classes))
    for k,v in obj.obsolete_classes.iteritems():
        logger.info("%s => %s "%(k, v))
        assert obj.obsolete_classes[k] == v

    # check the top_level classes
    logger.info(len(obj.top_level_classes))
    assert len(obj.top_level_classes) > 0
    assert 'http://purl.obolibrary.org/obo/HP_0001871' in obj.top_level_classes
    assert 'http://purl.obolibrary.org/obo/HP_0003549' in obj.top_level_classes
    assert 'http://purl.obolibrary.org/obo/HP_0000152' in obj.top_level_classes
    assert 'http://purl.obolibrary.org/obo/HP_0040064' in obj.top_level_classes


@with_setup(my_setup_function, my_teardown_function)
def test_load_mp_classes():

    obj = OntologyClassReader()
    assert not obj == None
    obj.load_mp_classes()

    logger.info(len(obj.current_classes))

    assert obj.current_classes["http://purl.obolibrary.org/obo/MP_0005559"] == "increased circulating glucose level"
    assert obj.current_classes["http://purl.obolibrary.org/obo/MP_0005376"] == "homeostasis/metabolism phenotype"
    assert obj.current_classes["http://purl.obolibrary.org/obo/MP_0003631"] == "nervous system phenotype"
    assert obj.current_classes["http://purl.obolibrary.org/obo/MP_0005370"] == "liver/biliary system phenotype"

    # Display obsolete terms
    logger.info(len(obj.obsolete_classes))
    for k,v in obj.obsolete_classes.iteritems():
        logger.info("%s => %s "%(k, v))
        assert obj.obsolete_classes[k] == v

@with_setup(my_setup_function, my_teardown_function)
def test_load_efo_classes():

    obj = OntologyClassReader()
    assert not obj == None
    obj.load_efo_classes()

    logger.info("Number of current classes: %i"%len(obj.current_classes))

    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0000408"] == "disease"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0000651"] == "phenotype"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0001444"] == "measurement"
    assert obj.current_classes["http://purl.obolibrary.org/obo/GO_0008150"] == "biological process"

    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0004566"] == "body weight gain"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0004329"] == "alcohol drinking"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0000544"] == "infection"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0000546"] == "injury"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_1001050"] == "multiple system atrophy"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0002950"] == "pregnancy"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0003015"] == "aggressive behavior"
    assert obj.current_classes["http://purl.obolibrary.org/obo/GO_0030431"] == "sleep"
    assert obj.current_classes["http://purl.obolibrary.org/obo/GO_0042493"] == "response to drug"

    # Display obsolete terms
    logger.info(len(obj.obsolete_classes))
    for k,v in obj.obsolete_classes.iteritems():
        logger.info("%s => %s "%(k, v))
        assert obj.obsolete_classes[k] == v

@with_setup(my_setup_function, my_teardown_function)
def test_load_open_targets_disease_ontology():

    obj = OntologyClassReader()
    assert not obj == None
    obj.load_open_targets_disease_ontology()

    logger.info(len(obj.current_classes))

    '''
    all top levels
    '''
    assert obj.current_classes["http://www.targetvalidation.org/disease/other"] == "other disease"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0000651"] == "phenotype"
    assert obj.current_classes["http://www.ebi.ac.uk/efo/EFO_0001444"] == "measurement"
    assert obj.current_classes["http://purl.obolibrary.org/obo/GO_0008150"] == "biological process"
    assert obj.current_classes['http://www.ifomis.org/bfo/1.1/snap#Function'] == "function"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_1000018'] == "bladder disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000319'] == "cardiovascular disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000405'] == "digestive system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0001379'] == "endocrine system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0003966'] == "eye disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000508'] == "genetic disorder"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000524'] == "head disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0005803'] == "hematological system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000540'] == "immune system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0003086'] == "kidney disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0005741'] == "infectious disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000589'] == "metabolic disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000616'] == "neoplasm"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000618'] == "nervous system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000512'] == "reproductive system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000684'] == "respiratory system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0002461'] == "skeletal system disease"
    assert obj.current_classes['http://www.ebi.ac.uk/efo/EFO_0000701'] == "skin disease"
    assert "http://www.ebi.ac.uk/efo/EFO_0000408" not in obj.current_classes

    '''
    test that all top levels have no ancestors
    '''
    for uri in EFO_TAS:
        for all_path in obj.classes_paths[uri]['all']:
            assert len(all_path) == 1
            assert all_path[0]['uri'] == uri
    for uri in [ 'http://www.targetvalidation.org/disease/other',
                            'http://www.ebi.ac.uk/efo/EFO_0000651',
                            'http://www.ebi.ac.uk/efo/EFO_0001444',
                            'http://purl.obolibrary.org/obo/GO_0008150',
                            'http://www.ifomis.org/bfo/1.1/snap#Function' ]:
        for all_path in obj.classes_paths[uri]['all']:
            assert len(all_path) == 1
            assert all_path[0]['uri'] == uri

    #for uri, label in obj.current_classes.items():
    #    properties = obj.parse_properties(URIRef(uri))
    #    definition = ''
    #    if 'http://www.ebi.ac.uk/efo/definition' in properties:
    #        definition = ". ".join(properties['http://www.ebi.ac.uk/efo/definition'])
    #    synonyms = []
    #    if 'http://www.ebi.ac.uk/efo/alternative_term' in properties:
    #        synonyms = properties['http://www.ebi.ac.uk/efo/alternative_term']
    #    logger.debug("URI: %s, label:%s, definition:%s, synonyms:%s"%(uri, label, definition, "; ".join(synonyms)))

    #vs = "hello"
    #print("%i" % vs)

@with_setup(my_setup_function, my_teardown_function)
def test_parse_properties():
    obj = OntologyClassReader()
    assert not obj == None
    obj.load_hpo_classes()
    obj.parse_properties(URIRef('http://purl.obolibrary.org/obo/HP_0040064'))
    vs = "hello"
    print ("%i"%vs)

@with_setup(my_setup_function, my_teardown_function)
def test_get_disease_phenotypes():
    obj = OntologyClassReader()
    assert obj is not None
    obj.load_efo_classes()
    utils = DiseaseUtils()
    assert utils is not None
    disease_phenotypes = utils.get_disease_phenotypes(ontologyclassreader=obj)

    logger.debug("Found %i diseases with associated phenotypes"%len(disease_phenotypes))

    for uri in [
        'http://www.ebi.ac.uk/efo/EFO_0000398',
        'http://www.ebi.ac.uk/efo/EFO_0000706',
        'http://www.ebi.ac.uk/efo/EFO_0004719',
        'http://www.ebi.ac.uk/efo/EFO_0000384',
        'http://www.ebi.ac.uk/efo/EFO_0005140',
        'http://www.orpha.net/ORDO/Orphanet_1812'
    ]:
        logger.debug("check %s contains phenotypes"%uri)
        assert uri in disease_phenotypes
        assert uri in obj.current_classes

@with_setup(my_setup_function, my_teardown_function)
def test_get_disease_tas():
    obj = DiseaseUtils()
    assert not obj == None
    obj.get_disease_tas(filename="/Users/koscieln/.ontologycache/efo/disease_tas.txt")


@with_setup(my_setup_function, my_teardown_function)
def test_parse_local_files():
    sparql = SPARQLWrapper(Config.SPARQL_ENDPOINT_URL)
    assert not sparql == None
    obj = PhenotypeSlim(sparql)
    assert not obj == None
    local_files = [ os.path.join(os.path.abspath(os.path.dirname(__file__)), '../samples/cttv008-22-07-2016.json.gz') ]
    obj.create_phenotype_slim(local_files=local_files)

