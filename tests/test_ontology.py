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
from modules.Ontology import OntologyClassReader, PhenotypeSlim
from SPARQLWrapper import SPARQLWrapper, JSON
from settings import Config
import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
import datetime

__author__ = "Gautier Koscielny"
__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@targetvalidation.org"
__status__ = "Production"


def setup_module(module):
    print("")  # this is to get a newline after the dots
    print("setup_module before anything in this file")

def teardown_module(module):
    print("teardown_module after everything in this file")

def my_setup_function():
    print("my_setup_function")

def my_teardown_function():
    print("my_teardown_function")


@with_setup(my_setup_function, my_teardown_function)
def test_hpo_load():
    obj = OntologyClassReader()
    assert not obj == None
    obj.get_ontology_classes(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))

@with_setup(my_setup_function, my_teardown_function)
def test_mp_load():
    obj = OntologyClassReader()
    assert not obj == None
    obj.get_ontology_classes(Config.ONTOLOGY_CONFIG.get('uris', 'mp'))

@with_setup(my_setup_function, my_teardown_function)
def test_hpo_load_classes():

    obj = OntologyClassReader()
    assert not obj == None
    obj.get_ontology_classes(Config.ONTOLOGY_CONFIG.get('uris', 'hpo'))
    base_class = 'http://purl.obolibrary.org/obo/HP_0000118'
    obj.load_ontology(base_class=base_class)
    logger.info(len(obj.current_classes))
    count = 0
    for k,v in obj.current_classes.iteritems():
        logger.info("%s => %s "%(k, v))
        assert obj.current_classes[k] == v
        count +=1
        if (count > 20):
            break
    #logger.info(obj.current_classes['http://purl.obolibrary.org/obo/HP_0001347'])
    #assert obj.current_classes['http://purl.obolibrary.org/obo/HP_0001347'] == 'Hyperreflexia"
    logger.info("'%s'"%obj.current_classes["http://purl.obolibrary.org/obo/HP_0008074"] )
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0008074"] == "Metatarsal periosteal thickening"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0000924"] == "Abnormality of the skeletal system"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0002715"] == "Abnormality of the immune system"
    assert obj.current_classes["http://purl.obolibrary.org/obo/HP_0000118"] == "Phenotypic abnormality"

@with_setup(my_setup_function, my_teardown_function)
def test_parse_local_files():
    sparql = SPARQLWrapper(Config.SPARQL_ENDPOINT_URL)
    assert not sparql == None
    obj = PhenotypeSlim(sparql)
    assert not obj == None
    local_files = [ os.path.join(os.path.abspath(os.path.dirname(__file__)), '../samples/cttv008-22-07-2016.json.gz') ]
    obj.create_phenotype_slim(local_files=local_files)