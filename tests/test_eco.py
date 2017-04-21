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
from modules.ECO import EcoProcess
from modules.Ontology import OntologyClassReader
from settings import Config
import logging
import os
import json
from logging.config import fileConfig

__author__ = "Gautier Koscielny"
__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

from logging.config import fileConfig

logger = logging.getLogger(__name__)

def setup_module(module):
    logger.info("Setting up the ontology tests")
    logger.info(__name__)

def teardown_module(module):
    logger.info("Tearing down the ontology tests")

def my_setup_function():
    logger.info("my_setup_function")

def my_teardown_function():
    logger.info("my_teardown_function")


@with_setup(my_setup_function, my_teardown_function)
def test_eco_load():
    eco_process = EcoProcess(loader=None)
    assert not eco_process == None
    eco_process._process_ontology_data()
    obj = eco_process.evidence_ontology
    assert not obj == None
    logger.info(len(obj.current_classes))

    assert obj.current_classes["http://purl.obolibrary.org/obo/SO_0001549"] == "transcription_variant"
    assert obj.current_classes["http://purl.obolibrary.org/obo/ECO_0000205"] == "curator inference"
    assert obj.current_classes["http://purl.obolibrary.org/obo/ECO_0000084"] == "gene neighbors evidence"
    assert obj.current_classes["http://purl.obolibrary.org/obo/SO_0002053"] == "gain_of_function_variant"
    assert obj.current_classes["http://purl.obolibrary.org/obo/ECO_0000177"] == "genomic context evidence"

    logger.error("Show classes paths")
    for node, value in obj.classes_paths.iteritems():
        logger.error(node)
        logger.error(json.dumps(value, indent=2))

    print(__name__)
