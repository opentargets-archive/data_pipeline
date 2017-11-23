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
from mrtarget.modules.EFO import EfoProcess
from mrtarget.modules.Ontology import OntologyClassReader
from mrtarget.Settings import Config
import logging
import os
import json
from logging.config import fileConfig


__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"


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
def test_efo_load():
    efo_process = EfoProcess(loader=None)
    assert efo_process is not None
    efo_process._process_ontology_data()

    for id in [
        'EFO_0000398',
        'EFO_0000706',
        'EFO_0004719',
        'EFO_0000384',
        'EFO_0005140',
        'Orphanet_1812'
    ]:
        logger.debug("check %s contains phenotypes"%id)
        assert id in efo_process.efos
        assert len(efo_process.efos[id].phenotypes) > 0
