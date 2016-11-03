"""
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

.. module:: Downloader
    :platform: Unix, Linux
    :synopsis: A data pipeline module to download data.
.. moduleauthor:: Gautier Koscielny <gautierk@opentargets.org>
"""

import re
import os
import json
import logging
import requests
from lxml.etree import tostring
from xml.etree import cElementTree as ElementTree
from common.Downloader import Downloader
from datetime import datetime
from settings import Config

__copyright__ = "Copyright 2014-2016, GlaxoSmithKline"

__credits__ = ["Gautier Koscielny", "Rita Santos"]
__license__ = "Apache 2.0"
__version__ = "1.2.2"
__maintainer__ = "Gautier Koscielny"
__email__ = "gautier.x.koscielny@gsk.com"
__status__ = "Production"

from logging.config import fileConfig

try:
    fileConfig(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../logging_config.ini'))
except:
    pass
logger = logging.getLogger(__name__)

class ChEMBLLookup(Downloader):
    def __init__(self):
        super(ChEMBLLookup, self).__init__()
        self.protein_class = dict()
        self.target_component = dict()

    def download(self):
        now = datetime.utcnow()
        today = datetime.strptime("{:%Y-%m-%d}".format(datetime.now()), '%Y-%m-%d')

        for label, url in Config.CHEMBL_URIS.iteritems():
            logging.debug(url)

            start = 0
            rows = 100
            nbItems = 0
            counter = 0
            total = 1

            while (start < total):

                counter+=1
                #print start
                uri = '%s?limit=%i&offset=%i' %(url,rows,start)
                logging.info("REQUEST {0}. {1}".format(counter, uri))
                raw = self.get_response(uri, directory=None, filename=None)
                print raw
                root = ElementTree.fromstring(raw)
                print root.tag, root.attrib
                #response><page_meta><limit>40</limit><next>/chembl/api/data/target_component?limit=40&offset=60</next><offset>20</offset><previous/><total_count>7234</total_count>
                for elem in root.iterfind('page_meta/total_count'):
                    total = int(elem.text)
                print total
                for child_of_root in root:
                    print child_of_root.tag, child_of_root.attrib

                if label == 'protein_class':

                    '''
                    <protein_class>
                        <l1>Enzyme</l1>
                        <l2>Kinase</l2>
                        <l3>Protein Kinase</l3>
                        <l4>Other protein kinase group</l4>
                        <l5>Other protein kinase Wnk family</l5>
                        <l6/>
                        <l7/>
                        <l8/>
                        <protein_class_id>260</protein_class_id>
                    </protein_class>
                    '''
                    for elem in root.iterfind('protein_classes/protein_class'):
                        #print elem.tag, elem.attrib
                        local_dict = dict()
                        for child_of_root in elem:
                            #print child_of_root.tag, child_of_root.attrib, child_of_root.text
                            local_dict[child_of_root.tag] = child_of_root.text
                        self.protein_class[local_dict['protein_class_id']] = local_dict
                        #print json.dumps(self.protein_class[local_dict['protein_class_id']], indent=4)
                    #break

                elif label == 'target_component':

                    '''
                    <target_component>
                        <accession>P24385</accession>
                        <component_id>34</component_id>
                        <component_type>PROTEIN</component_type>
                        <description>G1/S-specific cyclin-D1</description>
                        <go_slims></go_slims>
                        <organism>Homo sapiens</organism>
                        <protein_classifications>
                            <protein_classification>
                                <protein_classification_id>8</protein_classification_id>
                            </protein_classification>
                        </protein_classifications>
                    '''
                    for elem in root.iterfind('target_components/target_component'):

                        # check the species first
                        organism = None
                        for organism_elem in elem.iterfind('organism'):
                            organism  = organism_elem.text
                        if organism == 'Homo sapiens':
                            protein_classes = []
                            accession = None
                            for accession_elem in elem.iterfind('accession'):
                                accession = accession_elem.text
                            for protein_class_elem in elem.iterfind('protein_classifications/protein_classification/protein_classification_id'):
                                id = protein_class_elem.text
                                protein_classes.append(id)
                            self.target_component[accession] = protein_classes
                            print "%s: %s"%(accession, ",".join(protein_classes))
                    #break
                # validating therapeutic targets thorugh human genetics Robert Plenge, Edwasrd Scolnick, David Altshuler
                # RNA splicing is a primary link between genetic variation
                # focus on molecu;ar tractability

                start+=rows

    def get_target_classes_from_uniprot_protein_id(self, uniprot_protein_id):
        protein_classes = set()
        if uniprot_protein_id in self.target_component:
            for class_id in self.target_component[uniprot_protein_id]:
                if self.protein_class[class_id]['l7'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l7'])
                    protein_classes.add(self.protein_class[class_id]['l7'])
                elif self.protein_class[class_id]['l6'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l6'])
                    protein_classes.add(self.protein_class[class_id]['l6'])
                elif self.protein_class[class_id]['l5'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l5'])
                    protein_classes.add(self.protein_class[class_id]['l5'])
                elif self.protein_class[class_id]['l4'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l4'])
                    protein_classes.add(self.protein_class[class_id]['l4'])
                elif self.protein_class[class_id]['l3'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l3'])
                    protein_classes.add(self.protein_class[class_id]['l3'])
                elif self.protein_class[class_id]['l2'] is not None:
                    logging.debug("Adding %s"%self.protein_class[class_id]['l2'])
                    protein_classes.add(self.protein_class[class_id]['l2'])
                else:
                    protein_classes.add(self.protein_class[class_id]['l1'])
                    logging.info("Adding %s"%self.protein_class[class_id]['l1'])
        return protein_classes

    def get_target_classes_from_uniprot_protein_ids(self, uniprot_protein_ids):
        protein_classes = set()
        for uniprot_protein_id in uniprot_protein_ids:
            if len(protein_classes) == 0:
                protein_classes = self.get_target_classes_from_uniprot_protein_id(uniprot_protein_id)
            else:
                protein_classes.union(self.get_target_classes_from_uniprot_protein_id(uniprot_protein_id))
            logging.debug("Now having %s"%",".join(protein_classes))
        return protein_classes

def main():

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    logging.info("Load ChEMBL protein class data")
    chembl = ChEMBLLookup()
    chembl.download()

if __name__ == "__main__":
    main()