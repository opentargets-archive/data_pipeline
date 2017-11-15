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

import logging
import os

import requests

from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config
import mrtarget.common as c
import json

__copyright__ = "Copyright 2014-2016, GlaxoSmithKline"

__credits__ = ["Gautier Koscielny"]
__license__ = "Apache 2.0"
__version__ = "1.2.2"
__maintainer__ = "Gautier Koscielny"
__email__ = "gautier.x.koscielny@gsk.com"
__status__ = "Production"


def get_chembl_url(uri):
    '''return to json from uri'''
    next_get = True
    limit = 1000000
    offset = 0

    def _fmt(**kwargs):
        '''generate uri string params from kwargs dict'''
        l = ['='.join([k, str(v)]) for k, v in kwargs.iteritems()]
        return '?' + '&'.join(l)

    while next_get:
        chunk = None
        with c.URLZSource(uri + _fmt(limit=limit, offset=offset)).open() as f:
            chunk = json.loads(f.read())

        page_meta = chunk['page_meta']
        data_key = list(set(chunk.keys()) - set(['page_meta']))[0]

        if 'next' in page_meta and page_meta['next'] is not None:
            limit = page_meta['limit']
            offset += limit
        else:
            next_get = False

        for el in chunk[data_key]:
            yield el


class ChEMBLLookup(object):
    def __init__(self):
        super(ChEMBLLookup, self).__init__()
        self._logger = logging.getLogger(__name__)
        self.es_query = ESQuery()
        self.protein_class = dict()
        self.target_component = dict()
        self.mechanisms = {}
        self.target2molecule = {}
        self.disease2molecule = {}
        # TODO XXX here I need to enable this class
        # self.download = Downloader()
        self.targets = {}
        self.uni2chembl = {}
        self.molecule2synonyms = {}
        self.protein_classification = {}
        self.protein_class = {}
        self.protein_class_label_to_id = {}

    def download_targets(self):
        '''fetches all the targets from chembl and store their data and a mapping to uniprot id'''
        self._logger.info('ChEMBL getting targets from ' +
                          Config.CHEMBL_TARGET_BY_UNIPROT_ID)

        targets = get_chembl_url(Config.CHEMBL_TARGET_BY_UNIPROT_ID)
        for i in targets:
            if 'target_components' in i and \
                    i['target_components'] and \
                            'accession' in i['target_components'][0] and \
                    i['target_components'][0]['accession']:
                uniprot_id = i['target_components'][0]['accession']
                self.targets[uniprot_id] = i
                self.uni2chembl[uniprot_id] = i['target_chembl_id']

    def download_mechanisms(self):
        '''fetches mechanism data and stores which molecules are
        linked to any target'''

        mechanisms = get_chembl_url(Config.CHEMBL_MECHANISM)
        allowed_target_chembl_ids = set(self.uni2chembl.values())
        for i in mechanisms:
            self.mechanisms[i['record_id']] = i
            target_id = i['target_chembl_id']
            if target_id in allowed_target_chembl_ids:
                if target_id not in self.target2molecule:
                    self.target2molecule[target_id] = set()
                self.target2molecule[target_id].add(i['molecule_chembl_id'])

    def download_molecules_linked_to_target(self):
        '''generate a dictionary with all the synonyms known for a given molecules.
         Only retrieves molecules linked to a target'''
        self._logger.info('chembl downloading molecules linked to target')
        if not self.targets:
            self._logger.debug('chembl downloading targets')
            self.download_targets()
        if not self.target2molecule:
            self._logger.debug('chembl downloading mechanisms')
            self.download_mechanisms()
        required_molecules = set()
        self._logger.info('chembl t2m mols')
        for molecules in self.target2molecule.values():
            for molecule in molecules:
                required_molecules.add(molecule)
        required_molecules = list(required_molecules)
        batch_size = 100
        self._logger.debug('chembl populate synonyms')
        for i in range(0, len(required_molecules) + 1, batch_size):
            self._populate_synonyms_for_molecule(required_molecules[i:i + batch_size])

    def download_protein_classification(self):
        '''fetches targets components from chembls and inject the target class data in self.protein_classification'''
        self.download_protein_class()
        targets_components = get_chembl_url(Config.CHEMBL_TARGET_COMPONENT)
        for i in targets_components:
            if 'accession' in i:
                if i['accession'] not in self.protein_classification:
                    self.protein_classification[i['accession']] = []
                for classification in i['protein_classifications']:
                    protein_class_id = classification['protein_classification_id']
                    self.protein_classification[i['accession']].append(self.protein_class[protein_class_id])

    def download_protein_class(self):
        '''
        Fetches target classes from chembl and stores it in self.protein_class
        :return:
        '''
        protein_classes = get_chembl_url(Config.CHEMBL_PROTEIN_CLASS)
        for i in protein_classes:
            protein_class_id = i.pop('protein_class_id')
            protein_class_data = dict((k, dict(label=v, id='')) for k, v in i.items() if v)  # remove values with none
            self.protein_class[protein_class_id] = protein_class_data
            self._store_label_to_protein_class(protein_class_id, protein_class_data)

        '''inject missing ids'''
        for k, v in self.protein_class.items():
            for level, data in v.items():
                label = data['label']
                if label in self.protein_class_label_to_id:
                    data['id'] = self.protein_class_label_to_id[label]

    def _store_label_to_protein_class(self, protein_class_id, protein_class_data):
        max_level = 0
        label = ''
        for k, v in protein_class_data.items():
            level = int(k[1])
            if level >= max_level:
                max_level = level
                label = v['label']
        self.protein_class_label_to_id[label] = protein_class_id

    def get_molecules_from_evidence(self):
        self._logger.debug('get_molecules_from_evidence')
        for e in self.es_query.get_all_evidence_for_datasource(['chembl'],
                                                               fields=['target.id',
                                                                       'disease.id',
                                                                       'evidence.target2drug.urls']):
            self._logger.info('retrieving ChEMBL evidence...')
            molecule_ids = [i['url'].split('/')[-1] for i in e['evidence']['target2drug']['urls'] if
                           '/compound/' in i['url']]
            if molecule_ids:
                molecule_id=molecule_ids[0]
                self._logger.info(molecule_id)
                print('retrieving ChEMBL evidence... %s' % molecule_id)
                disease_id = e['disease']['id']
                target_id = e['target']['id']
                if disease_id not in self.disease2molecule:
                    self.disease2molecule[disease_id]=set()
                self.disease2molecule[disease_id].add(molecule_id)
                if target_id not in self.target2molecule:
                    self.target2molecule[target_id]=set()
                self.target2molecule[target_id].add(molecule_id)

    def _populate_synonyms_for_molecule(self, molecule_set):
        url = Config.CHEMBL_MOLECULE_SET.format(';'.join(molecule_set))
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        for i in data['molecules']:
            if 'molecule_synonyms' in i and i['molecule_synonyms']:
                synonyms = []
                for syn in i['molecule_synonyms']:
                    synonyms.append(syn['synonyms'])
                    synonyms.append(syn['molecule_synonym'])
                synonyms = list(set(synonyms))
                self.molecule2synonyms[i['molecule_chembl_id']] = synonyms

