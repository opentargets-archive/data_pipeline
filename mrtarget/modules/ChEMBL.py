import functools
import logging

import requests
import itertools
import more_itertools

from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.connection import new_es_client
from mrtarget.Settings import Config
from mrtarget.common import URLZSource
import json

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
        with URLZSource(uri + _fmt(limit=limit, offset=offset)).open() as f:
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
    def __init__(self, target_uri, mechanism_uri, component_uri, protein_uri, 
            molecule_set_uri_pattern):
        super(ChEMBLLookup, self).__init__()
        self._logger = logging.getLogger(__name__)
        
        #save configuration locally for future use
        self.target_uri = target_uri
        self.mechanism_uri = mechanism_uri
        self.component_uri = component_uri
        self.protein_uri = protein_uri
        self.molecule_set_uri_pattern = molecule_set_uri_pattern

        self.es_query = ESQuery(new_es_client())

        self.protein_class = dict()
        self.target_component = dict()
        self.mechanisms = {}
        self.target2molecule = {}
        self.disease2molecule = {}
        self.targets = {}
        self.uni2chembl = {}
        self.molecule2synonyms = {}
        self.protein_classification = {}
        self.protein_class = {}
        self.protein_class_label_to_id = {}

    def download_targets(self):
        '''fetches all the targets from chembl and store their data and a mapping to uniprot id'''

        self._logger.info('ChEMBL getting targets from ' +
                          self.target_uri)

        targets = get_chembl_url(self.target_uri)
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

        mechanisms = get_chembl_url(self.mechanism_uri)
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
        for i in range(0, len(required_molecules), batch_size):
            self.populate_synonyms_for_molecule(required_molecules[i:i + batch_size],
                                                 self.molecule2synonyms)

    def download_protein_classification(self):
        '''fetches targets components from chembls and inject the target class data in self.protein_classification'''
        self.download_protein_class()
        targets_components = get_chembl_url(self.component_uri)
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
        protein_classes = get_chembl_url(self.protein_uri)
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
        datatype = Config.DATASOURCE_TO_DATATYPE_MAPPING['chembl']
        for c, e in enumerate(self.es_query.get_all_evidence_for_datatype(datatype,
                fields=['target.id','disease.id', 'evidence.target2drug.urls'])):
            #get information from URLs that we need to extract short ids
            #e.g. https://www.ebi.ac.uk/chembl/compound/inspect/CHEMBL502835
            molecule_ids = [i['url'].split('/')[-1] for i in e['evidence']['target2drug']['urls'] if
                           '/compound/' in i['url']]
            if molecule_ids:
                molecule_id=molecule_ids[0]

                if c % 200 == 0:
                    self._logger.debug('retrieving ChEMBL evidence... %s', molecule_id)

                disease_id = e['disease']['id']
                target_id = e['target']['id']
                if disease_id not in self.disease2molecule:
                    self.disease2molecule[disease_id]=set()
                self.disease2molecule[disease_id].add(molecule_id)
                if target_id not in self.target2molecule:
                    self.target2molecule[target_id]=set()
                self.target2molecule[target_id].add(molecule_id)

    def populate_synonyms_for_molecule(self, molecule_set, molecules_syn_dict):
        def _append_to_mol2syn(m2s_dict, molecule):
            """if molecule has synonyms create a clean entry in m2s_dict with all synms for that chembl_id.
            Returns either None if goes ok or the molecule chembl id if something wrong"""
            if 'molecule_synonyms' in molecule and molecule['molecule_synonyms']:
                synonyms = []
                for syn in molecule['molecule_synonyms']:
                    synonyms.append(syn['synonyms'])
                    synonyms.append(syn['molecule_synonym'])
                synonyms = list(set(synonyms))
                m2s_dict[molecule['molecule_chembl_id']] = synonyms
                return None
            else:
                return molecule['molecule_chembl_id']

        if not molecule_set or not len(molecule_set):
            self._logger.warn("No molecules in set")
            return

        #build a URL to query chembl for
        url = self.molecule_set_uri_pattern.format(';'.join(molecule_set))

        #actually query that URL and get json back
        data = None
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self._logger.error('problem downloading molecule info from url %s', url)
            #re-raise exception to allow propegation
            raise e

        #if the data is what we expected, process it
        if 'molecules' in data:
            map_f = functools.partial(_append_to_mol2syn, molecules_syn_dict)
            mols_without_syn = \
                list(itertools.ifilterfalse(lambda mol: mol is None, itertools.imap(map_f, data['molecules'])))
            if mols_without_syn:
                self._logger.debug('molecule list with no synonyms %s', str(mols_without_syn))

        else:
            self._logger.error("there is no 'molecules' key in %s", url)
            raise RuntimeError("unexpected chembl API response")


