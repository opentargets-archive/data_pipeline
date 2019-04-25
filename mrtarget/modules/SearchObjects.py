import json
import logging

from collections import defaultdict

from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.modules.ChEMBL import ChEMBLLookup
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager

from opentargets_urlzsource import URLZSource

import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

class SearchObjectTypes(object):
    TARGET = 'target'
    DISEASE = 'disease'
    GENERIC = 'generic'


class SearchObject(JSONSerializable, object):
    """ Base class for search objects
    """
    def __init__(self,
                 id='',
                 name='',
                 full_name='',
                 description='',
                 ):
        self.id = id
        if not name:
            name = id
        self.name = name
        if not full_name:
            full_name = name
        self.full_name = full_name
        if not description:
            description = full_name
        self.description = description
        self.type = SearchObjectTypes.GENERIC
        self.private ={}
        self._create_suggestions()

    def set_associations(self,
                         top_associations =  dict(total = [], direct = []),
                         association_counts = dict(total = 0, direct = 0),
                         max_top = 20):
        self.top_associations = dict(total = top_associations['total'][:max_top],
                                     direct = top_associations['direct'][:max_top])
        self.association_counts = association_counts

    def _create_suggestions(self):
        '''reimplement in subclasses to allow a better autocompletion'''

        field_order = [self.id,
                       self.name,
                       self.description,
                       ]

        self.private['suggestions'] = dict(input = [],
                                           output = self.name,
                                           payload = dict(id = self.id,
                                                          title = self.name,
                                                          dull_name = self.full_name,
                                                          description = self.description),
                                           )


        for field in field_order:
            if isinstance(field, list):
                self.private['suggestions']['input'].extend(field)
            else:
                self.private['suggestions']['input'].append(field)

        self.private['suggestions']['input'] = [x.lower() for x in self.private['suggestions']['input']]

    def digest(self, json_input):
        pass

    def _parse_json(self, json_input):
        if isinstance(json_input, str) or isinstance(json_input, unicode):
            json_input=json.loads(json_input)
        return json_input


class SearchObjectTarget(SearchObject, object):
    """
    Target search object
    """
    def __init__(self,
                 id='',
                 name='',
                 description='',
                 ):
        super(SearchObjectTarget, self).__init__()
        self.type = SearchObjectTypes.TARGET
        self.ortholog = dict()

    def digest(self, json_input):
        json_input = self._parse_json(json_input)
        self.id=json_input['id']
        self.name=json_input['approved_symbol']
        self.full_name = json_input['approved_name']
        if json_input['uniprot_function']:
            self.description=json_input['uniprot_function'][0]
        self.approved_symbol=json_input['approved_symbol']
        self.approved_name=json_input['approved_name']
        self.symbol_synonyms=json_input['symbol_synonyms']
        self.name_synonyms=json_input['name_synonyms']
        self.biotype=json_input['biotype']
        self.gene_family_description=json_input['gene_family_description']
        self.uniprot_accessions=json_input['uniprot_accessions']
        self.hgnc_id=json_input['hgnc_id']
        self.ensembl_gene_id=json_input['ensembl_gene_id']
        if json_input['ortholog']:
            for species,ortholist in json_input['ortholog'].items():
                self.ortholog[species]=[
                        {'symbol': o["ortholog_species_symbol"],
                         'id':     o["ortholog_species_assert_ids"],
                         'name':   o["ortholog_species_name"]}
                        for o in ortholist
                ]
        if json_input['drugs']:
            self.drugs = json_input['drugs']
            self.drugs['drugbank'] = []
            for drug in json_input['drugbank']:
                if 'value' in drug and 'generic name' in drug['value']:
                    self.drugs['drugbank'].append(drug['value']['generic name'])



class SearchObjectDisease(SearchObject, object):
    """
    Target search object
    """
    def __init__(self,
                 id='',
                 name='',
                 description='',
                 ):
        super(SearchObjectDisease, self).__init__()
        self.type = SearchObjectTypes.DISEASE


    def digest(self, json_input):
        json_input = self._parse_json(json_input)
        self.id=json_input['path_codes'][0][-1]
        self.name=json_input['label']
        self.full_name=json_input['label']
        self.description=json_input['definition']
        self.efo_code=json_input['path_codes'][0][-1]
        self.efo_url=json_input['code']
        self.efo_label=json_input['label']
        self.efo_definition=json_input['definition']
        clean_synonyms = [i for i in json_input['efo_synonyms'] if not i.startswith('MSH:')]
        self.efo_synonyms=clean_synonyms
        self.efo_path_codes=json_input['path_codes']
        self.efo_path_labels=json_input['path_labels']
        self.min_path_len=len(json_input['path_codes'][0])
        if len(json_input['path_codes'])>1:
            for path in json_input['path_codes'][1]:
                path_len = len(path)
                if path_len < self.min_path_len:
                    self.min_path_len = path_len
        # self.min_path_len-=1#correct for cttv_root
        self.phenotypes = json_input['phenotypes']

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, dry_run, index, doc):
    for so in items:
        if not dry_run:
            action = {}
            action["_index"] = index
            action["_type"] = doc+'-'+so.type
            action["_id"] = so.id
            #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
            #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
            action["_source"] = so.to_json()

            yield action

def store_in_elasticsearch(so_it, dry_run, es, index, doc, workers_write, queue_write):
        #write into elasticsearch
        chunk_size = 1000 #TODO make configurable
        actions = elasticsearch_actions(so_it, dry_run, index, doc)
        failcount = 0
        for result in elasticsearch.helpers.parallel_bulk(es, actions,
                thread_count=workers_write, queue_size=queue_write, 
                chunk_size=chunk_size):
            success, details = result
            if not success:
                failcount += 1

class SearchObjectProcess(object):
    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings, 
            es_index_gene, es_index_efo,
            r_server, workers_write, queue_write,
            chembl_target_uri, 
            chembl_mechanism_uri, 
            chembl_component_uri, 
            chembl_protein_uri, 
            chembl_molecule_set_uri_pattern):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.es_index_gene = es_index_gene
        self.es_index_efo = es_index_efo
        self.r_server = r_server
        self.workers_write = workers_write
        self.queue_write = queue_write
        self.chembl_target_uri = chembl_target_uri
        self.chembl_mechanism_uri = chembl_mechanism_uri
        self.chembl_component_uri = chembl_component_uri
        self.chembl_protein_uri = chembl_protein_uri
        self.chembl_molecule_set_uri_pattern = chembl_molecule_set_uri_pattern

        self.logger = logging.getLogger(__name__)

        '''define data processing handlers'''
        self.data_handlers = defaultdict(lambda: SearchObject)
        self.data_handlers[SearchObjectTypes.TARGET] = SearchObjectTarget
        self.data_handlers[SearchObjectTypes.DISEASE] = SearchObjectDisease


    def process_all(self, 
            dry_run):
        ''' process all the objects that needs to be returned by the search method
        :return:
        '''

        es = new_es_client(self.es_hosts)
        esquery = ESQuery(es)

        #setup chembl handler
        self.chembl_handler = ChEMBLLookup(self.chembl_target_uri, 
            self.chembl_mechanism_uri, 
            self.chembl_component_uri, 
            self.chembl_protein_uri, 
            self.chembl_molecule_set_uri_pattern)
        self.chembl_handler.get_molecules_from_evidence(esquery)
        all_molecules = set()
        for target, molecules in  self.chembl_handler.target2molecule.items():
            all_molecules = all_molecules|molecules
        all_molecules = sorted(all_molecules)
        query_batch_size = 100
        for i in range(0, len(all_molecules) + 1, query_batch_size):
            self.chembl_handler.populate_synonyms_for_molecule(all_molecules[i:i + query_batch_size],
                self.chembl_handler.molecule2synonyms)

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):
            #process targets
            self.logger.info('handling targets')
            targets = Search().using(es).index(self.es_index_gene).query(MatchAll()).scan()
            so_it = self.handle_search_object(targets, esquery, SearchObjectTypes.TARGET)
            store_in_elasticsearch(so_it, dry_run, es, self.es_index, self.es_doc, 
                self.workers_write, self.queue_write)

            #process diseases
            self.logger.info('handling diseases')
            diseases = Search().using(es).index(self.es_index_efo).query(MatchAll()).scan()
            so_it = self.handle_search_object(diseases, esquery, SearchObjectTypes.DISEASE)
            store_in_elasticsearch(so_it, dry_run, es, self.es_index, self.es_doc, 
                self.workers_write, self.queue_write)


    def summarise_association(self, data):
        def cap_score(value):
            if value > 1:
                return 1.0
            elif value < -1:
                return -1
            return value
        return dict(total = [dict(id = data_point['id'],
                                score = cap_score(data_point['harmonic-sum']['overall'])) for data_point in data['total']],
                    direct = [dict(id = data_point['id'],
                                score = cap_score(data_point['harmonic-sum']['overall'])) for data_point in data['direct']]
            )

    def handle_search_object(self, data_it, esquery, search_type):
        for data in data_it:
            data["search_type"] = search_type
            '''process objects to simple search object'''
            so = self.data_handlers[data["search_type"]]()
            so.digest(json_input=data)

            '''inject drug data'''
            if not hasattr(so, 'drugs'):
                so.drugs = {}
            so.drugs['evidence_data'] = []

            '''count associations '''
            if data["search_type"] == SearchObjectTypes.TARGET:
                ass_data = esquery.get_associations_for_target(data['id'], fields=['id','harmonic-sum.overall'], size = 20)
                so.set_associations(self.summarise_association(ass_data.top_associations),
                    ass_data.associations_count)
                if so.id in self.chembl_handler.target2molecule:
                    drugs_synonyms = set()
                    for molecule in self.chembl_handler.target2molecule[so.id]:
                        if molecule in self.chembl_handler.molecule2synonyms:
                            drugs_synonyms = drugs_synonyms | set(self.chembl_handler.molecule2synonyms[molecule])
                    so.drugs['evidence_data'] = list(drugs_synonyms)

            elif data["search_type"] == SearchObjectTypes.DISEASE:
                ass_data = esquery.get_associations_for_disease(data['path_codes'][0][-1], fields=['id','harmonic-sum.overall'], size = 20)
                so.set_associations(self.summarise_association(ass_data.top_associations),
                    ass_data.associations_count)
                if so.id in self.chembl_handler.disease2molecule:
                    drugs_synonyms = set()
                    for molecule in self.chembl_handler.disease2molecule[so.id]:
                        if molecule in self.chembl_handler.molecule2synonyms:
                            drugs_synonyms = drugs_synonyms | set(self.chembl_handler.molecule2synonyms[molecule])
                    so.drugs['evidence_data'] = list(drugs_synonyms)
            else:
                so.set_associations()

            yield so