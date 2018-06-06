import json
import logging
import requests

from tqdm import tqdm
from mrtarget.Settings import Config
from mrtarget.common import Actions
#from mrtarget.common.ElasticsearchQuery import ESQuery

__copyright__  = "Copyright 2014-2018, Open Targets"
__credits__    = ["ChuangKee Ong"]
__license__    = "Apache 2.0"
__version__    = "1.2.8"
__maintainer__ = "ChuangKee Ong"
__email__      = ["data@targetvalidation.org"]
__status__     = "Production"

class CompoundActions(Actions):
    PROCESS = 'process'

class CompoundProcess():

    def __init__(self, loader):
        self._logger = logging.getLogger(__name__)
        self.loader = loader
        #self.es_query = ESQuery()
        self.cpd2indication = {}


    def process(self):
        self.download_cpd_indications()
        self.store_to_elasticsearch()

    def download_cpd_indications(self):
        '''
        Download compound/drug-disease indications &
        enrich the compound/drug attributes from ChEMBL API
        '''
        #TODO: Need to enable repeat call to indication API, now limited to max 20 per call
        # https://www.ebi.ac.uk/chembl/api/data/drug_indication?format=json
        r = requests.get(Config.CHEMBL_INDICATION)
        data = r.json()

        for row in tqdm(data['drug_indications'],
                    desc='Downloading compound/drug indications from ChEMBL API',
                    unit='compound'):

            cpd_id = row['molecule_chembl_id']
            disease_id = row['efo_id']
            disease_label = row['efo_term']
            disease_max_phase = row['max_phase_for_ind']
            reference = row['indication_refs']

            #self._logger("Extract compound attributes for %s", cpd_id)
            cpd_attrib = self.download_cpd_attributes(cpd_id)

            cpd_type = cpd_attrib['molecule_type']
            trade_name = cpd_attrib['pref_name']

            indication = \
                {
                 'disease_id': disease_id,
                 'disease_label': disease_label,
                 'disease_max_phase': disease_max_phase,
                 'reference': reference,
                 'cpd_type': cpd_type,
                 'trade_name': trade_name
                }

            #self.cpd2indication[cpd_id] = []
            #self.cpd2indication[cpd_id] = {'indication': indication}
            #self.cpd2indication[cpd_id] = {'attributes': cpd_attrib}

            try:
                self.cpd2indication[cpd_id]["indication"].append(indication)
            except KeyError:
                self.cpd2indication[cpd_id]["indication"] = list()
                self.cpd2indication[cpd_id]["indication"].append(indication)


            #self.cpd2indication[cpd_id] =\
            #{
            #  'compound_id': cpd_id,
            #  'compound_type': cpd_type,
            #  'trade_name': trade_name,
            #  'disease_id': disease_id,
            #  'disease_term': disease_label,
            #  'disease_max_phase': disease_max_phase,
            #  'ref': reference,
            #  'attributes': cpd_attrib
            # }

            # get info from molecule mechanism API end point MOA of a drug (get their targets)


    def download_cpd_attributes(self, compound_id):

        uri = 'https://www.ebi.ac.uk/chembl/api/data/molecule?format=json&chembl_id=' + compound_id
        r = requests.get(uri)
        data = r.json()

        return data['molecules'][0]

    def store_to_elasticsearch(self):

        #for k,v in self.cpd2indication.items():
        #    print(json.dumps(v, indent=4))

        for compound, data in self.cpd2indication.items():
            self.loader.put(Config.ELASTICSEARCH_COMPOUND_INDEX_NAME,
                            Config.ELASTICSEARCH_COMPOUND_DOC_NAME,
                            compound,
                            json.dumps(data),
                            True)

def query_chembl_url(uri):
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


