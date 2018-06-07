import json
import logging
import requests

from tqdm import tqdm
from mrtarget.Settings import Config
from mrtarget.common import Actions

__copyright__  = "Copyright 2014-2018, Open Targets"
__credits__    = ["ChuangKee Ong"]
__license__    = "Apache 2.0"
__version__    = "1.2.8"
__maintainer__ = "ChuangKee Ong"
__email__      = ["data@targetvalidation.org"]
__status__     = "Production"


def get_chembl_url(uri):
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


class CompoundActions(Actions):
    PROCESS = 'process'

class CompoundProcess():
    def __init__(self, loader):
        self._logger = logging.getLogger(__name__)
        self.loader = loader
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
        # self._logger("Download data of drug with indications")
        r = requests.get(Config.CHEMBL_INDICATION)
        data = r.json()
        #data = get_chembl_url(Config.CHEMBL_INDICATION)

        for row in tqdm(data['drug_indications'],
                    desc='Downloading compound/drug indications from ChEMBL API',
                    unit='compound'):

            cpd_id = row['molecule_chembl_id']
            disease_id = row['efo_id']
            disease_label = row['efo_term']
            disease_max_phase = row['max_phase_for_ind']
            reference = row['indication_refs']

            #self._logger("Download drug-target mechanism of action for %s" , cpd_id)
            cpd2target = self.download_mechanism_of_action(cpd_id)

            #self._logger("Extract compound attributes for %s", cpd_id)
            cpd_attrib = self.download_cpd_attributes(cpd_id)
            cpd_type = cpd_attrib['molecule_type']
            trade_name = cpd_attrib['pref_name']

            line = \
                {
                 'disease_id': disease_id,
                 'disease_label': disease_label,
                 'disease_max_phase': disease_max_phase,
                 'reference': reference,
                 'cpd_type': cpd_type,
                 'trade_name': trade_name
                }

            if cpd_id not in self.cpd2indication:
                self.cpd2indication[cpd_id] = \
                    {
                     'attributes': cpd_attrib,
                     'indications': [],
                     'mechanism': []
                    }

            try:
                self.cpd2indication[cpd_id]['indications'].append(line)
                self.cpd2indication[cpd_id]['mechanism'].append(cpd2target)
            except KeyError:
                self.cpd2indication[cpd_id]['indications'] = list()
                self.cpd2indication[cpd_id]['indications'].append(line)
                self.cpd2indication[cpd_id]['mechanism'] = list()
                self.cpd2indication[cpd_id]['mechanism'].append(cpd2target)

    def download_cpd_attributes(self, compound_id):

        uri = 'https://www.ebi.ac.uk/chembl/api/data/molecule?format=json&chembl_id=' + compound_id
        r = requests.get(uri)
        data = r.json()

        return data['molecules'][0]

    def download_mechanism_of_action(self, compound_id):

        #uri = requests.get(Config.CHEMBL_MECHANISM)
        uri = 'https://www.ebi.ac.uk/chembl/api/data/mechanism?format=json&molecule_chembl_id=' + compound_id
        r = requests.get(uri)
        data = r.json()

        return data['mechanisms']


    def store_to_elasticsearch(self):

        for key, data in self.cpd2indication.items():
            self.loader.put(Config.ELASTICSEARCH_COMPOUND_INDEX_NAME,
                            Config.ELASTICSEARCH_COMPOUND_DOC_NAME,
                            key,
                            json.dumps(data),
                            True)


