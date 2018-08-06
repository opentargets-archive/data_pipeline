import json
import logging
import requests
from tqdm import tqdm
from mrtarget.Settings import Config
from mrtarget.common import Actions
from requests.exceptions import Timeout, HTTPError, ConnectionError
import mrtarget.common as c

from multiprocessing import Pool, current_process
import time
import queue

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

        self.cpd_data = {}

    def process(self):
        self.get_indication_cpd_data()
        self.get_mechanism_cpd_data()
        self.store_to_elasticsearch()

        #data = (["A", 5], ["B", 2], ["C", 1], ["D", 3])
        #self.pool_handler(data)

    def work_log(self, work_data):

        print(" Process %s " % work_data[0])
        print(" Wait for sec %s " % work_data[1])

        time.sleep(5)

        #print(" Process %s Finished." % work_data[0])

    def pool_handler(self, data):
        p = Pool(2)
        p.map(self.work_log(work_data=data), data)

    def do_job(self, tasks_to_accomplish, tasks_that_are_done):
        while True:
            try:
                task = tasks_to_accomplish.get_nowait()
            except queue.Empty:
                break
            else:
                print (task)
                tasks_that_are_done(task + ' complete by ' + current_process().name)
                time.sleep(5)

    # Get info for compounds with indication(s) in ChEMBL
    # Call indication API - save all compounds into a dictionary with cpd_id as key
    # Call molecule API to get info for each new compound
    def get_indication_cpd_data(self):

        ct = 0
        data = self.query_rest_api(Config.CHEMBL_INDICATION)

        for cpd in tqdm(data,
                  desc='Extract indication compound data from ChEMBL API',
                  unit=' compound(s)'):

            if ct == 30000: # 15000
                break

            cpd_id = cpd['molecule_chembl_id']
            # Create entry for molecule if not in cpd_data already
            if cpd_id not in self.cpd_data:
                cpd_attributes = self.download_from_uri(cpd_id, Config.CHEMBL_MOLECULE)
                self.cpd_data[cpd_id] = {
                        'attributes': cpd_attributes,
                        'indications': [],
                        'mechanisms': []
                    }
            line = {
                    'disease_id': cpd['efo_id'],
                    'disease_label': cpd['efo_term'],
                    'disease_max_phase': cpd['max_phase_for_ind'],
                    'reference': cpd['indication_refs'],
                    'mesh_heading': cpd['mesh_heading'],
                    'mesh_id': cpd['mesh_id']
                }

            try:
                self.cpd_data[cpd_id]['indications'].append(line)
            except KeyError:
                self.cpd_data[cpd_id]['indications'] = list()
                self.cpd_data[cpd_id]['indications'].append(line)
            #print(cpd_id)
            ct += 1

    # Get info for compounds with indication(s) in ChEMBL
    # Call indication API - save all compounds into a dictionary with cpd_id as key
    # Call molecule API to get info for each new compound
    def get_mechanism_cpd_data(self):

        ct = 0
        data = self.query_rest_api(Config.CHEMBL_MECHANISM)

        for cpd in tqdm(data,
                  desc='Extract mechanism compound data from ChEMBL API',
                  unit=' compound(s)'):

            if ct == 5000: # 15000
                break

            cpd_id = cpd['molecule_chembl_id']
            # Create entry for molecule if not in cpd_data already
            if cpd_id not in self.cpd_data:
                cpd_attributes = self.download_from_uri(cpd_id, Config.CHEMBL_MOLECULE)
                self.cpd_data[cpd_id] = {
                        'attributes': cpd_attributes,
                        'indications': [],
                        'mechanisms': []
                    }

            try:
                self.cpd_data[cpd_id]['mechanisms'].append(cpd)
            except KeyError:
                self.cpd_data[cpd_id]['mechanisms'] = list()
                self.cpd_data[cpd_id]['mechanisms'].append(cpd)
            #print(cpd_id)
            ct += 1

    def query_rest_api(self, uri):
        '''return json from uri'''
        next_get = True
        limit = 1000 # 1000000
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

    def download_from_uri(self, compound_id, uri):
            url = uri + '?molecule_chembl_id=' + compound_id

            try:
                r = requests.get(url, timeout=300)
                if r.status_code == 200:
                    return r.json()
                else:
                    raise IOError('failed to get data from ChEMBL API')

            except (ConnectionError, Timeout, HTTPError) as e:
                raise IOError(e)

    def store_to_elasticsearch(self):

        self._logger.debug('Store compound data into ElasticSearch')

        for key, data in self.cpd_data.items():
            self.loader.put(Config.ELASTICSEARCH_COMPOUND_INDEX_NAME,
                            Config.ELASTICSEARCH_COMPOUND_DOC_NAME,
                            key,
                            json.dumps(data),
                            True)


