from __future__ import absolute_import
import logging
import re
import csv
import ujson as json
import itertools
import functools
import operator as oper

import pypeln.process as pr
import petl
import more_itertools
from mrtarget.common import URLZSource

from mrtarget.common.ElasticsearchQuery import ESQuery, Loader

from mrtarget.constants import Const
from mrtarget.Settings import Config #TODO remove this eventually
from mrtarget.common.connection import new_es_client
from addict import Dict
from mrtarget.common.DataStructure import JSONSerializable, json_serialize, PipelineEncoder


_missing_tissues = {'names': {},
                    'codes': {}}

def level_from_text(key):
    level_translation = {'Not detected': 0,
                              'Low': 1,
                              'Medium': 2,
                              'High': 3,
                              }
    return level_translation[key]


def reliability_from_text(key):
    reliability_translation = {'Supportive': True,
                                'Uncertain': False,
                                # # new types for hpa v16
                                'Approved' : True,
                                'Supported': True,
                                'Enhanced': True
                                }
    return reliability_translation[key]


class HPAExpression(Dict, JSONSerializable):
    def __init__(self, *args, **kwargs):
        super(HPAExpression, self).__init__(*args, **kwargs)
        if 'data_release' not in self:
            self.data_release = Config.RELEASE_VERSION

        if 'tissues' not in self:
            self.tissues = []

        if 'cancer' not in self:
            self.cancer = Dict()

        if 'subcellular_location' not in self:
            self.subcellular_location = {}

    def set_id(self, gene_id):
        self.gene = gene_id

    def get_id(self):
        return self.gene if 'gene' in self else None

    @staticmethod
    def new_tissue_protein(*args, **kwargs):
        protein = Dict(*args, **kwargs)

        if 'level' not in protein:
            protein.level = -1

        if 'reliability' not in protein:
            protein.reliability = False

        if 'cell_type' not in protein:
            protein.cell_type = []

        return protein

    @staticmethod
    def new_tissue_rna(*args, **kwargs):
        rna = Dict(*args, **kwargs)

        if 'level' not in rna:
            rna.level = -1

        if 'value' not in rna:
            rna.value = 0

        if 'unit' not in rna:
            rna.unit = ''

        if 'zscore' not in rna:
            rna.zscore = -1

        return rna

    @staticmethod
    def new_tissue(*args, **kwargs):
        tissue = Dict(*args, **kwargs)
        if 'efo_code' not in tissue:
            tissue.efo_code = ''

        if 'label' not in tissue:
            tissue.label = ''

        if 'anatomical_systems' not in tissue:
            tissue.anatomical_systems = []

        if 'organs' not in tissue:
            tissue.organs = []

        if 'protein' not in tissue:
            tissue.protein = HPAExpression.new_tissue_protein()

        if 'rna' not in tissue:
            tissue.rna = HPAExpression.new_tissue_rna()

        return tissue

    def to_json(self):
        return json.dumps(self.to_dict(),
                          default=json_serialize,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        try:
            self.update(json.loads(data))
        except Exception as e:
            raise e


def format_expression(rec):
    d = HPAExpression(gene=rec['gene'],
                      data_release=Config.RELEASE_VERSION)

    # for each tissue
    for el in rec['data']:
        t_code, t_name, c_types, t_asys, t_organs = el

        asys = list(set([item for sublist in t_asys for item in sublist]))
        organs = list(set([item for sublist in t_organs for item in sublist]))

        tissue = d.new_tissue(label=list(t_name)[0],
                              efo_code=t_code,
                              anatomical_systems=asys,
                              organs=organs)

        # iterate all cell_types
        for ct in c_types:
            ct_name, ct_level, ct_reliability = ct
            ctype = Dict()
            ctype.level = ct_level
            ctype.reliability = ct_reliability
            ctype.name = ct_name
            tissue.protein.cell_type.append(ctype)

            if ct_level > tissue.protein.level:
                tissue.protein.level = ct_level
                tissue.protein.reliability = ct_reliability

        # per tissue
        d.tissues.append(tissue)

    return d.to_dict()


def format_expression_with_rna(rec):
    # get gene,result,data = rec
    exp = HPAExpression(gene=rec['gene'],
                        data_release=Config.RELEASE_VERSION)

    if rec['result']:
        exp.update(rec['result'])

    if rec['data']:
        new_tissues = []
        has_tissues = len(exp.tissues) > 0

        t_set = functools.reduce(lambda x, y: x.union(set([y['efo_code']])),
                          exp.tissues, set()) \
                    if has_tissues else set()
        nt_set = functools.reduce(lambda x, y: x.union(set([y[0]])),
                          rec['data'], set())

        intersection = t_set.intersection(nt_set)
        intersection_idxs = {e['efo_code']: i for i, e in enumerate(exp.tissues) if e['efo_code'] in intersection}
        intersection_idxs_data = {e[0]: i for i, e in enumerate(rec['data']) if e[0] in intersection}
        difference = nt_set.difference(t_set)
        difference_idxs = [i for i, e in enumerate(rec['data']) if e[0] in difference]

        for ec in intersection:
            tidx = intersection_idxs[ec]
            didx = intersection_idxs_data[ec]

            exp.tissues[tidx].rna.level = int(rec['data'][didx][2])
            exp.tissues[tidx].rna.value = float(rec['data'][didx][3])
            exp.tissues[tidx].rna.unit = rec['data'][didx][4]
            exp.tissues[tidx].rna.zscore = int(rec['data'][didx][7])


        for idx in difference_idxs:
            rna = rec['data'][idx]
            t = exp.new_tissue(efo_code=rna[0],
                               label=rna[1],
                               anatomical_systems=rna[5],
                               organs=rna[6])
            t.rna.level = int(rna[2])
            t.rna.value = float(rna[3])
            t.rna.unit = rna[4]
            t.rna.zscore = int(rna[7])

            new_tissues.append(t)

        # iterate all tissues
        exp.tissues.extend(new_tissues)

    return exp.to_dict()


def name_from_tissue(tissue_name, t2m):
    curated = None
    tname = None

    try:
        curated = t2m['curations'].get(tissue_name, tissue_name)
        tname = t2m['tissues'][curated]['label']
    except KeyError:
        # TODO the id has to be one word to not get splitted by the analyser
        tname = curated

        if curated not in _missing_tissues['names']:
            _missing_tissues['names'][tname] = tissue_name
            logger = logging.getLogger(__name__)
            logger.warn('the tissue name %s was not found in the mapping', curated)

    return tname.strip()


def code_from_tissue(tissue_name, t2m):
    curated = None
    tid = None
    try:
        curated = t2m['curations'].get(tissue_name, tissue_name)
        tid = t2m['tissues'][curated]['efo_code']
    except KeyError:
        # TODO the id has to be one word to not get splitted by the analyser
        tid = tissue_name.strip().replace(' ', '_')
        tid = re.sub('[^0-9a-zA-Z_]+', '', tid)

        #try to ensure each missing tissue is only logged once
        if tid not in _missing_tissues['codes']:
            _missing_tissues['codes'][tid] = tissue_name
            logger = logging.getLogger(__name__)
            logger.warn('the tissue code %s was not found in the mapping', curated)

    return tid.strip()


def asys_from_tissue(tissue_name, t2m):
    curated = None
    tname = []
    try:
        curated = t2m['curations'].get(tissue_name, tissue_name)
        tname = t2m['tissues'][curated].get('anatomical_systems', [])
    except KeyError:
        pass

    return tname


def organs_from_tissue(tissue_name, t2m):
    curated = None
    tname = []
    try:
        curated = t2m['curations'].get(tissue_name, tissue_name)
        tname = t2m['tissues'][curated].get('organs', [])
    except KeyError:
        pass

    return tname


def hpa2tissues(hpa=None):
    '''return a list of tissues if any or empty list'''
    def _split_tissue(k, v):
        rna_level = v['rna']['level'] if v['rna'] else -1
        '''from tissue dict to rna and protein dicts pair'''
        rna = list(itertools.imap(lambda e: {'id': '_'.join([str(e), k]),
                                      'level': e} if v['rna'] else {},
                           xrange(0, rna_level + 1) if rna_level >= 0 else xrange(-1, 0)))

        zscore_level = v['rna']['zscore'] if v['rna'] else -1
        '''from tissue dict to rna and protein dicts pair'''
        zscore = list(itertools.imap(lambda e: {'id': '_'.join([str(e), k]),
                                      'level': e} if v['rna'] else {},
                           xrange(0, zscore_level + 1) if zscore_level >= 0 else xrange(-1, 0)))


        pro_level = v['protein']['level'] if v['protein'] else -1
        protein = list(itertools.imap(lambda e: {'id': '_'.join([str(e), k]),
                                          'level': e} if v['protein'] else {},
                               xrange(0, pro_level + 1) if pro_level >= 0 else xrange(-1, 0)))

        return (rna, protein, zscore)

    # generate a list with (rna, protein, zscore) tuple pairs per tissue
    splitted_tissues = [_split_tissue(t['efo_code'], t) for t in hpa.tissues
                        if hpa is not None]

    rnas = []
    proteins = []
    zscores = []

    for tissue in splitted_tissues:
        if tissue[0]:
            rnas += tissue[0]

        if tissue[1]:
            proteins += tissue[1]

        if tissue[2]:
            zscores += tissue[2]

    return {'rna': rnas,
            'protein': proteins,
            'zscore': zscores}

class HPADataDownloader():
    def __init__(self, tissue_translation_map, 
            tissue_curation_map,
            normal_tissue_url,
            rna_level_url,
            rna_value_url,
            rna_zscore_url):
        self.logger = logging.getLogger(__name__)
        self.tissue_translation_map = tissue_translation_map
        self.tissue_curation_map = tissue_curation_map
        self.normal_tissue_url = normal_tissue_url
        self.rna_level_url = rna_level_url
        self.rna_value_url = rna_value_url
        self.rna_zscore_url = rna_zscore_url



        #load t2m
        t2m = {'tissues': {} ,
               'curations': {}}

        with URLZSource(self.tissue_translation_map).open() as r_file:
            t2m['tissues'] = json.load(r_file)['tissues']


        with URLZSource(self.tissue_curation_map).open() as r_file:
            t2m['curations'] = {el['name']: el['canonical']
                                    for el in csv.DictReader(r_file,
                                              fieldnames=['name', 'canonical'],
                                              delimiter='\t')}
        self.t2m = t2m

    def retrieve_normal_tissue_data(self):
        """Parse 'normal_tissue' csv file,
        the expression profiles for proteins in human tissues from HPA

        :return: dict
        """
        self.logger.info('get normal tissue rows into dicts')
        table = (
            petl.fromcsv(URLZSource(self.normal_tissue_url), delimiter='\t')
            .rename({'Tissue': 'tissue',
                     'Cell type': 'cell_type',
                     'Level': 'level',
                     'Reliability': 'reliability',
                     'Gene': 'gene'})
            .cut('tissue', 'cell_type', 'level', 'reliability', 'gene')
            .addfield('tissue_label',
                      lambda rec: name_from_tissue(rec['tissue'].strip(), self.t2m))
            .addfield('tissue_code',
                      lambda rec: code_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('tissue_level', lambda rec: level_from_text(rec['level']))
            .addfield('anatomical_systems',
                      lambda rec: asys_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('organs',
                      lambda rec: organs_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('tissue_reliability', lambda rec: reliability_from_text(rec['reliability']))
            .cut('gene', 'tissue_code',
                 'tissue_label', 'tissue_level',
                 'tissue_reliability', 'cell_type',
                 'anatomical_systems', 'organs')
            .aggregate(('gene', 'tissue_code'),
                       aggregation={'cell_types': (('cell_type', 'tissue_level',
                                              'tissue_reliability'), list),
                                    'tissue_label': ('tissue_label', set),
                                    'anatomical_systems': ('anatomical_systems', list),
                                    'organs': ('organs', list)},
                       presorted=True)
            .aggregate('gene', aggregation={'data': (('tissue_code',
                                                      'tissue_label',
                                                      'cell_types',
                                                      'anatomical_systems',
                                                      'organs'), list)},
                       presorted=True)
            .addfield('result', lambda rec: format_expression(rec))
            .cut('gene', 'result')
            )

        return table

    def retrieve_rna_data(self):
        """
        Parse 'rna_tissue' csv file,
        RNA levels in 56 cell lines and 37 tissues based on RNA-seq from HPA.

        :return: dict
        """
        self.logger.info('get rna tissue rows into dicts')
        self.logger.debug('melting rna level table into geneid tissue level')

        t_level = (petl.fromcsv(URLZSource(self.rna_level_url), delimiter='\t')
            .melt(key='ID', variablefield='tissue', valuefield='rna_level')
            .rename({'ID': 'gene'})
            .addfield('tissue_label',
                      lambda rec: name_from_tissue(rec['tissue'].strip(), self.t2m))
            .addfield('tissue_code',
                      lambda rec: code_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('anatomical_systems',
                      lambda rec: asys_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('organs',
                      lambda rec: organs_from_tissue(rec['tissue_label'], self.t2m))
            .cutout('tissue')
        )

        t_value = (petl.fromcsv(URLZSource(self.rna_value_url), delimiter='\t')
            .melt(key='ID', variablefield='tissue', valuefield='rna_value')
            .rename({'ID': 'gene'})
            .addfield('tissue_label',
                      lambda rec: name_from_tissue(rec['tissue'].strip(), self.t2m))
            .addfield('tissue_code',
                      lambda rec: code_from_tissue(rec['tissue_label'], self.t2m))
            .addfield('rna_unit', 'TPM')
            .cutout('tissue')
        )

        t_zscore = (petl.fromcsv(URLZSource(self.rna_zscore_url), delimiter='\t')
            .melt(key='ID', variablefield='tissue', valuefield='zscore_level')
            .rename({'ID': 'gene'})
            .addfield('tissue_label',
                      lambda rec: name_from_tissue(rec['tissue'].strip(), self.t2m))
            .addfield('tissue_code',
                      lambda rec: code_from_tissue(rec['tissue_label'], self.t2m))
            .cutout('tissue')
        )

        t_vl = petl.join(t_level,
                           t_value,
                           key=('gene', 'tissue_code', 'tissue_label'),
                           presorted=True)

        t_join = (petl.join(t_vl,
                           t_zscore,
                           key=('gene', 'tissue_code', 'tissue_label'),
                           presorted=True)
                  .aggregate('gene',
                             aggregation={'data': (('tissue_code',
                                                      'tissue_label',
                                                      'rna_level',
                                                      'rna_value',
                                                      'rna_unit',
                                                      'anatomical_systems',
                                                      'organs',
                                                      'zscore_level'), list)},
                       presorted=True)
        )

        return t_join


def write_on_start(es_hosts):
    kwargs = {}
    es_client = new_es_client(es_hosts)
    kwargs['es_loader'] = Loader(es=es_client)

    return kwargs

def write_on_done(status, resources):
    resources['es_loader'].flush_all_and_wait(Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME)
    resources['es_loader'].close()

def write_to_elastic(data, resources):
    hpa = data[0]
    resources['es_loader'].put(Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                Const.ELASTICSEARCH_EXPRESSION_DOC_NAME,
                ID=hpa['gene'],
                body=hpa,
                create_index=False)



class HPAProcess():
    def __init__(self, loader, r_server, 
            es_hosts,
            tissue_translation_map_url, 
            tissue_curation_map_url,
            normal_tissue_url,
            rna_level_url,
            rna_value_url,
            rna_zscore_url):
        self.loader = loader
        self.r_server = r_server
        self.es_hosts = es_hosts
        self.downloader = HPADataDownloader(tissue_translation_map_url, 
            tissue_curation_map_url,
            normal_tissue_url,
            rna_level_url,
            rna_value_url,
            rna_zscore_url)
        self.logger = logging.getLogger(__name__)
        self.hpa_normal_table = None
        self.hpa_rna_table = None
        self.hpa_merged_table = None

    def process_all(self, dry_run=False):
        self.hpa_normal_table = self.process_normal_tissue()
        self.hpa_rna_table = self.process_rna()
        self.hpa_merged_table = self.process_join()

        if not dry_run:
            self.store_data()

    def process_normal_tissue(self):
        return self.downloader.retrieve_normal_tissue_data()

    def process_rna(self):
        return self.downloader.retrieve_rna_data()

    def process_join(self):
        hpa_merged_table = (
            petl.outerjoin(self.hpa_normal_table, self.hpa_rna_table,
                           key='gene', presorted=True)
            .addfield('expression', lambda rec: format_expression_with_rna(rec))
            .cut('expression')
        )
        return hpa_merged_table


    def store_data(self):
        self.logger.info('store_data called')

        self.logger.debug('calling to create new expression index')

        self.loader.create_new_index(Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME)
        self.loader.prepare_for_bulk_indexing(
            self.loader.get_versioned_index(
                Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME))

        #actually store it into elasticsearch       
        self.logger.info('starting to write to elasticsearch') 

        data = self.hpa_merged_table.data()

        write_on_start_partial = functools.partial(write_on_start, self.es_hosts)

        #handle everything on this main thread
        resources = write_on_start_partial()
        more_itertools.consume(
            itertools.imap(write_to_elastic, 
                data, itertools.repeat(resources)))
        write_on_done(None, resources)

        #this is a way to do it using pypyeln mulitprocessing
        #but this is 33% slower than the main thread version
        #
        #setup the mulitprocesses but dont do much
        #pl_stage = pr.map(write_to_elastic, data,
        #    workers=Config.WORKERS_NUMBER, maxsize=1000,
        #    on_start=write_on_start_partial,
        #    on_done=write_on_done)
        #now wait for all the multiprocessing to actually finish
        #more_itertools.consume(pr.to_iterable(pl_stage))

        self.loader.flush_all_and_wait(Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME)
        #restore old pre-load settings
        #note this automatically does all prepared indexes
        self.loader.restore_after_bulk_indexing()
        
        self.logger.info('all expressions objects pushed to elasticsearch')

        self.logger.info('missing tissues %s', str(_missing_tissues))


    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):
        self.logger.info("Starting QC")

        #number of hpa entries
        hpa_count = 0
        #Note: try to avoid doing this more than once!
        for hpa_entry in esquery.get_all_hpa():
            hpa_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["hpa.count"] = hpa_count

        self.logger.info("Finished QC")
        return metrics
