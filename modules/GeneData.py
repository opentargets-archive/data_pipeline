import warnings
from collections import OrderedDict
import copy
import datetime
import time
import logging
from StringIO import StringIO
import urllib2

import sys

import multiprocessing
from sqlalchemy import and_
import ujson as json
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage, Loader
from common.ElasticsearchQuery import ESQuery
from common.PGAdapter import HgncGeneInfo, EnsemblGeneInfo, UniprotInfo, ElasticsearchLoad
from common.Redis import RedisLookupTablePickle, RedisQueue, RedisQueueStatusReporter
from common.UniprotIO import UniprotIterator
from modules.Reactome import ReactomeRetriever
from settings import Config


'''line profiler code'''
try:
    from line_profiler import LineProfiler

    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()
                    print 'done'
            return profiled_func
        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner
'''end of line profiler code'''

__author__ = 'andreap'

UNI_ID_ORG_PREFIX = 'http://identifiers.org/uniprot/'
ENS_ID_ORG_PREFIX = 'http://identifiers.org/ensembl/'

class GeneActions(Actions):
    MERGE='merge'
    UPLOAD='upload'

class Gene(JSONSerializable):
    def __init__(self, id=None):

        self.id = id
        self.hgnc_id = None
        self.approved_symbol = ""
        self.approved_name = ""
        self.status = ""
        self.locus_group = ""
        self.previous_symbols = []
        self.previous_names = []
        self.symbol_synonyms = []
        self.name_synonyms = []
        self.chromosome = ""
        self.enzyme_ids = []
        self.entrez_gene_id = ""
        self.ensembl_gene_id = ""
        self.refseq_ids = []
        self.gene_family_tag = ""
        self.gene_family_description = ""
        self.ccds_ids = []
        self.vega_ids = []
        self.alias_name=[]
        self.alias_symbol=[]
        self.pubmed_ids =[]
        self.is_active_in_ensembl = False
        self.ensembl_assembly_name = ""
        self.biotype = ""
        self.ensembl_description = ""
        self.gene_end = None
        self.gene_start = None
        self.strand = None
        self.ensembl_external_name = ""
        self.ensembl_gene_version = None
        self.cytobands = ""
        self.ensembl_release = None
        self.uniprot_id = ""
        self.uniprot_mapping_date = None
        self.uniprot_accessions = []
        self.is_in_swissprot = False
        self.dbxrefs = []
        self.uniprot_function = []
        self.uniprot_keywords = []
        self.uniprot_similarity = []
        self.uniprot_subunit = []
        self.uniprot_subcellular_location = []
        self.uniprot_pathway = []
        self.reactome = []
        self.go =[]
        self.pdb = []
        self.chembl = []
        self.drugbank = []
        self.pfam = []
        self.interpro = []
        self.is_ensembl_reference = []
        self._private ={}


    def _set_id(self):
        if self.ensembl_gene_id:
            self.id = self.ensembl_gene_id
        elif self.hgnc_id:
            self.id = self.hgnc_id
        elif self.entrez_gene_id:
            self.id = self.entrez_gene_id
        else:
            self.id = None


    def load_hgnc_data(self, data):
        if data.hgnc_id:
            self.hgnc_id = data.hgnc_id
        if data.approved_symbol:
            self.approved_symbol = data.approved_symbol
        if data.approved_name:
            self.approved_name = data.approved_name
        if data.status:
            self.status = data.status
        if data.locus_group:
            self.locus_group = data.locus_group
        if data.previous_symbols:
            self.previous_symbols = data.previous_symbols.split(', ')
        if data.previous_names:
            self.previous_names = data.previous_names.split(', ')
        if data.synonyms:
            self.symbol_synonyms.extend(data.synonyms.split(', '))
        if data.name_synonyms:
            self.name_synonyms = data.name_synonyms.split(', ')
        # if data.chromosome :
        # self.chromosome = data.chromosome
        if data.enzyme_ids:
            self.enzyme_ids = data.enzyme_ids.split(', ')
        if data.entrez_gene_id:
            self.entrez_gene_id = data.entrez_gene_id
        if data.ensembl_gene_id:
            self.ensembl_gene_id = data.ensembl_gene_id
            if not self.ensembl_gene_id:
                self.ensembl_gene_id = data.ensembl_id_supplied_by_ensembl
        if data.refseq_ids:
            self.refseq_ids = data.refseq_ids.split(', ')
        if data.gene_family_tag:
            self.gene_family_tag = data.gene_family_tag
        if data.gene_family_description:
            self.gene_family_description = data.gene_family_description
        if data.ccds_ids:
            self.ccds_ids = data.ccds_ids.split(', ')
        if data.vega_ids:
            self.vega_ids = data.vega_ids.split(', ')

    def load_hgnc_data_from_json(self, data):

        if 'ensembl_gene_id' in data:
            self.ensembl_gene_id = data['ensembl_gene_id']
            if not self.ensembl_gene_id:
                self.ensembl_gene_id = data['ensembl_id_supplied_by_ensembl']
            if 'hgnc_id' in data:
                self.hgnc_id = data['hgnc_id']
            if 'symbol' in data:
                self.approved_symbol = data['symbol']
            if 'name' in data:
                self.approved_name = data['name']
            if 'status' in data:
                self.status = data['status']
            if 'locus_group' in data:
                self.locus_group = data['locus_group']
            if 'prev_symbols' in data:
                self.previous_symbols = data['prev_symbols']
            if 'prev_names' in data:
                self.previous_names = data['prev_names']
            if 'alias_symbol' in data:
                self.symbol_synonyms.extend( data['alias_symbol'])
            if 'alias_name' in data:
                self.name_synonyms = data['alias_name']
            if 'enzyme_ids' in data:
                self.enzyme_ids = data['enzyme_ids']
            if 'entrez_id' in data:
                self.entrez_gene_id = data['entrez_id']
            if 'refseq_accession' in data:
                self.refseq_ids = data['refseq_accession']
            if 'gene_family_tag' in data:
                self.gene_family_tag = data['gene_family_tag']
            if 'gene_family_description' in data:
                self.gene_family_description = data['gene_family_description']
            if 'ccds_ids' in data:
                self.ccds_ids = data['ccds_ids']
            if 'vega_id' in data:
                self.vega_ids = data['vega_id']
            if 'uniprot_ids' in data:
                self.uniprot_accessions = data['uniprot_ids']
                if not self.uniprot_id:
                    self.uniprot_id = self.uniprot_accessions[0]
            if 'pubmed_id' in data:
                self.pubmed_ids = data['pubmed_id']

    def load_ensembl_data(self, data):

        if 'id' in data:
            self.is_active_in_ensembl = True
            self.ensembl_gene_id = data['id']
        if 'assembly_name' in data:
            self.ensembl_assembly_name = data['assembly_name']
        if 'biotype' in data:
            self.biotype = data['biotype']
        if 'description' in data:
            self.ensembl_description = data['description'].split(' [')[0]
            if not self.approved_name:
                self.approved_name= self.ensembl_description
        if 'end' in data:
            self.gene_end = data['end']
        if 'start' in data:
            self.gene_start = data['start']
        if 'strand' in data:
            self.strand = data['strand']
        if 'seq_region_name' in data:
            self.chromosome = data['seq_region_name']
        if 'display_name' in data:
            self.ensembl_external_name = data['display_name']
            if not self.approved_symbol:
                self.approved_symbol= data['display_name']
        if 'version' in data:
            self.ensembl_gene_version = data['version']
        if 'cytobands' in data:
            self.cytobands = data['cytobands']
        if 'ensembl_release' in data:
            self.ensembl_release = data['ensembl_release']
        is_reference = (data['is_reference'] and data['id'].startswith('ENSG'))
        self.is_ensembl_reference = is_reference


    def load_uniprot_entry(self, seqrec):
        self.uniprot_id = seqrec.id
        self.is_in_swissprot = True
        if seqrec.dbxrefs:
            self.dbxrefs.extend(seqrec.dbxrefs)
            self.dbxrefs= sorted(list(set(self.dbxrefs)))
        for k, v in seqrec.annotations.items():
            if k == 'accessions':
                self.uniprot_accessions = v
            if k == 'keywords':
                self.uniprot_keywords = v
            if k == 'comment_function':
                self.uniprot_function = v
            if k == 'comment_similarity':
                self.uniprot_similarity = v
            if k == 'comment_subunit':
                self.uniprot_subunit = v
            if k == 'comment_subcellularlocation_location':
                self.uniprot_subcellular_location = v
            if k == 'comment_pathway':
                self.uniprot_pathway = v
            if k == 'gene_name_primary':
                if not self.approved_symbol:
                    self.approved_symbol= v
                elif v!= self.approved_symbol:
                    if v not in self.symbol_synonyms:
                        self.symbol_synonyms.append(v)
            if k == 'gene_name_synonym':
                for symbol in v:
                    if symbol not in self.symbol_synonyms:
                        self.symbol_synonyms.append(symbol)
            if k.startswith('recommendedName'):
                self.name_synonyms.extend(v)
            if k.startswith('alternativeName'):
                self.name_synonyms.extend(v)
        self.name_synonyms.append(seqrec.description)
        self.name_synonyms = list(set(self.name_synonyms))
        if 'GO' in  seqrec.annotations['dbxref_extended']:
            self.go = seqrec.annotations['dbxref_extended']['GO']
        if 'Reactome' in  seqrec.annotations['dbxref_extended']:
            self.reactome = seqrec.annotations['dbxref_extended']['Reactome']
            # self._extend_reactome_data()
        if 'PDB' in  seqrec.annotations['dbxref_extended']:
            self.pdb = seqrec.annotations['dbxref_extended']['PDB']
        if 'ChEMBL' in  seqrec.annotations['dbxref_extended']:
            self.chembl = seqrec.annotations['dbxref_extended']['ChEMBL']
        if 'DrugBank' in  seqrec.annotations['dbxref_extended']:
            self.drugbank = seqrec.annotations['dbxref_extended']['DrugBank']
        if 'Pfam' in  seqrec.annotations['dbxref_extended']:
            self.pfam = seqrec.annotations['dbxref_extended']['Pfam']
        if 'InterPro' in  seqrec.annotations['dbxref_extended']:
            self.interpro = seqrec.annotations['dbxref_extended']['InterPro']

    def get_id_org(self):
        return ENS_ID_ORG_PREFIX + self.ensembl_gene_id

    def preprocess(self):
        self._create_suggestions()
        self._create_facets()

    def _create_suggestions(self):

        field_order = [self.approved_symbol,
                       self.approved_name,
                       self.symbol_synonyms,
                       self.name_synonyms,
                       self.previous_symbols,
                       self.previous_names,
                       self.uniprot_id,
                       self.uniprot_accessions,
                       self.ensembl_gene_id,
                       self.entrez_gene_id,
                       self.refseq_ids
                       ]

        self._private['suggestions'] = dict(input = [],
                                              output = self.approved_symbol,
                                              payload = dict(gene_id = self.id,
                                                             gene_symbol = self.approved_symbol,
                                                             gene_name = self.approved_name),
                                              )


        for field in field_order:
            if isinstance(field, list):
                self._private['suggestions']['input'].extend(field)
            else:
                self._private['suggestions']['input'].append(field)
        try:
            self._private['suggestions']['input'] = [x.lower() for x in self._private['suggestions']['input']]
        except:
            print "error", repr(self._private['suggestions']['input'])

    def _create_facets(self):
        self._private['facets'] = dict()
        if self.reactome:
            pathways=[]
            pathway_types=[]
            for r in self.reactome:
                reaction_code, reaction = r['id'], r['value']
                pathways.append(reaction_code)
                if 'pathway types' in reaction:
                    for ptype in reaction['pathway types']:
                        pathway_types.append(ptype["pathway type"])
            if not pathway_types:
                pathway_types.append('other')
            pathway_types=list(set(pathway_types))
            self._private['facets']['reactome']=dict(pathway_code = pathways,
                                                     # pathway_name=pathways,
                                                     pathway_type_code=pathway_types,
                                                     # pathway_type_name=pathway_types,
                                                     )



class GeneSet():
    def __init__(self):
        self.genes = OrderedDict()

    def __contains__(self, item):
        return self.genes.__contains__(item)


    def remove_gene(self,key):
        del self.genes[key]


    def add_gene(self, gene):
        if isinstance(gene, Gene):
            if not gene.id:
                gene._set_id()
            if gene.id:
                self.genes[gene.id] = gene

    def get_gene(self, geneid):
        try:
            return copy.deepcopy(self.genes[geneid])
        except KeyError:
            return copy.deepcopy(self.genes[geneid.replace(ENS_ID_ORG_PREFIX, '')])

    def iterate(self):
        for k, v in self.genes.items():
            yield k, v

    def __len__(self):
        return len(self.genes)

    def get_stats(self):
        stats = '''%i Genes Parsed:
\t%i (%1.1f%%) with Ensembl ID
\t%i (%1.1f%%) with HGNC ID
\t%i (%1.1f%%) with other IDs
\t%i (%1.1f%%) with Uniprot IDs
\t%i (%1.1f%%) with a Uniprot reviewed entry
\t%i (%1.1f%%) active genes in Emsembl'''

        ens, hgnc, other, ens_active, uni, swiss = 0., 0., 0., 0., 0., 0.

        for geneid, gene in self.genes.items():
            if geneid.startswith('ENS'):
                ens += 1
            elif geneid.startswith('HGNC:'):
                hgnc += 1
            else:
                other += 1

            if gene.is_active_in_ensembl:
                ens_active += 1
            if gene.uniprot_id:
                uni += 1
            if gene.is_in_swissprot:
                swiss += 1

        tot = len(self)
        stats = stats % (tot,
                         ens, ens / tot * 100.,
                         hgnc, hgnc / tot * 100.,
                         other, other / tot * 100.,
                         uni, uni / tot * 100.,
                         swiss, swiss / tot * 100.,
                         ens_active, ens_active / tot * 100.)
        return stats


class GeneObjectStorer(multiprocessing.Process):

    def __init__(self, es, r_server, queue):
        super(GeneObjectStorer, self).__init__()
        self.es = es
        self.r_server = r_server
        self.queue = queue


    def run(self):
        with Loader(self.es, chunk_size=100) as loader:
            while not self.queue.is_done(r_server=self.r_server):
                data = self.queue.get(r_server=self.r_server, timeout=1)
                if data is not None:
                    key, value = data
                    geneid, gene = value
                    error = False
                    try:
                        '''process objects to simple search object'''
                        gene.preprocess()
                        loader.put(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                   Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                   geneid,
                                   gene.to_json(),
                                   create_index=False)
                    except Exception, e:
                        error = True
                        logging.exception('Error processing key %s: %s' % (key, e))
                    self.queue.done(key, error=error, r_server=self.r_server)
                else:
                    time.sleep(0.1)


class GeneManager():
    """
    Merge data available in postgres into proper json objects
    """

    def __init__(self,
                 es, r_server):


        self.es = es
        self.r_server = r_server
        self.esquery = ESQuery(es)
        self.genes = GeneSet()
        self.reactome_retriever=ReactomeRetriever(es)



    def merge_all(self):
        self._get_hgnc_data_from_json()
        self._get_ensembl_data()
        self._get_uniprot_data()
        self._store_data()


    def _get_hgnc_data_from_json(self):
        req = urllib2.Request(Config.HGNC_COMPLETE_SET)
        response = urllib2.urlopen(req)
        # print response.code
        # if response.code == '200':
        data = json.loads(response.read())
        for row in data['response']['docs']:
            gene = Gene()
            gene.load_hgnc_data_from_json(row)
            self.genes.add_gene(gene)

        logging.info("STATS AFTER HGNC PARSING:\n" + self.genes.get_stats())


    def _get_ensembl_data(self):
        for row in self.esquery.get_all_ensembl_genes():
            if row['id'] in self.genes:
                gene = self.genes.get_gene(row['id'])
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)
            else:
                gene = Gene()
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)

        self._clean_non_reference_genes()

        logging.info("STATS AFTER ENSEMBL PARSING:\n" + self.genes.get_stats())

    def _clean_non_reference_genes(self):
        for geneid, gene in self.genes.iterate():
            if not gene.is_ensembl_reference:
                self.genes.remove_gene(geneid)

    # @do_profile
    def _get_uniprot_data(self):
        c = 0
        for seqrec in self.esquery.get_all_uniprot_entries():
            c += 1
            if c % 1000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
            if 'Ensembl' in seqrec.annotations['dbxref_extended']:
                ensembl_data=seqrec.annotations['dbxref_extended']['Ensembl']
                ensembl_genes_id=[]
                for ens_data_point in ensembl_data:
                    ensembl_genes_id.append(ens_data_point['value']['gene ID'])
                ensembl_genes_id = list(set(ensembl_genes_id))
                success = False
                for ensembl_id in ensembl_genes_id:
                    if ensembl_id in self.genes:
                        gene = self.genes.get_gene(ensembl_id)
                        gene.load_uniprot_entry(seqrec)
                        if gene.reactome:
                            gene = self._extend_reactome_data(gene)
                        self.genes.add_gene(gene)
                        success=True
                        break
                if not success:
                    logging.debug('Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (ensembl_genes_id, seqrec.id))
            else:
                logging.debug('Cannot find ensembl mapping in the uniprot entry %s' % seqrec.id)
        logging.info("%i entries retrieved for uniprot" % c)

        logging.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())

    def _extend_reactome_data(self, gene):
        reaction_types = dict()
        for r in gene.reactome:
            key, reaction = r['id'], r['value']
            for reaction_type in self._get_pathway_type(key):
                reaction_types[reaction_type['pathway type']]=reaction_type
        for r in gene.reactome:
            if r['id']==key:
                r['pathway types']=reaction_types.values()
        return gene

    def _get_pathway_type(self, reaction_id):
        types = []
        try:
            reaction = self.reactome_retriever.get_reaction(reaction_id)
            type_codes =[]
            for path in reaction.path:
                if len(path)>1:
                    type_codes.append(path[1])
            for type_code in type_codes:
                types.append({'pathway type':type_code,
                              'pathway type name': self.reactome_retriever.get_reaction(type_code).label
                              })
        except:
            logging.warn("cannot find additional info for reactome pathway %s. | SKIPPED"%reaction_id)
        return types


    def _store_data(self):

        with Loader(self.es) as loader:
            loader.create_new_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME)
        queue = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|gene_data_storage',
                           r_server=self.r_server,
                           max_size=10000,
                           job_timeout=600)

        q_reporter = RedisQueueStatusReporter([queue])
        q_reporter.start()

        workers = [GeneObjectStorer(self.es,self.r_server,queue) for i in range(multiprocessing.cpu_count()*2)]
        # workers = [SearchObjectAnalyserWorker(queue)]
        for w in workers:
            w.start()

        for geneid, gene in self.genes.iterate():
            queue.put((geneid, gene), self.r_server)

        queue.set_submission_finished(r_server=self.r_server)

        while not queue.is_done(r_server=self.r_server):
            time.sleep(0.5)

        logging.info('all gene objects pushed to elasticsearch')





class GeneRetriever():
    """
    DEPRECATED USE TargetLookUpTable
    Will retrieve a Gene object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter,
                 cache_size = 25):
        warnings.warn('use GeneLookUpTable instead', DeprecationWarning, stacklevel=2)
        self.adapter=adapter
        self.session=adapter.session
        self.cache = OrderedDict()
        self.cache_size = cache_size

    def get_gene(self, geneid):
        if geneid in self.cache:
            gene = self.cache[geneid]
        else:
            gene = self._get_from_db(geneid)
            self._add_to_cache(geneid, gene)

        return gene

    def _get_from_db(self, geneid):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                                       Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                                       geneid)
        gene = Gene(geneid)
        if json_data:
            gene.load_json(json_data)
        return gene

    def _add_to_cache(self, geneid, gene):
        self.cache[geneid]=gene
        while len(self.cache) >self.cache_size:
            self.cache.popitem(last=False)


class GeneLookUpTable(object):
    """
    A redis-based pickable gene look up table
    """

    def __init__(self,
                 es,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7):
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = r_server,
                                            ttl = ttl)
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        if r_server is not None:
            self._load_gene_data(r_server)

    def _load_gene_data(self, r_server = None):
        for target in self._es_query.get_all_targets():
            self._table.set(target['id'],target, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

    def get_gene(self, target_id, r_server = None):
        return self._table.get(target_id, r_server=self._get_r_server(r_server))

    def set_gene(self, target, r_server = None):
        self._table.set(target['id'],target, r_server=self._get_r_server(r_server))

    def get_available_gene_ids(self, r_server = None):
        return self._table.keys(r_server = self._get_r_server(r_server))

    def __contains__(self, key, r_server = None):
        return self._table.__contains__(key, r_server = self._get_r_server(r_server))

    def __getitem__(self, key, r_server = None):
        return self.get_gene(key, r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def __missing__(self, key):
        print key


    def _get_r_server(self, r_server=None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class instantation or at the method level')
        return r_server

    def keys(self):
        return self._table.keys()
