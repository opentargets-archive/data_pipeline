from collections import OrderedDict
import copy
from datetime import datetime
import logging
from StringIO import StringIO
import urllib2
from sqlalchemy import and_
import ujson as json
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage, Loader
from common.ElasticsearchQuery import ESQuery
from common.PGAdapter import HgncGeneInfo, EnsemblGeneInfo, UniprotInfo, ElasticsearchLoad
from common.Redis import RedisLookupTablePickle
from common.UniprotIO import UniprotIterator
from modules.Reactome import ReactomeRetriever
from settings import Config

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
            for reaction_code, reaction in self.reactome.items():
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



class GeneManager():
    """
    Merge data available in postgres into proper json objects
    """

    def __init__(self,
                 adapter,
                 es):

        self.adapter=adapter
        self.session=adapter.session
        self.es = es
        self.esquery = ESQuery(es)
        self.genes = GeneSet()
        self.reactome_retriever=ReactomeRetriever(adapter)



    def merge_all(self):
        # self._get_hgnc_data()
        self._get_hgnc_data_from_json()
        self._get_ensembl_data()
        self._get_uniprot_data()
        self._store_data()

    def _get_hgnc_data(self):
        for row in self.session.query(HgncGeneInfo).yield_per(1000):
            if not '~' in row.approved_symbol:
                gene = Gene()
                gene.load_hgnc_data(row)
                self.genes.add_gene(gene)

        logging.info("STATS AFTER HGNC PARSING:\n" + self.genes.get_stats())

    def _get_hgnc_data_from_json(self):
        req = urllib2.Request('ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json')
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

    def _get_uniprot_data(self):
        c = 0
        for row in self.session.query(UniprotInfo).yield_per(1000):
            seqrec = UniprotIterator(StringIO(row.uniprot_entry), 'uniprot-xml').next()
            c += 1
            if c % 5000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
            if 'Ensembl' in seqrec.annotations['dbxref_extended']:
                ensembl_data=seqrec.annotations['dbxref_extended']['Ensembl']
                ensembl_genes_id=[]
                for enst in ensembl_data:
                    ensembl_genes_id.append(ensembl_data[enst]['gene ID'])
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
        for key, reaction in gene.reactome.items():
            for reaction_type in self._get_pathway_type(key):
                reaction_types[reaction_type['pathway type']]=reaction_type
        gene.reactome[key]['pathway types']=reaction_types.values()
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

        with Loader(self.es, chunk_size=10) as loader:
            c = 0
            for geneid, gene in self.genes.iterate():
                if gene.is_ensembl_reference:
                    gene.preprocess()
                    c += 1
                    print c
                    loader.put(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                               Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                               geneid,
                               gene.to_json(),
                               create_index=True)
                    if c % 5000 == 0:
                        logging.info("%i gene objects pushed to elasticsearch" % c)

        logging.info('%i gene objects pushed to elasticsearch'%c)




class GeneUploader():
    """upload the gene objects to elasticsearch"""

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader


    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                         Config.ELASTICSEARCH_GENE_NAME_DOC_NAME)
        self.loader.optimize_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME)


class GeneRetriever():
    """
    Will retrieve a Gene object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter,
                 cache_size = 25):
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


class TargetLookUpTable(object):
    """
    A redis-based pickable target look up table
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
        self.r_server = None
        if r_server is not None:
            self._load_target_data()

    def _load_target_data(self, r_server = None):
        for target in self._es_query.get_all_targets():
            self._table.set(target['id'],target, r_server=r_server)#TODO can be improved by sending elements in batches

    def get_target(self, target_id, r_server = None ):
        return self._table.get(target_id, r_server=r_server)

    def set_target(self, target, r_server = None):
        self._table.set(target['id'],target, r_server=r_server)

    def get_available_target_ids(self, r_server = None):
        return self._table.keys()