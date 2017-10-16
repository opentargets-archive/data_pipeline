import csv
import gzip
import logging
import multiprocessing
import ujson as json
import urllib2
from StringIO import StringIO
from collections import OrderedDict

import requests
from tqdm import tqdm
from mrtarget.common import TqdmToLogger

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle, RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess
from mrtarget.common.connection import PipelineConnectors
from mrtarget.modules.ChEMBL import ChEMBLLookup
# from mrtarget.modules.GenotypePhenotype import MouseminePhenotypeETL
from mrtarget.modules.Reactome import ReactomeRetriever
from mrtarget.Settings import Config
from elasticsearch.exceptions import NotFoundError

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
        self.ortholog = {}
        self._private ={}
        self.drugs = {}
        self.mouse_phenotypes = {}
        self.protein_classification = {}

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


    def load_ortholog_data(self, data):
        '''loads data from the HCOP ortholog table
        '''
        if 'ortholog_species' in data:
            if data['ortholog_species'] in Config.HGNC_ORTHOLOGS_SPECIES:
                # get rid of some redundant (ie.human) field that we are going to
                # get from other sources anyways
                ortholog_data = dict((k, v) for (k, v) in data.iteritems() if k.startswith('ortholog'))

                # split the fields with multiple values into lists
                if 'ortholog_species_assert_ids' in data:
                    ortholog_data['ortholog_species_assert_ids'] = data['ortholog_species_assert_ids'].split(',')
                if 'support' in data:
                    ortholog_data['support'] = data['support'].split(',')

                # use a readable key for the species in the ortholog dictionary
                species = Config.HGNC_ORTHOLOGS_SPECIES[ortholog_data['ortholog_species']]

                try:
                    # I am appending because there are more than one records
                    # with the same ENSG id and the species.
                    # They can come from the different orthology predictors
                    # or just the case of multiple orthologs per gene.
                    self.ortholog[species].append(ortholog_data)
                except KeyError:
                    self.ortholog[species] = [ortholog_data]

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


    def load_uniprot_entry(self, seqrec, reactome_retriever):
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
            self._extend_reactome_data(reactome_retriever)
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

    def _extend_reactome_data(self, reactome_retriever):
        for r in self.reactome:
            key, reaction = r['id'], r['value']
            reaction['pathway types'] = []
            for reaction_type in self._get_pathway_type(key, reactome_retriever):
                reaction['pathway types'].append(reaction_type)
        return

    def _get_pathway_type(self, reaction_id, reactome_retriever):
        types = []
        try:
            reaction = reactome_retriever.get_reaction(reaction_id)
            type_codes =[]
            for path in reaction.path:
                if len(path)>1:
                    type_codes.append(path[1])
            for type_code in type_codes:
                types.append({'pathway type':type_code,
                              'pathway type name': reactome_retriever.get_reaction(type_code).label
                              })
        except:
            logger = logging.getLogger(__name__)
            logger.warn("cannot find additional info for reactome pathway %s. | SKIPPED"%reaction_id)
        return types

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

    def __getitem__(self, geneid):
        return self.genes[geneid]

    def get_gene(self, geneid):
        return self.genes[geneid]


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
\t%i (%1.1f%%) active genes in Ensembl'''

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



class GeneObjectStorer(RedisQueueWorkerProcess):

    def __init__(self, es, r_server, queue, dry_run=False):
        super(GeneObjectStorer, self).__init__(queue, None)
        self.es = None
        self.r_server = None
        self.loader = None
        self.dry_run = dry_run

    def process(self, data):
        geneid, gene = data
        '''process objects to simple search object'''
        gene.preprocess()
        self.loader.put(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                       Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                       geneid,
                       gene.to_json(),
                       create_index=False)

    def init(self):
        super(GeneObjectStorer, self).init()
        self.loader = Loader(dry_run=self.dry_run)

    def close(self):
        super(GeneObjectStorer, self).close()
        self.loader.close()



class GeneManager():
    """
    Merge data available in ?elasticsearch into proper json objects
    """

    def __init__(self,
                 loader,
                 r_server):


        self.loader = loader
        self.r_server = r_server
        self.esquery = ESQuery(loader.es)
        self.genes = GeneSet()
        self.reactome_retriever=ReactomeRetriever(loader.es)
        self.chembl_handler = ChEMBLLookup()
        self._logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self._logger,level=logging.INFO)



    def merge_all(self, dry_run = False):
        bar = tqdm(desc='Merging data from available databases',
                   total = 7,
                   unit= 'steps',
                   file=self.tqdm_out)
        self._get_hgnc_data_from_json()
        bar.update()
        self._get_ortholog_data()
        bar.update()
        try:
            self._get_ensembl_data()
        except NotFoundError:
            self._logger.error('no ensembl index in ES. Skipping. Has the --ensembl step been run?')
        bar.update()
        try:
            self._get_uniprot_data()
        except NotFoundError:
            self._logger.error('no uniprot index in ES. Skipping. Has the --uniprot step been run?')
        bar.update()
        self._get_chembl_data()
        bar.update()
        # self._get_mouse_phenotypes_data()
        # bar.update()
        self._store_data(dry_run=dry_run)
        bar.update()


    def _get_hgnc_data_from_json(self):
        self._logger.info("HGNC parsing - requesting from URL %s" % Config.HGNC_COMPLETE_SET)
        req = urllib2.Request(Config.HGNC_COMPLETE_SET)
        response = urllib2.urlopen(req)
        self._logger.info("HGNC parsing - response code %s" % response.code)
        data = json.loads(response.read())
        for row in tqdm(data['response']['docs'],
                        desc='loading genes from HGNC',
                        unit_scale=True,
                        unit='genes',
                        file=self.tqdm_out,
                        leave=False):
            gene = Gene()
            gene.load_hgnc_data_from_json(row)
            self.genes.add_gene(gene)

        self._logger.info("STATS AFTER HGNC PARSING:\n" + self.genes.get_stats())

    def _get_ortholog_data(self):

        self._logger.info("Ortholog parsing - requesting from URL %s" % Config.HGNC_ORTHOLOGS)
        req = requests.get(Config.HGNC_ORTHOLOGS)
        self._logger.info("Ortholog parsing - response code %s" % req.status_code)
        req.raise_for_status()

        # io.BytesIO is StringIO.StringIO in python 2
        for row in tqdm(csv.DictReader(gzip.GzipFile(fileobj=StringIO(req.content)),delimiter="\t"),
                        desc='loading orthologues genes from HGNC',
                        unit_scale=True,
                        unit='genes',
                        file=self.tqdm_out,
                        leave=False):
            if row['human_ensembl_gene'] in self.genes:
                self.genes[row['human_ensembl_gene']].load_ortholog_data(row)

        self._logger.info("STATS AFTER HGNC ortholog PARSING:\n" + self.genes.get_stats())

    # def _get_mouse_phenotypes_data(self):
    #
    #     mpetl = MouseminePhenotypeETL(loader=self.loader, r_server=self.r_server)
    #     mpetl.process_all()
    #     for gene_id, gene in tqdm(self.genes.iterate(),
    #                               desc='Adding phenotype data from MGI',
    #                               unit=' gene',
    #                               file=self.tqdm_out):
    #         ''' extend gene with related mouse phenotype data '''
    #         if gene.approved_symbol in mpetl.human_genes:
    #                 self._logger.info("Adding phenotype data from MGI for gene %s" % (gene.approved_symbol))
    #                 gene.mouse_phenotypes = mpetl.human_genes[gene.approved_symbol]["mouse_orthologs"]

    def _get_ensembl_data(self):
        for row in tqdm(self.esquery.get_all_ensembl_genes(),
                        desc='loading genes from Ensembl',
                        unit_scale=True,
                        unit='genes',
                        file=self.tqdm_out,
                        leave=False,
                        total=self.esquery.count_elements_in_index(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME)):
            if row['id'] in self.genes:
                gene = self.genes.get_gene(row['id'])
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)
            else:
                gene = Gene()
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)

        self._clean_non_reference_genes()

        self._logger.info("STATS AFTER ENSEMBL PARSING:\n" + self.genes.get_stats())

    def _clean_non_reference_genes(self):
        for geneid, gene in self.genes.iterate():
            if not gene.is_ensembl_reference:
                self.genes.remove_gene(geneid)

    # @do_profile
    def _get_uniprot_data(self):
        c = 0
        for seqrec in tqdm(self.esquery.get_all_uniprot_entries(),
                           desc='loading genes from UniProt',
                           unit_scale=True,
                           unit='genes',
                           leave=False,
                           file=self.tqdm_out,
                           total= self.esquery.count_elements_in_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME)):
            c += 1
            if c % 1000 == 0:
                self._logger.info("%i entries retrieved for uniprot" % c)
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
                        gene.load_uniprot_entry(seqrec, self.reactome_retriever)
                        self.genes.add_gene(gene)
                        success=True
                        break
                if not success:
                    self._logger.debug('Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (ensembl_genes_id, seqrec.id))
            else:
                self._logger.debug('Cannot find ensembl mapping in the uniprot entry %s' % seqrec.id)
        self._logger.info("%i entries retrieved for uniprot" % c)

        # self._logger.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())



    def _store_data(self, dry_run = False):

        self.loader.create_new_index(Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME)
        queue = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|gene_data_storage',
                           r_server=self.r_server,
                           serialiser='jsonpickle',
                           max_size=10000,
                           job_timeout=600)

        q_reporter = RedisQueueStatusReporter([queue])
        q_reporter.start()

        workers = [GeneObjectStorer(self.loader.es,
                                    None,
                                    queue,
                                    dry_run=dry_run) for i in range(4)]
        # workers = [SearchObjectAnalyserWorker(queue)]
        for w in workers:
            w.start()

        for geneid, gene in self.genes.iterate():
            queue.put((geneid, gene), self.r_server)

        queue.set_submission_finished(r_server=self.r_server)

        for w in workers:
            w.join()

        self._logger.info('all gene objects pushed to elasticsearch')

    def _get_chembl_data(self):
        self._logger.info("Retrieving Chembl Drug")
        self.chembl_handler.download_molecules_linked_to_target()
        self._logger.info("Retrieving Chembl Target Class ")
        self.chembl_handler.download_protein_classification()
        self._logger.info("Adding Chembl data to genes ")
        for gene_id, gene in tqdm(self.genes.iterate(),
                                  desc='Getting drug data from chembl',
                                  unit=' gene',
                                  file=self.tqdm_out):
            target_drugnames = []
            ''' extend gene with related drug names '''
            if gene.uniprot_accessions:
                for a in gene.uniprot_accessions:
                    if a in self.chembl_handler.uni2chembl:
                        chembl_id = self.chembl_handler.uni2chembl[a]
                        if chembl_id in self.chembl_handler.target2molecule:
                            molecules = self.chembl_handler.target2molecule[chembl_id]
                            for mol in molecules:
                                if mol in self.chembl_handler.molecule2synonyms:
                                    synonyms = self.chembl_handler.molecule2synonyms[mol]
                                    target_drugnames.extend(synonyms)
                        if a in self.chembl_handler.protein_classification:
                            gene.protein_classification['chembl'] = self.chembl_handler.protein_classification[a]
                        break
            if target_drugnames:
                gene.drugs['chembl_drugs'] = target_drugnames



