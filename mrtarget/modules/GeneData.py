import logging
from collections import OrderedDict
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from opentargets_urlzsource import URLZSource

import simplejson as json
from yapsy.PluginManager import PluginManager
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

UNI_ID_ORG_PREFIX = 'http://identifiers.org/uniprot/'
ENS_ID_ORG_PREFIX = 'http://identifiers.org/ensembl/'

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
        self.tractability = {}

    def _set_id(self):
        if self.ensembl_gene_id:
            self.id = self.ensembl_gene_id
        elif self.hgnc_id:
            self.id = self.hgnc_id
        elif self.entrez_gene_id:
            self.id = self.entrez_gene_id
        else:
            self.id = None

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
        for k, v in self.genes.iteritems():
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


"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(genes, index, doc):
    for geneid, gene in genes.iterate():
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = geneid
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = gene.to_json()

        yield action


class GeneManager():
    """
    Merge data available in ?elasticsearch into proper json objects


    plugin_paths is a collection of filesystem paths to search for potential plugins

    plugin_names is an ordered collection of class names of plugins which determines
    the order they are handled in

    """

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, 
            es_settings, r_server,
            plugin_paths, plugin_order, 
            data_config, es_config,
            workers_write, queue_write):

        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.r_server = r_server
        self.plugin_order = plugin_order
        self.data_config = data_config
        self.es_config = es_config
        self.workers_write = workers_write
        self.queue_write = queue_write

        self.genes = GeneSet()
        self._logger = logging.getLogger(__name__)

        self._logger.debug("Preparing the plug in management system")
        # Build the manager
        self.simplePluginManager = PluginManager()
        # Tell it the default place(s) where to find plugins
        self.simplePluginManager.setPluginPlaces(plugin_paths)
        for dir in plugin_paths:
            self._logger.debug("Looking for plugins in %s", dir)
        # Load all plugins
        self.simplePluginManager.collectPlugins()


    def merge_all(self,  dry_run):


        es = new_es_client(self.es_hosts)

        #run the actual plugins
        for plugin_name in self.plugin_order:
            plugin = self.simplePluginManager.getPluginByName(plugin_name)
            plugin.plugin_object.merge_data(self.genes, 
                es, self.r_server, 
                self.data_config, self.es_config)

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):

            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(self.genes, self.es_index, self.es_doc)
            failcount = 0

            if not dry_run:
                results = None
                if self.workers_write > 0:
                    results = elasticsearch.helpers.parallel_bulk(es, actions,
                            thread_count=self.workers_write,
                            queue_size=self.queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(es, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)



    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):

        #number of gene entries
        gene_count = 0
        #Note: try to avoid doing this more than once!
        for gene_entry in Search().using(es).index(index).query(MatchAll()).scan():
            gene_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["gene.count"] = gene_count

        return metrics