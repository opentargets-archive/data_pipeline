from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll
import sys
import logging

class Ensembl(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def load_ensembl_data(self, gene, data):

        if 'id' in data:
            gene.is_active_in_ensembl = True
            gene.ensembl_gene_id = data['id']
        if 'assembly_name' in data:
            gene.ensembl_assembly_name = data['assembly_name']
        if 'biotype' in data:
            gene.biotype = data['biotype']
        if 'description' in data:
            gene.ensembl_description = data['description'].split(' [')[0]
            if not gene.approved_name:
                gene.approved_name = gene.ensembl_description
        if 'end' in data:
            gene.gene_end = data['end']
        if 'start' in data:
            gene.gene_start = data['start']
        if 'strand' in data:
            gene.strand = data['strand']
        if 'seq_region_name' in data:
            gene.chromosome = data['seq_region_name']
        if 'display_name' in data:
            gene.ensembl_external_name = data['display_name']
            if not gene.approved_symbol:
                gene.approved_symbol= data['display_name']
        if 'version' in data:
            gene.ensembl_gene_version = data['version']
        if 'cytobands' in data:
            gene.cytobands = data['cytobands']
        if 'ensembl_release' in data:
            gene.ensembl_release = data['ensembl_release']
        is_reference = (data['is_reference'] and data['id'].startswith('ENSG'))
        gene.is_ensembl_reference = is_reference

    def merge_data(self, genes, es, r_server, data_config, es_config):

        index = es_config.ens.name

        with URLZSource(self.ensembl_filename).open() as ensembl_filename

        for row in Search().using(es).index(index).query(MatchAll()).scan():
            gene = None
            if row['id'] in genes:
                gene = genes.get_gene(row['id'])
            else:
                gene = Gene()
            self.load_ensembl_data(gene, row)
            genes.add_gene(gene)

        self._clean_non_reference_genes(genes)

        self._logger.info("STATS AFTER ENSEMBL PARSING:\n" + genes.get_stats())

    def _clean_non_reference_genes(self, genes):
        for geneid, gene in genes.iterate():
            if not gene.is_ensembl_reference:
                genes.remove_gene(geneid)
