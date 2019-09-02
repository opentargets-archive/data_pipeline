import logging

from yapsy.IPlugin import IPlugin
import simplejson as json
import configargparse

from mrtarget.modules.GeneData import Gene
from opentargets_urlzsource import URLZSource


class HGNC(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)


    def load_hgnc_data_from_json(self, gene, data):
        if 'ensembl_gene_id' in data:
            gene.ensembl_gene_id = data['ensembl_gene_id']
            if not gene.ensembl_gene_id:
                # TODO warn ?
                gene.ensembl_gene_id = data['ensembl_id_supplied_by_ensembl']
            if 'hgnc_id' in data:
                gene.hgnc_id = data['hgnc_id']
            if 'symbol' in data:
                gene.approved_symbol = data['symbol']
            if 'name' in data:
                gene.approved_name = data['name']
            if 'status' in data:
                gene.status = data['status']
            if 'locus_group' in data:
                gene.locus_group = data['locus_group']
            if 'prev_symbols' in data:
                gene.previous_symbols = data['prev_symbols']
            if 'prev_names' in data:
                gene.previous_names = data['prev_names']
            if 'alias_symbol' in data:
                gene.symbol_synonyms.extend( data['alias_symbol'])
            if 'alias_name' in data:
                gene.name_synonyms = data['alias_name']
            if 'enzyme_ids' in data:
                gene.enzyme_ids = data['enzyme_ids']
            if 'entrez_id' in data:
                gene.entrez_gene_id = data['entrez_id']
            if 'refseq_accession' in data:
                gene.refseq_ids = data['refseq_accession']
            if 'gene_family_tag' in data:
                gene.gene_family_tag = data['gene_family_tag']
            if 'gene_family_description' in data:
                gene.gene_family_description = data['gene_family_description']
            if 'ccds_ids' in data:
                gene.ccds_ids = data['ccds_ids']
            if 'vega_id' in data:
                gene.vega_ids = data['vega_id']
            if 'uniprot_ids' in data:
                #gene.uniprot_accessions = data['uniprot_ids']
                # Split the set(list) to avoid erroor Nonetype
                gene.uniprot_accessions.extend(data['uniprot_ids'])
                acc_set = set(gene.uniprot_accessions)
                gene.uniprot_accessions = list(acc_set)


                if not gene.uniprot_id:
                    # TODO warn?
                    gene.uniprot_id = gene.uniprot_accessions[0]
            if 'pubmed_id' in data:
                gene.pubmed_ids = data['pubmed_id']

    def merge_data(self, genes, es, r_server, data_config, es_config):

        self._logger.info("HGNC parsing - requesting from URL %s", data_config.hgnc_complete_set)

        with URLZSource(data_config.hgnc_complete_set).open() as source:

            data = json.load(source)

            for row in data['response']['docs']:
                gene = Gene()
                self.load_hgnc_data_from_json(gene, row)
                genes.add_gene(gene)

            self._logger.info("STATS AFTER HGNC PARSING:\n" + genes.get_stats())
