
import csv
import gzip
import logging

from yapsy.IPlugin import IPlugin
import configargparse

from mrtarget.Settings import Config
from mrtarget.common import URLZSource

class Orthologs(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)


    def print_name(self):
        self._logger.info("This is plugin ORTHOLOGS")

    def merge_data(self, genes, loader, r_server, data_config):

        #turn the species id/label mappings into a dict from the argument list
        self.orthologs_species = dict()
        if data_config.hgnc_orthologs_species:
            for value in data_config.hgnc_orthologs_species:
                code,label = value.split("-")
                label = label.strip()
                code = code.strip()
                self.orthologs_species[code] = label

        self._logger.info("Ortholog parsing - requesting from URL %s",data_config.hgnc_orthologs)

        with URLZSource(data_config.hgnc_orthologs).open() as source:
            reader = csv.DictReader(source, delimiter="\t")
            for row in reader:
                if row['human_ensembl_gene'] in genes:
                    self.add_ortholog_data_to_gene(gene=genes[row['human_ensembl_gene']], data=row)

        self._logger.info("STATS AFTER HGNC ortholog PARSING:\n" + genes.get_stats())

    def add_ortholog_data_to_gene(self, gene, data):
        if 'ortholog_species' in data:
            if data['ortholog_species'] in self.orthologs_species:
                # get rid of some redundant (ie.human) field that we are going to
                # get from other sources anyways
                ortholog_data = dict((k, v) for (k, v) in data.iteritems() if k.startswith('ortholog'))

                # split the fields with multiple values into lists
                if 'ortholog_species_assert_ids' in data:
                    ortholog_data['ortholog_species_assert_ids'] = data['ortholog_species_assert_ids'].split(',')
                if 'support' in data:
                    ortholog_data['support'] = data['support'].split(',')

                # use a readable key for the species in the ortholog dictionary
                species = self.orthologs_species[ortholog_data['ortholog_species']]

                try:
                    # I am appending because there are more than one records
                    # with the same ENSG id and the species.
                    # They can come from the different orthology predictors
                    # or just the case of multiple orthologs per gene.
                    gene.ortholog[species].append(ortholog_data)
                except KeyError:
                    gene.ortholog[species] = [ortholog_data]
