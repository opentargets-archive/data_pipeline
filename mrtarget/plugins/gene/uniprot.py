from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
from mrtarget.common.ElasticsearchQuery import ESQuery
from elasticsearch.exceptions import NotFoundError
from mrtarget.modules.Reactome import ReactomeRetriever
from tqdm import tqdm
import logging
logging.basicConfig(level=logging.INFO)

class Uniprot(IPlugin):

    def print_name(self):
        logging.info("Uniprot (and Reactome) gene data plugin")
        gene = Gene()

    def merge_data(self, genes, loader, r_server, tqdm_out):

        esquery = ESQuery(loader.es)
        reactome_retriever = ReactomeRetriever(loader.es)

        try:
            esquery.count_elements_in_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME)
        except NotFoundError as ex:
            logging.error('no Uniprot index found in ES. Skipping. Has the --uniprot step been run? Are you pointing to the correct index? %s' % ex)
            raise ex

        c = 0
        for seqrec in tqdm(esquery.get_all_uniprot_entries(),
                           desc='loading genes from UniProt',
                           unit_scale=True,
                           unit='genes',
                           leave=False,
                           file=tqdm_out,
                           total=esquery.count_elements_in_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME)):
            c += 1
            if c % 1000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
            if 'Ensembl' in seqrec.annotations['dbxref_extended']:
                ensembl_data = seqrec.annotations['dbxref_extended']['Ensembl']
                ensembl_genes_id = []
                for ens_data_point in ensembl_data:
                    ensembl_genes_id.append(ens_data_point['value']['gene ID'])
                ensembl_genes_id = list(set(ensembl_genes_id))
                success = False
                for ensembl_id in ensembl_genes_id:
                    if ensembl_id in genes:
                        gene = genes.get_gene(ensembl_id)
                        gene.load_uniprot_entry(seqrec, reactome_retriever)
                        genes.add_gene(gene)
                        success = True
                        break
                if not success:
                    logging.debug(
                        'Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (
                        ensembl_genes_id, seqrec.id))
            else:
                logging.debug('Cannot find ensembl mapping in the uniprot entry %s' % seqrec.id)
        logging.info("%i entries retrieved for uniprot" % c)

        # self._logger.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())


