from yapsy.IPlugin import IPlugin
import jsonpickle
import base64
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll, Match
import logging
logging.basicConfig(level=logging.INFO)


class ReactomeRetriever():
    def __init__(self, es, index):
        self.es = es
        self.index = index

    def get_reaction(self, reaction_id):
        response = Search().using(self.es).index(self.index).query(Match(_id=reaction_id))[0:1].execute()
        return response.hits[0]


class Uniprot(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def merge_data(self, genes, loader, r_server, data_config, es_config):

        es = loader.es
        index = es_config.uni.name
        reactome_retriever = ReactomeRetriever(es, es_config.rea.name)

        c = 0
        for seqrec in Search().using(es).index(index).query(MatchAll()).scan():
            #these are base 64 encoded json - need to decode
            #TODO access the source directly
            seqrec = jsonpickle.decode(base64.b64decode(seqrec['entry']))
            c += 1
            if c % 1000 == 0:
                self._logger.info("%i entries retrieved for uniprot" % c)
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
                    self._logger.debug(
                        'Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (
                        ensembl_genes_id, seqrec.id))
            else:
                self._logger.debug('Cannot find ensembl mapping in the uniprot entry %s' % seqrec.id)
        self._logger.info("%i entries retrieved for uniprot" % c)

        # self._logger.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())


