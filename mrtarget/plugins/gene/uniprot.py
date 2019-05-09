from yapsy.IPlugin import IPlugin
from mrtarget.constants import Const
from mrtarget.common.ElasticsearchQuery import ESQuery
from elasticsearch.exceptions import NotFoundError
from mrtarget.modules.Reactome import ReactomeRetriever
import logging


class ReactomeRetriever():
    def __init__(self, es, index):
        self.es = es
        self.index = index

    def get_reaction(self, reaction_id):
        response = Search().using(self.es).index(self.index).query(Match(_id=reaction_id))[0:1].execute()
        if response.hits.total > 0:
            return response.hits[0]
        else:
            return None


class Uniprot(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def print_name(self):
        self._logger.info("Uniprot (and Reactome) gene data plugin")

    def merge_data(self, genes, loader, r_server, data_config):

        esquery = ESQuery(loader.es)
        reactome_retriever = ReactomeRetriever(loader.es)
        self.missing_ensembl = set()
        self.missing_reactome = set()


    def load_uniprot_entry(self, gene, seqrec, reactome_retriever):
        gene.uniprot_id = seqrec.id
        gene.is_in_swissprot = True
        if seqrec.dbxrefs:
            gene.dbxrefs.extend(seqrec.dbxrefs)
            gene.dbxrefs= sorted(list(set(gene.dbxrefs)))
        for k, v in seqrec.annotations.items():
            if k == 'accessions':
                gene.uniprot_accessions = v
            if k == 'keywords':
                gene.uniprot_keywords = v
            if k == 'comment_function':
                gene.uniprot_function = v
            if k == 'comment_similarity':
                gene.uniprot_similarity = v
            if k == 'comment_subunit':
                gene.uniprot_subunit = v
            if k == 'comment_subcellularlocation_location':
                gene.uniprot_subcellular_location = v
            if k == 'comment_pathway':
                gene.uniprot_pathway = v
            if k == 'gene_name_primary':
                if not gene.approved_symbol:
                    gene.approved_symbol= v
                elif v!= gene.approved_symbol:
                    if v not in gene.symbol_synonyms:
                        gene.symbol_synonyms.append(v)
            if k == 'gene_name_synonym':
                for symbol in v:
                    if symbol not in gene.symbol_synonyms:
                        gene.symbol_synonyms.append(symbol)
            if k.startswith('recommendedName'):
                gene.name_synonyms.extend(v)
            if k.startswith('alternativeName'):
                gene.name_synonyms.extend(v)
        gene.name_synonyms.append(seqrec.description)
        gene.name_synonyms = list(set(gene.name_synonyms))
        if 'GO' in  seqrec.annotations['dbxref_extended']:
            gene.go = seqrec.annotations['dbxref_extended']['GO']
        if 'Reactome' in  seqrec.annotations['dbxref_extended']:
            gene.reactome = seqrec.annotations['dbxref_extended']['Reactome']
            for r in gene.reactome:
                reaction = reactome_retriever.get_reaction(r['id'])
                if reaction is None:
                    self.missing_reactome.add(r["id"])
                else:
                    r['value'] = reaction
                    r['value']['pathway types'] = []
                    type_codes =[]
                    for path in r['value'].path:
                        if len(path) > 1:
                            type_codes.append(path[1])
                    for type_code in type_codes:
                        r['value']['pathway types'].append({
                            'pathway type':type_code,
                            'pathway type name': reactome_retriever.get_reaction(type_code).label
                            })
        if 'PDB' in  seqrec.annotations['dbxref_extended']:
            gene.pdb = seqrec.annotations['dbxref_extended']['PDB']
        if 'ChEMBL' in  seqrec.annotations['dbxref_extended']:
            gene.chembl = seqrec.annotations['dbxref_extended']['ChEMBL']
        if 'DrugBank' in  seqrec.annotations['dbxref_extended']:
            gene.drugbank = seqrec.annotations['dbxref_extended']['DrugBank']
        if 'Pfam' in  seqrec.annotations['dbxref_extended']:
            gene.pfam = seqrec.annotations['dbxref_extended']['Pfam']
        if 'InterPro' in  seqrec.annotations['dbxref_extended']:
            gene.interpro = seqrec.annotations['dbxref_extended']['InterPro']


    def merge_data(self, genes, es, r_server, data_config, es_config):

        try:
            esquery.count_elements_in_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME)
        except NotFoundError as ex:
            self._logger.error('no Uniprot index found in ES. Skipping. Has the --uniprot step been run? Are you pointing to the correct index? %s' % ex)
            raise ex

        c = 0
        for seqrec in esquery.get_all_uniprot_entries():
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
                        self.load_uniprot_entry(gene, seqrec, reactome_retriever)
                        genes.add_gene(gene)
                        success = True
                        break
                if not success:
                    self._logger.debug(
                        'Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (
                        ensembl_genes_id, seqrec.id))
            else:
                self.missing_ensembl.add(seqrec.id)

        for reactome_id in sorted(self.missing_reactome):
            self._logger.warning("Unable to find reactome for %s", reactome_id)
        for uniprot_id in sorted(self.missing_ensembl):
            self._logger.warning("Unable to find ensemble for %s", uniprot_id)

        self._logger.info("%i entries retrieved for uniprot" % c)

        # self._logger.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())


